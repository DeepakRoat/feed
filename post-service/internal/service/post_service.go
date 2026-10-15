package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"post-service/internal/domain"
)

var (
	ErrInvalidPostID   = errors.New("invalid post ID")
	ErrInvalidAuthorID = errors.New("invalid author ID")
	ErrEmptyPost       = errors.New("post must contain text or media")
)

type SnowflakeGenerator struct {
	mu sync.Mutex
	timestamp int64
	workerID int64
	sequence int64
}

func NewSnowflakeGenerator(workerID int64) *SnowflakeGenerator {
	return &SnowflakeGenerator{workerID: workerID & 1023}
}

func (s *SnowflakeGenerator) Generate() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	if now == s.timestamp {
		s.sequence = (s.sequence + 1) & 4095
		if s.sequence == 0 {
			// Sequence exhausted within this millisecond — spin until clock advances
			for now <= s.timestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}
	s.timestamp = now // P0 fix: persist current millisecond for next call

	const customEpoch int64 = 1704067200000
	return ((now-customEpoch) << 22) | (s.workerID << 12) | s.sequence
}

type postService struct {
	repo     domain.PostRepository
	producer domain.EventProducer
	idGenerator *SnowflakeGenerator
}

func NewPostService(repo domain.PostRepository, producer domain.EventProducer, workerID int64) domain.PostService {
	return &postService{
		repo:     repo,
		producer: producer,
		idGenerator: NewSnowflakeGenerator(workerID),
	}
}

func (s *postService) CreatePost(ctx context.Context, authorID int64, text string, mediaURLs []string) (*domain.Post, error) {
	if authorID <= 0 {
		return nil, ErrInvalidAuthorID
	}
	if text == "" && len(mediaURLs) == 0 {
		return nil, ErrEmptyPost
	}
	if mediaURLs == nil {
		mediaURLs = []string{}
	}

	// TODO: Generate a 64-bit time-sortable Snowflake ID
	postID := s.idGenerator.Generate() // Replace with real Snowflake ID generator

	now := time.Now()
	post := &domain.Post{
		ID:           postID,
		AuthorID:     authorID,
		Text:         text,
		MediaURLs:    mediaURLs,
		LikeCount:    0,
		CommentCount: 0,
		CreatedAt:    now,
	}

	// 1. Synchronous write to persistent store (MongoDB)
	if err := s.repo.CreatePost(ctx, post); err != nil {
		return nil, err
	}

	// 2. Asynchronous write to Kafka/SQS event queue for Fan-out processing
	event := domain.PostCreatedEvent{
		PostID:    post.ID,
		AuthorID:  post.AuthorID,
		Timestamp: now.UnixMilli(), // P1 fix: use milliseconds to match Redis score units and Snowflake epoch
	}

	// Non-blocking or asynchronous emission to event bus
	go func() {
		// Non-blocking publish call using a detached context
		asyncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.producer.PublishPostCreated(asyncCtx, event); err != nil {
			// Log error without aborting synchronous post creation response
			fmt.Printf("[ERROR] Async event publishing failed for PostID=%d: %v\n", event.PostID, err)
		}
	}()

	return post, nil
}

func (s *postService) GetPost(ctx context.Context, postID int64) (*domain.Post, error) {
	if postID <= 0 {
		return nil, ErrInvalidPostID
	}

	post, err := s.repo.GetPostByID(ctx, postID)
	if err != nil {
		return nil, fmt.Errorf("service failed to fetch post: %w", err)
	}

	return post, nil
}

func (s *postService) GetAuthorTimeline(ctx context.Context, authorID int64, cursor int64, limit int) ([]*domain.Post, int64, error) {
	if authorID <= 0 {
		return nil, 0, ErrInvalidAuthorID
	}

	limit = max(20, min(limit, 100))

	posts, nextCursor, err := s.repo.GetPostsByAuthorID(ctx, authorID, cursor, limit)

	if err != nil {
		return nil, 0, fmt.Errorf("service failed to fetch author timeline: %w", err)
	}

	return posts, nextCursor, nil
}
