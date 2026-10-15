package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"feed-service/internal/domain"
)

type feedService struct {
	feedRepo   domain.FeedRepository
	userClient domain.UserClient
	postClient domain.PostClient
}

func NewFeedService(repo domain.FeedRepository, userClient domain.UserClient, postClient domain.PostClient) domain.FeedService {
	return &feedService{
		feedRepo:   repo,
		userClient: userClient,
		postClient: postClient,
	}
}

// ProcessPostCreatedEvent processes Kafka events (Push vs Pull hybrid routing)
func (s *feedService) ProcessPostCreatedEvent(ctx context.Context, event domain.PostCreatedEvent) error {
	// 1. Check if the author is a celebrity
	author, err := s.userClient.GetUserProfile(ctx, event.AuthorID)
	if err != nil {
		return fmt.Errorf("failed to fetch author profile: %w", err)
	}

	// 2. CELEBRITY PATH (PULL MODEL)
	if author.IsCelebrity {
		if err := s.feedRepo.UpdateCelebLastPostAt(ctx, event.AuthorID, event.Timestamp); err != nil {
			return fmt.Errorf("failed to update celeb last_post_at: %w", err)
		}
		if err := s.feedRepo.PushToCelebTimeline(ctx, event.AuthorID, event.PostID, event.Timestamp); err != nil {
			return fmt.Errorf("failed to push to celeb timeline: %w", err)
		}
		return nil
	}

	// 3. NORMAL USER PATH (PUSH MODEL)
	var cursor int64 = 0
	batchSize := 1000

	for {
		followerIDs, nextCursor, err := s.userClient.GetFollowerIDs(ctx, event.AuthorID, cursor, batchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch followers page: %w", err)
		}

		if len(followerIDs) > 0 {
			if err := s.feedRepo.PushToFeedBatch(ctx, followerIDs, event.PostID, event.Timestamp); err != nil {
				// Log and continue on partial failure (idempotent retries will resolve missing IDs)
				fmt.Printf("[Worker Error] Failed batch feed push: %v\n", err)
			}
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	return nil
}

// GetHomeFeed executes the hybrid read-time merge algorithm
func (s *feedService) GetHomeFeed(ctx context.Context, userID int64, cursor int64, limit int) ([]*domain.Post, int64, error) {
	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	// 1. Fetch precomputed feed from Redis (Push path)
	precomputedIDs, err := s.feedRepo.GetFeedPostIDs(ctx, userID, cursor, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get precomputed feed IDs: %w", err)
	}

	// 2. Fetch followed celebrities (Pull path)
	followeeIDs, err := s.userClient.GetFolloweeIDs(ctx, userID)
	if err != nil {
		// Log warning and fallback to precomputed feed if user service fails
		followeeIDs = []int64{}
	}

	celebPostIDs := make([]int64, 0)
	if len(followeeIDs) > 0 {
		celebTimestamps, err := s.feedRepo.GetCelebLastPostAtBatch(ctx, followeeIDs)
		if err == nil {
			for celebID, lastPostAt := range celebTimestamps {
				// If celebrity posted since the cursor time (or generally recently)
				if cursor == 0 || lastPostAt >= cursor {
					posts, err := s.feedRepo.GetCelebPosts(ctx, celebID, limit)
					if err == nil {
						celebPostIDs = append(celebPostIDs, posts...)
					}
				}
			}
		}
	}

	// 3. Merge & Deduplicate post IDs in memory
	idSet := make(map[int64]bool)
	allIDs := make([]int64, 0, len(precomputedIDs)+len(celebPostIDs))

	for _, id := range precomputedIDs {
		if !idSet[id] {
			idSet[id] = true
			allIDs = append(allIDs, id)
		}
	}
	for _, id := range celebPostIDs {
		if !idSet[id] {
			idSet[id] = true
			allIDs = append(allIDs, id)
		}
	}

	// Sort IDs descending (Snowflake IDs are time-sortable)
	sort.Slice(allIDs, func(i, j int) bool {
		return allIDs[i] > allIDs[j]
	})

	// Slice top N for current page
	if len(allIDs) > limit {
		allIDs = allIDs[:limit]
	}

	if len(allIDs) == 0 {
		return []*domain.Post{}, 0, nil
	}

	// 4. Hydrate Post Bodies (Redis Cache MGET -> Fallback to Post Service)
	cachedPosts, missingIDs, err := s.feedRepo.GetCachedPosts(ctx, allIDs)
	if err != nil {
		missingIDs = allIDs
		cachedPosts = make(map[int64]*domain.Post)
	}

	if len(missingIDs) > 0 {
		fetchedPosts, err := s.postClient.GetPostsByIDs(ctx, missingIDs)
		if err == nil && len(fetchedPosts) > 0 {
			for _, p := range fetchedPosts {
				cachedPosts[p.ID] = p
			}
			// Asynchronously populate Redis cache for missing items
			go func(posts []*domain.Post) {
				_ = s.feedRepo.SetCachedPosts(context.Background(), posts, 24*time.Hour)
			}(fetchedPosts)
		}
	}

	// Preserve sorted order in final hydrated list
	resultPosts := make([]*domain.Post, 0, len(allIDs))
	for _, id := range allIDs {
		if post, exists := cachedPosts[id]; exists && post != nil {
			resultPosts = append(resultPosts, post)
		}
	}

	var nextCursor int64 = 0
	if len(resultPosts) == limit {
		nextCursor = resultPosts[len(resultPosts)-1].ID
	}

	return resultPosts, nextCursor, nil
}