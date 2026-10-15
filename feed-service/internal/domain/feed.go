package domain

import (
	"context"
	"time"
)

// Post represents the hydrated post body returned in the feed response
type Post struct {
	ID           int64     `json:"post_id"`
	AuthorID     int64     `json:"author_id"`
	Text         string    `json:"text"`
	MediaURLs    []string  `json:"media_urls"`
	LikeCount    int64     `json:"like_count"`
	CommentCount int64     `json:"comment_count"`
	CreatedAt    time.Time `json:"created_at"`
}

// UserProfile represents minimal user metadata needed for fan-out decisions
type UserProfile struct {
	ID          int64     `json:"id"`
	IsCelebrity bool      `json:"is_celebrity"`
	LastPostAt  time.Time `json:"last_post_at,omitempty"`
}

// PostCreatedEvent represents the incoming message payload from Kafka/SQS
type PostCreatedEvent struct {
	PostID    int64 `json:"post_id"`
	AuthorID  int64 `json:"author_id"`
	Timestamp int64 `json:"timestamp"`
}

// FeedRepository interface for Redis sorted sets and cache operations
type FeedRepository interface {
	// Push path operations
	PushToFeedBatch(ctx context.Context, followerIDs []int64, postID int64, timestamp int64) error
	TrimFeed(ctx context.Context, userID int64, maxItems int64) error

	// Pull path (celebrity) operations
	UpdateCelebLastPostAt(ctx context.Context, celebID int64, timestamp int64) error
	PushToCelebTimeline(ctx context.Context, celebID int64, postID int64, timestamp int64) error
	GetCelebLastPostAtBatch(ctx context.Context, celebIDs []int64) (map[int64]int64, error)
	GetCelebPosts(ctx context.Context, celebID int64, limit int) ([]int64, error)

	// Read path operations
	GetFeedPostIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, error)
	
	// Cache operations
	GetCachedPosts(ctx context.Context, postIDs []int64) (map[int64]*Post, []int64, error)
	SetCachedPosts(ctx context.Context, posts []*Post, ttl time.Duration) error
}

// UserClient interface for communicating with user-service
type UserClient interface {
	GetUserProfile(ctx context.Context, userID int64) (*UserProfile, error)
	GetFollowerIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error)
	GetFolloweeIDs(ctx context.Context, userID int64) ([]int64, error)
}

// PostClient interface for communicating with post-service
type PostClient interface {
	GetPostByID(ctx context.Context, postID int64) (*Post, error)
	GetPostsByIDs(ctx context.Context, postIDs []int64) ([]*Post, error)
}

// FeedService interface for feed domain logic
type FeedService interface {
	ProcessPostCreatedEvent(ctx context.Context, event PostCreatedEvent) error
	GetHomeFeed(ctx context.Context, userID int64, cursor int64, limit int) ([]*Post, int64, error)
}