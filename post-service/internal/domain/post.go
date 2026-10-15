package domain

import (
	"context"
	"time"
)

// Post represents the MongoDB document schema
type Post struct {
	ID           int64     `bson:"_id" json:"post_id"` // Snowflake ID (Time-sortable Primary Key)
	AuthorID     int64     `bson:"author_id" json:"author_id"`
	Text         string    `bson:"text" json:"text"`
	MediaURLs    []string  `bson:"media_urls" json:"media_urls"`
	LikeCount    int64     `bson:"like_count" json:"like_count"`
	CommentCount int64     `bson:"comment_count" json:"comment_count"`
	CreatedAt    time.Time `bson:"created_at" json:"created_at"`
}

// PostCreatedEvent represents the async payload published to Kafka/SQS
type PostCreatedEvent struct {
	PostID    int64 `json:"post_id"`
	AuthorID  int64 `json:"author_id"`
	Timestamp int64 `json:"timestamp"`
}

// EventProducer interface for publishing async events
type EventProducer interface {
	PublishPostCreated(ctx context.Context, event PostCreatedEvent) error
}

// PostRepository interface for MongoDB operations
type PostRepository interface {
	CreatePost(ctx context.Context, post *Post) error
	GetPostByID(ctx context.Context, postID int64) (*Post, error)
	GetPostsByAuthorID(ctx context.Context, authorID int64, cursor int64, limit int) ([]*Post, int64, error)
	InitIndexes(ctx context.Context) error
}

// PostService interface for business logic operations
type PostService interface {
	CreatePost(ctx context.Context, authorID int64, text string, mediaURLs []string) (*Post, error)
	GetPost(ctx context.Context, postID int64) (*Post, error)
	GetAuthorTimeline(ctx context.Context, authorID int64, cursor int64, limit int) ([]*Post, int64, error)
}