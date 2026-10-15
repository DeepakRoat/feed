package domain

import (
	"context"
	"time"
)

// User represents user profile metadata
type User struct {
	ID          int64     `json:"id"`
	Username    string    `json:"username"`
	Email       string    `json:"email"`
	IsCelebrity bool      `json:"is_celebrity"`
	LastPostAt  time.Time `json:"last_post_at,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Follow represents the social graph edge
type Follow struct {
	FollowerID int64     `json:"follower_id"`
	FolloweeID int64     `json:"followee_id"`
	CreatedAt  time.Time `json:"created_at"`
}

// UserRepository defines database contracts
type UserRepository interface {
	CreateUser(ctx context.Context, user *User) error
	GetUserByID(ctx context.Context, id int64) (*User, error)
	
	// Follow operations
	AddFollow(ctx context.Context, followerID, followeeID int64) error
	RemoveFollow(ctx context.Context, followerID, followeeID int64) error
	
	// Querying follow graph
	// Crucial for Fan-out worker (push path) - MUST support cursor pagination
	GetFollowerIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error)
	// Crucial for Feed Service read path (pull path) - fetches celebs followed by user
	GetFolloweeIDs(ctx context.Context, userID int64) ([]int64, error)
}

// UserService defines business logic contracts
type UserService interface {
	RegisterUser(ctx context.Context, username, email string) (*User, error)
	GetUserProfile(ctx context.Context, id int64) (*User, error)
	FollowUser(ctx context.Context, followerID, followeeID int64) error
	UnfollowUser(ctx context.Context, followerID, followeeID int64) error
	GetFollowersList(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error)
	GetFolloweesList(ctx context.Context, userID int64) ([]int64, error)
}