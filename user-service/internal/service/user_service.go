package service

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"user-service/internal/domain"
)

var (
	ErrInvalidUserID	= errors.New("invalid user ID")
	ErrSelfFollow 		= errors.New("user cannot follow themselves")
	ErrEmptyUsername	= errors.New("username cannot be emtpy")
	ErrInvalidEmail 	= errors.New("invalid email address")
	ErrUsernameTooShort	= errors.New("username must be at least 3 characters")
)

type userService struct {
	repo domain.UserRepository
}

func NewUserService(repo domain.UserRepository) domain.UserService {
	return &userService{repo: repo}
}

func (s *userService) RegisterUser(ctx context.Context, username, email string) (*domain.User, error) {
	// TODO: Validate input parameters (email format, username length)
	username = strings.TrimSpace(username)
	email = strings.TrimSpace(email)

	if len(username) < 3{
		return nil, ErrUsernameTooShort
	}

	if _, err := mail.ParseAddress(email); err!=nil {
		return nil, ErrInvalidEmail
	}

	// TODO: Construct domain.User struct and call s.repo.CreateUser
	user := &domain.User{
		Username: username,
		Email: email,
		IsCelebrity: false,
	}

	if err := s.repo.CreateUser(ctx, user); err != nil {
		return nil, fmt.Errorf("service failed to register user: %w", err)
	}

	return user, nil
}

func (s *userService) GetUserProfile(ctx context.Context, id int64) (*domain.User, error) {
	// TODO: Fetch from repo; optionally cache hot user metadata in Redis (user:{id})
	return s.repo.GetUserByID(ctx, id)
}

func (s *userService) FollowUser(ctx context.Context, followerID, followeeID int64) error {
	if followerID == followeeID {
		return errors.New("users cannot follow themselves")
	}
	// TODO: Call repo to create relationship
	// TODO: (Optional) Publish FollowCreated event to Kafka/SQS if real-time notifications are enabled
	return s.repo.AddFollow(ctx, followerID, followeeID)
}

func (s *userService) UnfollowUser(ctx context.Context, followerID, followeeID int64) error {
	// TODO: Call repo to delete relationship
	// TODO: (Optional) Remove unfollowed posts asynchronously from follower's feed cache
	return s.repo.RemoveFollow(ctx, followerID, followeeID)
}

func (s *userService) GetFollowersList(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error) {
	// TODO: Ensure limit caps (e.g., max 1000 per page) to prevent worker OOM issues
	return s.repo.GetFollowerIDs(ctx, userID, cursor, limit)
}

func (s *userService) GetFolloweesList(ctx context.Context, userID int64) ([]int64, error) {
	// TODO: Return list of users followed by given user
	return s.repo.GetFolloweeIDs(ctx, userID)
}