package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"user-service/internal/domain"
)

type postgresUserRepo struct {
	db *sql.DB
}

// NewPostgresUserRepo constructs a PostgreSQL implementation of domain.UserRepository
func NewPostgresUserRepo(db *sql.DB) domain.UserRepository {
	return &postgresUserRepo{db: db}
}

// CreateUser inserts a new user record and populates the auto-generated ID and creation time
func (r *postgresUserRepo) CreateUser(ctx context.Context, user *domain.User) error {
	query := `
		INSERT INTO users (username, email, is_celebrity, created_at)
		VALUES ($1, $2, $3, NOW())
		RETURNING id, created_at;
	`

	err := r.db.QueryRowContext(
		ctx,
		query,
		user.Username,
		user.Email,
		user.IsCelebrity,
	).Scan(&user.ID, &user.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

// GetUserByID fetches user profile metadata by ID
func (r *postgresUserRepo) GetUserByID(ctx context.Context, id int64) (*domain.User, error) {
	query := `
		SELECT id, username, email, is_celebrity, last_post_at, created_at
		FROM users
		WHERE id = $1;
	`

	var user domain.User
	var lastPostAt sql.NullTime

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.IsCelebrity,
		&lastPostAt,
		&user.CreatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found with id %d: %w", id, err)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	if lastPostAt.Valid {
		user.LastPostAt = lastPostAt.Time
	}

	return &user, nil
}

// AddFollow inserts a follow relationship edge into the follows table
func (r *postgresUserRepo) AddFollow(ctx context.Context, followerID, followeeID int64) error {
	query := `
		INSERT INTO follows (follower_id, followee_id, created_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (follower_id, followee_id) DO NOTHING;
	`

	_, err := r.db.ExecContext(ctx, query, followerID, followeeID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert follow relationship: %w", err)
	}

	return nil
}

// RemoveFollow deletes a follow relationship edge from the follows table
func (r *postgresUserRepo) RemoveFollow(ctx context.Context, followerID, followeeID int64) error {
	query := `
		DELETE FROM follows
		WHERE follower_id = $1 AND followee_id = $2;
	`

	_, err := r.db.ExecContext(ctx, query, followerID, followeeID)
	if err != nil {
		return fmt.Errorf("failed to delete follow relationship: %w", err)
	}

	return nil
}

// GetFollowerIDs streams follower IDs in batches ordered by follower_id using cursor pagination
func (r *postgresUserRepo) GetFollowerIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error) {
	query := `
		SELECT follower_id
		FROM follows
		WHERE followee_id = $1 AND follower_id > $2
		ORDER BY follower_id ASC
		LIMIT $3;
	`

	rows, err := r.db.QueryContext(ctx, query, userID, cursor, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query follower ids: %w", err)
	}
	defer rows.Close()

	followerIDs := make([]int64, 0, limit)
	var lastID int64

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, 0, fmt.Errorf("failed to scan follower id: %w", err)
		}
		followerIDs = append(followerIDs, id)
		lastID = id
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error: %w", err)
	}

	// Determine next cursor value (0 indicates no further pages in batch)
	var nextCursor int64 = 0
	if len(followerIDs) == limit {
		nextCursor = lastID
	}

	return followerIDs, nextCursor, nil
}

// GetFolloweeIDs fetches all user IDs that the specified user follows (used in Feed merge pull logic)
func (r *postgresUserRepo) GetFolloweeIDs(ctx context.Context, userID int64) ([]int64, error) {
	query := `
		SELECT followee_id
		FROM follows
		WHERE follower_id = $1;
	`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query followee ids: %w", err)
	}
	defer rows.Close()

	followeeIDs := make([]int64, 0)

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan followee id: %w", err)
		}
		followeeIDs = append(followeeIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return followeeIDs, nil
}
