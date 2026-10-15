package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"feed-service/internal/domain"
)

type userHTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewUserHTTPClient(baseURL string) domain.UserClient {
	return &userHTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func (c *userHTTPClient) GetUserProfile(ctx context.Context, userID int64) (*domain.UserProfile, error) {
	url := fmt.Sprintf("%s/v1/users/%d", c.baseURL, userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("user-service call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("user-service returned status: %d", resp.StatusCode)
	}

	var user domain.UserProfile
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}

	return &user, nil
}

func (c *userHTTPClient) GetFollowerIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, int64, error) {
	url := fmt.Sprintf("%s/v1/internal/users/%d/followers?cursor=%d&limit=%d", c.baseURL, userID, cursor, limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("user-service followers call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("user-service followers returned status: %d", resp.StatusCode)
	}

	var res struct {
		FollowerIDs []int64 `json:"follower_ids"`
		NextCursor  int64   `json:"next_cursor"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, 0, err
	}

	return res.FollowerIDs, res.NextCursor, nil
}

func (c *userHTTPClient) GetFolloweeIDs(ctx context.Context, userID int64) ([]int64, error) {
	// For production, this endpoint returns all IDs followed by given user
	url := fmt.Sprintf("%s/v1/internal/users/%d/followees", c.baseURL, userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("user-service followees call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("user-service followees returned status: %d", resp.StatusCode)
	}

	var res struct {
		FolloweeIDs []int64 `json:"followee_ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res.FolloweeIDs, nil
}