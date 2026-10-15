package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"feed-service/internal/domain"
)

type postHTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewPostHTTPClient(baseURL string) domain.PostClient {
	return &postHTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func (c *postHTTPClient) GetPostByID(ctx context.Context, postID int64) (*domain.Post, error) {
	url := fmt.Sprintf("%s/v1/posts/%d", c.baseURL, postID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post-service call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("post-service returned status: %d", resp.StatusCode)
	}

	var post domain.Post
	if err := json.NewDecoder(resp.Body).Decode(&post); err != nil {
		return nil, err
	}

	return &post, nil
}

func (c *postHTTPClient) GetPostsByIDs(ctx context.Context, postIDs []int64) ([]*domain.Post, error) {
	if len(postIDs) == 0 {
		return []*domain.Post{}, nil
	}

	// Concurrent fetch or batch endpoint
	posts := make([]*domain.Post, 0, len(postIDs))
	for _, id := range postIDs {
		p, err := c.GetPostByID(ctx, id)
		if err == nil && p != nil {
			posts = append(posts, p)
		}
	}

	return posts, nil
}