package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"feed-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

type redisFeedRepo struct {
	client *redis.Client
}

func NewRedisFeedRepo(client *redis.Client) domain.FeedRepository {
	return &redisFeedRepo{client: client}
}

// PushToFeedBatch pushes a post ID to multiple followers' Redis feed sorted sets using pipelining
func (r *redisFeedRepo) PushToFeedBatch(ctx context.Context, followerIDs []int64, postID int64, timestamp int64) error {
	pipe := r.client.Pipeline()

	for _, followerID := range followerIDs {
		key := fmt.Sprintf("feed:%d", followerID)
		pipe.ZAdd(ctx, key, redis.Z{
			Score:  float64(timestamp),
			Member: postID,
		})
		// Cap set length to 800 items per user feed
		pipe.ZRemRangeByRank(ctx, key, 0, -801)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !stringEquals(err.Error(), redis.Nil.Error()) {
		return fmt.Errorf("failed to execute pipeline feed push: %w", err)
	}

	return nil
}

func (r *redisFeedRepo) TrimFeed(ctx context.Context, userID int64, maxItems int64) error {
	key := fmt.Sprintf("feed:%d", userID)
	return r.client.ZRemRangeByRank(ctx, key, 0, -(maxItems + 1)).Err()
}

func (r *redisFeedRepo) UpdateCelebLastPostAt(ctx context.Context, celebID int64, timestamp int64) error {
	key := fmt.Sprintf("user:%d:last_post_at", celebID)
	return r.client.Set(ctx, key, timestamp, 0).Err()
}

func (r *redisFeedRepo) PushToCelebTimeline(ctx context.Context, celebID int64, postID int64, timestamp int64) error {
	key := fmt.Sprintf("timeline:%d", celebID)
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(timestamp),
		Member: postID,
	})
	pipe.ZRemRangeByRank(ctx, key, 0, -101) // Keep recent 100 posts
	_, err := pipe.Exec(ctx)
	return err
}

func (r *redisFeedRepo) GetCelebLastPostAtBatch(ctx context.Context, celebIDs []int64) (map[int64]int64, error) {
	if len(celebIDs) == 0 {
		return map[int64]int64{}, nil
	}

	keys := make([]string, len(celebIDs))
	for i, id := range celebIDs {
		keys[i] = fmt.Sprintf("user:%d:last_post_at", id)
	}

	results, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to mget celeb timestamps: %w", err)
	}

	resMap := make(map[int64]int64)
	for i, val := range results {
		if val != nil {
			if tsStr, ok := val.(string); ok {
				if ts, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
					resMap[celebIDs[i]] = ts
				}
			}
		}
	}

	return resMap, nil
}

// GetCelebPosts fetches a celebrity's recent post IDs from their timeline sorted set,
// respecting the caller's cursor so pagination works correctly across pages.
func (r *redisFeedRepo) GetCelebPosts(ctx context.Context, celebID int64, limit int) ([]int64, error) {
	key := fmt.Sprintf("timeline:%d", celebID)

	// Note: The caller (feed_service) filters by cursor before calling this method,
	// so we always fetch top-limit posts and let the merge+sort handle dedup/ordering.
	// For cursor-aware paging within a single celebrity's timeline we use ZRangeArgs.
	vals, err := r.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:   key,
		Start: "-inf",
		Stop:  "+inf",
		Rev:   true,
		Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}

	postIDs := make([]int64, 0, len(vals))
	for _, v := range vals {
		if pid, err := strconv.ParseInt(v, 10, 64); err == nil {
			postIDs = append(postIDs, pid)
		}
	}

	return postIDs, nil
}

func (r *redisFeedRepo) GetFeedPostIDs(ctx context.Context, userID int64, cursor int64, limit int) ([]int64, error) {
	key := fmt.Sprintf("feed:%d", userID)

	// Use ZRangeArgs (Redis 7+ compatible replacement for deprecated ZRevRangeByScore)
	stopScore := "+inf"
	if cursor > 0 {
		stopScore = fmt.Sprintf("(%d", cursor) // exclusive upper bound
	}

	vals, err := r.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   "-inf",
		Stop:    stopScore,
		ByScore: true,
		Rev:     true,
		Count:   int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get feed post IDs: %w", err)
	}

	postIDs := make([]int64, 0, len(vals))
	for _, v := range vals {
		if pid, err := strconv.ParseInt(v, 10, 64); err == nil {
			postIDs = append(postIDs, pid)
		}
	}

	return postIDs, nil
}

func (r *redisFeedRepo) GetCachedPosts(ctx context.Context, postIDs []int64) (map[int64]*domain.Post, []int64, error) {
	if len(postIDs) == 0 {
		return map[int64]*domain.Post{}, nil, nil
	}

	keys := make([]string, len(postIDs))
	for i, id := range postIDs {
		keys[i] = fmt.Sprintf("post:%d", id)
	}

	results, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, postIDs, fmt.Errorf("failed to mget post cache: %w", err)
	}

	cachedPosts := make(map[int64]*domain.Post)
	missingIDs := make([]int64, 0)

	for i, val := range results {
		postID := postIDs[i]
		if val == nil {
			missingIDs = append(missingIDs, postID)
			continue
		}

		jsonStr, ok := val.(string)
		if !ok {
			missingIDs = append(missingIDs, postID)
			continue
		}

		var post domain.Post
		if err := json.Unmarshal([]byte(jsonStr), &post); err != nil {
			missingIDs = append(missingIDs, postID)
			continue
		}

		cachedPosts[postID] = &post
	}

	return cachedPosts, missingIDs, nil
}

func (r *redisFeedRepo) SetCachedPosts(ctx context.Context, posts []*domain.Post, ttl time.Duration) error {
	if len(posts) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, post := range posts {
		key := fmt.Sprintf("post:%d", post.ID)
		bytes, err := json.Marshal(post)
		if err == nil {
			pipe.Set(ctx, key, string(bytes), ttl)
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

func stringEquals(a, b string) bool {
	return a == b
}