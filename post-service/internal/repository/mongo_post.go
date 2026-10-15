package repository

import (
	"context"
	"errors"
	"fmt"

	"post-service/internal/domain"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoPostRepo struct {
	db         *mongo.Database
	collection *mongo.Collection
}

// NewMongoPostRepo instantiates a MongoDB implementation of domain.PostRepository
func NewMongoPostRepo(db *mongo.Database) domain.PostRepository {
	return &mongoPostRepo{
		db:         db,
		collection: db.Collection("posts"),
	}
}

// InitIndexes creates compound index on { author_id: 1, _id: -1 } for timeline query optimization
func (r *mongoPostRepo) InitIndexes(ctx context.Context) error {
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "author_id", Value: 1},
			{Key: "_id", Value: -1},
		},
		Options: options.Index().SetName("idx_author_timeline"),
	}

	_, err := r.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create mongodb indexes: %w", err)
	}

	return nil
}

// CreatePost inserts a new post document into MongoDB
func (r *mongoPostRepo) CreatePost(ctx context.Context, post *domain.Post) error {
	_, err := r.collection.InsertOne(ctx, post)
	if err != nil {
		return fmt.Errorf("failed to insert post document: %w", err)
	}
	return nil
}

// GetPostByID retrieves a single post document by its Snowflake _id
func (r *mongoPostRepo) GetPostByID(ctx context.Context, postID int64) (*domain.Post, error) {
	filter := bson.M{"_id": postID}

	var post domain.Post
	err := r.collection.FindOne(ctx, filter).Decode(&post)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("post not found: %w", err)
		}
		return nil, fmt.Errorf("failed to query post by id: %w", err)
	}

	return &post, nil
}

// GetPostsByAuthorID queries author timeline using cursor pagination ($lt on _id)
func (r *mongoPostRepo) GetPostsByAuthorID(ctx context.Context, authorID int64, cursor int64, limit int) ([]*domain.Post, int64, error) {
	filter := bson.M{"author_id": authorID}

	// Apply cursor constraint if specified (fetch posts older than cursor post_id)
	if cursor > 0 {
		filter["_id"] = bson.M{"$lt": cursor}
	}

	findOptions := options.Find().
		SetSort(bson.M{"_id": -1}). // Descending by time-sortable Snowflake ID
		SetLimit(int64(limit))

	cursorResult, err := r.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query author posts: %w", err)
	}
	defer cursorResult.Close(ctx)

	posts := make([]*domain.Post, 0, limit)
	for cursorResult.Next(ctx) {
		var post domain.Post
		if err := cursorResult.Decode(&post); err != nil {
			return nil, 0, fmt.Errorf("failed to decode post document: %w", err)
		}
		posts = append(posts, &post)
	}

	if err := cursorResult.Err(); err != nil {
		return nil, 0, fmt.Errorf("cursor iteration error: %w", err)
	}

	// Calculate nextCursor for client pagination
	var nextCursor int64 = 0
	if len(posts) == limit {
		nextCursor = posts[len(posts)-1].ID
	}

	return posts, nextCursor, nil
}