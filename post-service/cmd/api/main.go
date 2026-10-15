package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"post-service/internal/domain"
	"post-service/internal/handler"
	"post-service/internal/repository"
	"post-service/internal/service"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MockKafkaProducer implements domain.EventProducer for development
type MockKafkaProducer struct{}

func (m *MockKafkaProducer) PublishPostCreated(ctx context.Context, event domain.PostCreatedEvent) error {
	log.Printf("[Kafka Producer] Published Event -> PostID: %d | AuthorID: %d | Timestamp: %d",
		event.PostID, event.AuthorID, event.Timestamp)
	return nil
}

func main() {
	// 1. Load Configurations
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	workerIDStr := os.Getenv("WORKER_ID")
	workerID, err := strconv.ParseInt(workerIDStr, 10, 64)
	if err != nil {
		workerID = 1
	}

	// 2. MongoDB Connection Setup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().
		ApplyURI(mongoURI).
		SetMaxPoolSize(50).
		SetMinPoolSize(10)

	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to create MongoDB client: %v", err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()

	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB server: %v", err)
	}
	log.Println("Successfully connected to MongoDB database")

	db := mongoClient.Database("post_db")

	// 3. Initialize Repository and Database Indexes
	postRepo := repository.NewMongoPostRepo(db)
	if err := postRepo.InitIndexes(context.Background()); err != nil {
		log.Printf("Warning: Database index initialization failed: %v", err)
	}

	// 4. Wire Dependencies
	eventProducer := &MockKafkaProducer{}
	postService := service.NewPostService(postRepo, eventProducer, workerID)
	postHandler := handler.NewPostHandler(postService)

	// 5. Register HTTP Routes (Go 1.22+ ServeMux Pattern Matching)
	mux := http.NewServeMux()

	// Post Management
	mux.HandleFunc("POST /v1/posts", postHandler.Create)
	mux.HandleFunc("GET /v1/posts/{id}", postHandler.Get)

	// Explicit Path for Author Timeline (Routes directly to Post Service without /v1/users prefix)
	mux.HandleFunc("GET /v1/posts/author/{author_id}", postHandler.GetAuthorTimeline)

	// Health Check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
	    w.Header().Set("Content-Type", "application/json")
	    w.WriteHeader(http.StatusOK)
	    _, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	// 6. Start Server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Post Service running on port %s...", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server encounter error: %v", err)
	}
}