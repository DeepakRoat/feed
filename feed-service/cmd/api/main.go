package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"feed-service/internal/client"
	"feed-service/internal/domain"
	"feed-service/internal/handler"
	"feed-service/internal/repository"
	"feed-service/internal/service"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Configurations
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	userServiceURL := os.Getenv("USER_SERVICE_URL")
	if userServiceURL == "" {
		userServiceURL = "http://localhost:8081"
	}

	postServiceURL := os.Getenv("POST_SERVICE_URL")
	if postServiceURL == "" {
		postServiceURL = "http://localhost:8082"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	// 2. Redis Connection
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     50,
		MinIdleConns: 10,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis Cluster successfully")

	// 3. Wiring Dependencies
	feedRepo := repository.NewRedisFeedRepo(rdb)
	userClient := client.NewUserHTTPClient(userServiceURL)
	postClient := client.NewPostHTTPClient(postServiceURL)

	feedService := service.NewFeedService(feedRepo, userClient, postClient)
	feedHandler := handler.NewFeedHandler(feedService)

	// 4. Background Fan-Out Queue Worker Simulation (Kafka Consumer)
	go startAsyncWorkerSimulation(feedService)

	// 5. Register HTTP Routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/feed", feedHandler.GetFeed)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	// 6. Start HTTP Server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("Feed Service running on port %s...", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server encounter error: %v", err)
	}
}

// Simulated background consumer polling events from Kafka/SQS
func startAsyncWorkerSimulation(svc domain.FeedService) {
	log.Println("Fan-out Event Consumer Worker started...")
	// In production: Consume from Kafka topic "PostCreated"
	// Example event loop stub:
	// for msg := range kafkaConsumer.Messages() {
	//     var event domain.PostCreatedEvent
	//     json.Unmarshal(msg.Value, &event)
	//     svc.ProcessPostCreatedEvent(context.Background(), event)
	// }
}