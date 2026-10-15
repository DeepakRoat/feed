package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"
	"user-service/internal/handler"
	"user-service/internal/repository"
	"user-service/internal/service"
)

func main() {
	// 1. Fetch Environment Configuration
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/user_db?sslmode=disable"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	// 2. Database Connection & Connection Pool Settings
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to open DB connection: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL database: %v", err)
	}
	log.Println("Database connection established successfully")

	// 3. Dependency Injection Assembly
	userRepo := repository.NewPostgresUserRepo(db)
	userService := service.NewUserService(userRepo)
	userHandler := handler.NewUserHandler(userService)

	// 4. HTTP Route Registration (Using Go 1.22+ Standard ServeMux)
	mux := http.NewServeMux()

	// External API Endpoints
	mux.HandleFunc("POST /v1/users", userHandler.Register)
	mux.HandleFunc("GET /v1/users/{id}", userHandler.GetProfile)
	mux.HandleFunc("POST /v1/users/{id}/follow", userHandler.Follow)
	mux.HandleFunc("DELETE /v1/users/{id}/unfollow", userHandler.Unfollow)

	// Internal Microservice Endpoints (Used by Fan-out Workers)
	mux.HandleFunc("GET /v1/internal/users/{id}/followers", userHandler.GetFollowersInternal)

	// Health Check Endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	// 5. HTTP Server Configuration
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("User Service listening on port %s...", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server startup failed: %v", err)
	}
}