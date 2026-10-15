package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"post-service/internal/domain"
	"post-service/internal/service"
)

type PostHandler struct {
	svc domain.PostService
}

// NewPostHandler instantiates HTTP handlers wired to the post service
func NewPostHandler(svc domain.PostService) *PostHandler {
	return &PostHandler{svc: svc}
}

// Data Transfer Objects (DTOs)

type createPostRequest struct {
	Text      string   `json:"text"`
	MediaURLs []string `json:"media_urls"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type timelineResponse struct {
	Posts      []*domain.Post `json:"posts"`
	NextCursor int64          `json:"next_cursor"`
}

// Helper functions for HTTP responses

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if payload != nil {
		_ = json.NewEncoder(w).Encode(payload)
	}
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, errorResponse{Error: message})
}

// Handlers

// Create handles POST /v1/posts
func (h *PostHandler) Create(w http.ResponseWriter, r *http.Request) {
	// Extract authenticated author_id from gateway header
	authorHeader := r.Header.Get("X-User-ID")
	authorID, err := strconv.ParseInt(authorHeader, 10, 64)
	if err != nil || authorID <= 0 {
		respondWithError(w, http.StatusUnauthorized, "Missing or invalid authentication header X-User-ID")
		return
	}

	var req createPostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	post, err := h.svc.CreatePost(r.Context(), authorID, req.Text, req.MediaURLs)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidAuthorID), errors.Is(err, service.ErrEmptyPost):
			respondWithError(w, http.StatusBadRequest, err.Error())
		default:
			respondWithError(w, http.StatusInternalServerError, "Failed to create post")
		}
		return
	}

	respondWithJSON(w, http.StatusCreated, post)
}

// Get handles GET /v1/posts/{id}
func (h *PostHandler) Get(w http.ResponseWriter, r *http.Request) {
	postIDStr := r.PathValue("id")
	postID, err := strconv.ParseInt(postIDStr, 10, 64)
	if err != nil || postID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid post ID")
		return
	}

	post, err := h.svc.GetPost(r.Context(), postID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Post not found")
		return
	}

	respondWithJSON(w, http.StatusOK, post)
}

// GetAuthorTimeline handles GET /v1/posts/author/{author_id}?cursor=0&limit=20
func (h *PostHandler) GetAuthorTimeline(w http.ResponseWriter, r *http.Request) {
	authorIDStr := r.PathValue("author_id")
	authorID, err := strconv.ParseInt(authorIDStr, 10, 64)
	if err != nil || authorID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid author ID in path")
		return
	}

	// Extract pagination parameters
	query := r.URL.Query()
	
	var cursor int64 = 0
	if cStr := query.Get("cursor"); cStr != "" {
		if parsed, err := strconv.ParseInt(cStr, 10, 64); err == nil {
			cursor = parsed
		}
	}

	limit := 20
	if lStr := query.Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	posts, nextCursor, err := h.svc.GetAuthorTimeline(r.Context(), authorID, cursor, limit)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to fetch author timeline")
		return
	}

	respondWithJSON(w, http.StatusOK, timelineResponse{
		Posts:      posts,
		NextCursor: nextCursor,
	})
}