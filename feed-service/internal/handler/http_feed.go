package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"feed-service/internal/domain"
)

type FeedHandler struct {
	svc domain.FeedService
}

func NewFeedHandler(svc domain.FeedService) *FeedHandler {
	return &FeedHandler{svc: svc}
}

type feedResponse struct {
	Posts      []*domain.Post `json:"posts"`
	NextCursor int64          `json:"next_cursor"`
}

type errorResponse struct {
	Error string `json:"error"`
}

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

// GetFeed handles GET /v1/feed?cursor=0&limit=20
func (h *FeedHandler) GetFeed(w http.ResponseWriter, r *http.Request) {
	// Extract authenticated user ID from gateway header
	userHeader := r.Header.Get("X-User-ID")
	userID, err := strconv.ParseInt(userHeader, 10, 64)
	if err != nil || userID <= 0 {
		respondWithError(w, http.StatusUnauthorized, "Missing or invalid X-User-ID header")
		return
	}

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

	posts, nextCursor, err := h.svc.GetHomeFeed(r.Context(), userID, cursor, limit)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve home feed")
		return
	}

	respondWithJSON(w, http.StatusOK, feedResponse{
		Posts:      posts,
		NextCursor: nextCursor,
	})
}