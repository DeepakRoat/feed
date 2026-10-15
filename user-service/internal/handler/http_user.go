package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"user-service/internal/domain"
	"user-service/internal/service"
)

type UserHandler struct {
	svc domain.UserService
}

// NewUserHandler instantiates HTTP handlers wired to the user service
func NewUserHandler(svc domain.UserService) *UserHandler {
	return &UserHandler{svc: svc}
}

// Data Transfer Objects (DTOs)

type registerRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type followersResponse struct {
	FollowerIDs []int64 `json:"follower_ids"`
	NextCursor  int64   `json:"next_cursor"`
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

// Register handles POST /v1/users
func (h *UserHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	user, err := h.svc.RegisterUser(r.Context(), req.Username, req.Email)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrUsernameTooShort), errors.Is(err, service.ErrInvalidEmail):
			respondWithError(w, http.StatusBadRequest, err.Error())
		default:
			respondWithError(w, http.StatusInternalServerError, "Failed to register user")
		}
		return
	}

	respondWithJSON(w, http.StatusCreated, user)
}

// GetProfile handles GET /v1/users/{id}
func (h *UserHandler) GetProfile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	userID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || userID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid user ID in path")
		return
	}

	user, err := h.svc.GetUserProfile(r.Context(), userID)
	if err != nil {
		if errors.Is(err, service.ErrInvalidUserID) {
			respondWithError(w, http.StatusBadRequest, err.Error())
		} else {
			respondWithError(w, http.StatusNotFound, "User not found")
		}
		return
	}

	respondWithJSON(w, http.StatusOK, user)
}

// Follow handles POST /v1/users/{id}/follow
func (h *UserHandler) Follow(w http.ResponseWriter, r *http.Request) {
	// Extract followee ID from path parameter
	followeeIDStr := r.PathValue("id")
	followeeID, err := strconv.ParseInt(followeeIDStr, 10, 64)
	if err != nil || followeeID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid followee user ID")
		return
	}

	// Extract authenticated follower ID from gateway header (e.g., X-User-ID)
	followerHeader := r.Header.Get("X-User-ID")
	followerID, err := strconv.ParseInt(followerHeader, 10, 64)
	if err != nil || followerID <= 0 {
		respondWithError(w, http.StatusUnauthorized, "Missing or invalid authentication header X-User-ID")
		return
	}

	err = h.svc.FollowUser(r.Context(), followerID, followeeID)
	if err != nil {
		if errors.Is(err, service.ErrSelfFollow) {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Failed to follow user")
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{"status": "success"})
}

// Unfollow handles DELETE /v1/users/{id}/unfollow
func (h *UserHandler) Unfollow(w http.ResponseWriter, r *http.Request) {
	followeeIDStr := r.PathValue("id")
	followeeID, err := strconv.ParseInt(followeeIDStr, 10, 64)
	if err != nil || followeeID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid followee user ID")
		return
	}

	followerHeader := r.Header.Get("X-User-ID")
	followerID, err := strconv.ParseInt(followerHeader, 10, 64)
	if err != nil || followerID <= 0 {
		respondWithError(w, http.StatusUnauthorized, "Missing or invalid authentication header X-User-ID")
		return
	}

	err = h.svc.UnfollowUser(r.Context(), followerID, followeeID)
	if err != nil {
		if errors.Is(err, service.ErrSelfFollow) {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Failed to unfollow user")
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{"status": "success"})
}

// GetFollowersInternal handles internal worker batch calls:
// GET /v1/internal/users/{id}/followers?cursor=0&limit=1000
func (h *UserHandler) GetFollowersInternal(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.PathValue("id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil || userID <= 0 {
		respondWithError(w, http.StatusBadRequest, "Invalid target user ID")
		return
	}

	// Parse pagination parameters
	query := r.URL.Query()
	
	var cursor int64 = 0
	if cStr := query.Get("cursor"); cStr != "" {
		if parsed, err := strconv.ParseInt(cStr, 10, 64); err == nil {
			cursor = parsed
		}
	}

	limit := 1000
	if lStr := query.Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	followerIDs, nextCursor, err := h.svc.GetFollowersList(r.Context(), userID, cursor, limit)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to fetch followers batch")
		return
	}

	respondWithJSON(w, http.StatusOK, followersResponse{
		FollowerIDs: followerIDs,
		NextCursor:  nextCursor,
	})
}