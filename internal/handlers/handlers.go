package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// DeliveryEventsHandler routes between POST (receive events) and GET (query events)
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		handlePostDeliveryEvent(w, r)
	} else if r.Method == http.MethodGet {
		handleGetDeliveryEvents(w, r)
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePostDeliveryEvent processes incoming delivery events
func handlePostDeliveryEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	platformToken := r.Header.Get("X-Platform-Token")

	validTokens, err := validation.FetchPlatformTokens()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if validation.ValidatePlatformToken(platformToken, validTokens) {
		err = storage.StoreEvent(event, platformToken, "valid")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

// handleGetDeliveryEvents returns stored events for debugging/inspection
func handleGetDeliveryEvents(w http.ResponseWriter, r *http.Request) {
	logger.Info("GET /delivery-events requested", nil)

	// Get optional limit parameter (default 100)
	limitStr := r.URL.Query().Get("limit")
	filtersStr := r.URL.Query().Get("filters")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	logger.Info("GET /delivery-events requested string", filtersStr)
	events, err := storage.QueryEvents(limit, filtersStr)
	if err != nil {
		logger.Error("failed to query events", map[string]interface{}{"error": err.Error()})
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	logger.Info("returning events", map[string]interface{}{"count": len(events)})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// HealthHandler returns service health status
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
