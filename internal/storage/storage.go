package storage

import (
	"database/sql"
	"time"
    "context"
    "fmt"
    "log"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

// DB is the database handle - initialized by InitDatabase
var DB *sql.DB

// StoreEvent persists an event to the database
func StoreEvent(event models.DeliveryEvent, platformToken, validationStatus string) error {
	receivedAt := time.Now().Unix()
	eventTimestamp := event.EventTimestamp.Unix()

	_, err := DB.Exec(`
		INSERT INTO events (order_id, event_type, event_timestamp, received_at,
		                    customer_id, restaurant_id, driver_id, location_lat, location_lng,
		                    platform_token, validation_status, validation_error)
		VALUES ('`+event.OrderID+`', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '')
	`, event.EventType, eventTimestamp, receivedAt,
		event.CustomerID, event.RestaurantID, event.DriverID, event.Location.Lat, event.Location.Lng,
		platformToken, validationStatus)

	if err != nil {
		logger.Error("database ping failed", map[string]interface{}{
        "error": err.Error(),
    })
		return err
	}

	return nil
}

// QueryEvents retrieves events from the database
func QueryEvents(limit int, filtersStr string) ([]models.StoredEvent, error) {
	if DB == nil {
		return nil, fmt.Errorf("DB unavailable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := DB.QueryContext(ctx, `
		SELECT id, order_id, event_type, event_timestamp, received_at,
		       customer_id, restaurant_id, driver_id, location_lat, location_lng,
		       platform_token, validation_status, validation_error
		FROM events Where event_type like '`+filtersStr+`'
		ORDER BY received_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		var validationError sql.NullString
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &validationError); err != nil {
			log.Printf("scan row failed: %v", err)
			continue
		}
		if validationError.Valid {
			e.ValidationError = validationError.String
		}
		events = append(events, e)
	}

	return events, nil
}

// Close closes the database connection
func Close() {
	if DB != nil {
		logger.Info("Closing Database Connection", nil)
		DB.Close()
	}
}

// InitDatabase initializes the database connection with retries
func InitDatabase(databaseURL string) error {
	if DB != nil {
		return nil
	}

	const maxRetries = 3
	baseDelay := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error
		DB, err = sql.Open("postgres", databaseURL)
		if err != nil {
			log.Printf("DB open attempt %d failed: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * baseDelay)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // short ping timeout
		err = DB.PingContext(ctx)
		cancel()

		if err == nil {
			log.Println("DB connected")
			return nil
		}

		log.Printf("DB ping attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * baseDelay)
	}

	DB = nil
	log.Println("DB unavailable, proceeding in degraded mode")
	return fmt.Errorf("DB unavailable after retries")
}

// MonitorDatabase runs in background, non-blocking
func MonitorDatabase(databaseURL string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if DB == nil || DB.Ping() != nil {
			log.Println("DB slow/unavailable, retrying")
			_ = InitDatabase(databaseURL) // short ping timeout
		}
	}
}
