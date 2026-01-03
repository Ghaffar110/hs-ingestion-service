package storage

import (
	"database/sql"
	"time"
    // "context"
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
		return err
	}

	return nil
}

// QueryEvents retrieves events from the database
func QueryEvents(limit int, filtersStr string) ([]models.StoredEvent, error) {
	rows, err := DB.Query(`
		SELECT id, order_id, event_type, event_timestamp, received_at,
		       customer_id, restaurant_id, driver_id, location_lat, location_lng,
		       platform_token, validation_status, validation_error
		FROM events
		`+filtersStr+`
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
			logger.Error("failed to scan row", map[string]interface{}{"error": err.Error()})
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
		DB.Close()
	}
}

// InitDatabase opens the database connection
func InitDatabase(databaseURL string) error {
	var err error
	DB, err = connectWithRetry(databaseURL, 3)
	if err != nil {
		return err
	}

	// Test connection
	if err = DB.Ping(); err != nil {
		return err
	}

	logger.Info("database connection established", nil)
	return nil
}

// storage/storage.go
func MonitorDatabase(databaseURL string) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        if DB != nil {
            if err := DB.Ping(); err == nil {
                continue
            }
        }

        // Try to reconnect
        err := InitDatabase(databaseURL)
        if err != nil {
            log.Printf("DB reconnection failed: %v\n", err)
        } else {
            log.Println("DB reconnected successfully")
        }
    }
}


// Connect with retries
func connectWithRetry(dsn string, maxRetries int) (*sql.DB, error) {
    var err error

    for i := 1; i <= maxRetries; i++ {
        DB, err = sql.Open("postgres", dsn)
        if err == nil {
            if err = DB.Ping(); err == nil {
                log.Println("database connected")
                return DB, nil
            }
        }

        log.Printf("db connection failed (attempt %d/%d): %v", i, maxRetries, err)
        time.Sleep(time.Duration(i) * time.Second)
    }

    return nil, fmt.Errorf("database unavailable after retries")
}


