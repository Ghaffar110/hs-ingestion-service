package validation

import (
	"encoding/json"
	"fmt"
	"time"
	"net/http"
)

// ControlServerURL is the URL of the control server
var ControlServerURL string

// FetchPlatformTokens calls the control server to get valid platform tokens with timeout & retry

func FetchPlatformTokens() ([]string, error) {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.Get(ControlServerURL + "/platform-tokens")
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		var result map[string][]string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		tokens, ok := result["platform_tokens"]
		if !ok {
			lastErr = fmt.Errorf("platform_tokens missing")
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		return tokens, nil
	}

	return nil, fmt.Errorf("failed to fetch tokens: %w", lastErr)
}

// ValidatePlatformToken checks if a token exists in the list of valid tokens
func ValidatePlatformToken(token string, validTokens []string) bool {
	for _, validToken := range validTokens {
		if validToken == token {
			return true
		}
	}
	return false
}
