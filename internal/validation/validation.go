package validation

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ControlServerURL is the URL of the control server
var ControlServerURL string

// FetchPlatformTokens calls the control server to get valid platform tokens
func FetchPlatformTokens() ([]string, error) {
	client := &http.Client{}

	resp, _ := client.Get(ControlServerURL + "/platform-tokens")

	defer resp.Body.Close()

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	tokens, ok := result["platform_tokens"]
	if !ok {
		return nil, fmt.Errorf("platform_tokens not found in response")
	}

	return tokens, nil
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
