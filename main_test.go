package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestTokenExtractionAndAdd(t *testing.T) {
	tp := NewTokenPool()

	// Test 1: GitHubTokenRegex matches ghp, github_pat, gho, ghu with various lengths
	testTokens := []string{
		"ghp_1234567890abcdefghijklmnopqrstuvwxyz",
		"github_pat_11AEXAMPLE0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_longpattoken",
		"gho_dummyOAuthToken1234567890abcdefghijklmn",
		"ghu_dummyUserToServerToken1234567890abcdefgh",
	}

	for _, tok := range testTokens {
		matched := GitHubTokenRegex.FindAllString(tok, -1)
		if len(matched) == 0 {
			t.Errorf("Expected token %s to match regex, but it didn't", tok)
		}
	}

	// Test 2: /add logic
	input := `/add ghp_111111111111111111111111111111111111
github_pat_2222222222222222222222_33333333333333333333333333333333333333333333333333333333333333333333
custom_token_without_prefix`

	trimmed := strings.TrimSpace(input)
	parts := strings.Fields(trimmed)
	cmd := strings.ToLower(parts[0])
	if cmd != "/add" {
		t.Fatalf("Expected cmd to be /add")
	}

	rawTokens := parts[1:]
	var candidateTokens []string
	matched := GitHubTokenRegex.FindAllString(trimmed, -1)
	if len(matched) > 0 {
		candidateTokens = append(candidateTokens, matched...)
	}
	for _, token := range rawTokens {
		tkn := strings.TrimSpace(token)
		if tkn != "" {
			candidateTokens = append(candidateTokens, tkn)
		}
	}

	added := tp.AddTokens(candidateTokens)
	if added != 3 {
		t.Errorf("Expected 3 unique tokens added, got %d", added)
	}
	if tp.ActiveCount() != 3 {
		t.Errorf("Expected 3 active tokens in RAM pool, got %d", tp.ActiveCount())
	}

	// Test duplicate addition
	addedAgain := tp.AddTokens(candidateTokens)
	if addedAgain != 0 {
		t.Errorf("Expected 0 tokens added on duplicate, got %d", addedAgain)
	}

	// Purge
	purged := tp.PurgeAll()
	if purged != 3 || tp.ActiveCount() != 0 {
		t.Errorf("Expected 3 purged, pool active count 0, got purged=%d, active=%d", purged, tp.ActiveCount())
	}
}

func TestHTTPServerEndpoints(t *testing.T) {
	tp := NewTokenPool()
	ds := NewDeduplicationStore(t.TempDir())

	server := StartHTTPServer("0", tp, ds)
	defer server.Close()

	handler := server.Handler

	// 1. Test GET /status
	reqStatus := httptest.NewRequest(http.MethodGet, "/status", nil)
	rrStatus := httptest.NewRecorder()
	handler.ServeHTTP(rrStatus, reqStatus)

	if rrStatus.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /status, got %d", rrStatus.Code)
	}
	var statusData map[string]interface{}
	if err := json.Unmarshal(rrStatus.Body.Bytes(), &statusData); err != nil {
		t.Fatalf("Failed to parse /status json: %v", err)
	}
	if statusData["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", statusData["status"])
	}

	// 2. Test POST /add with raw multiline curl body
	rawCurlBody := `ghp_tokenA11111111111111111111111111111111
ghp_tokenB22222222222222222222222222222222
github_pat_tokenC3333333333333333333333333333333333333333333333333333333333333333333333333333`

	reqAddRaw := httptest.NewRequest(http.MethodPost, "/add", strings.NewReader(rawCurlBody))
	rrAddRaw := httptest.NewRecorder()
	handler.ServeHTTP(rrAddRaw, reqAddRaw)

	if rrAddRaw.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for POST /add raw body, got %d, body: %s", rrAddRaw.Code, rrAddRaw.Body.String())
	}

	var addResp map[string]interface{}
	if err := json.Unmarshal(rrAddRaw.Body.Bytes(), &addResp); err != nil {
		t.Fatalf("Failed to parse /add json response: %v", err)
	}
	if added, ok := addResp["added"].(float64); !ok || int(added) != 3 {
		t.Errorf("Expected 3 added tokens, got %v", addResp["added"])
	}
	if tp.ActiveCount() != 3 {
		t.Errorf("Expected active RAM tokens to be 3, got %d", tp.ActiveCount())
	}

	// 3. Test POST /add with JSON payload
	jsonPayload := `{"tokens":["ghp_tokenD44444444444444444444444444444444", "ghp_tokenA11111111111111111111111111111111"]}`
	reqAddJSON := httptest.NewRequest(http.MethodPost, "/add", bytes.NewBufferString(jsonPayload))
	reqAddJSON.Header.Set("Content-Type", "application/json")
	rrAddJSON := httptest.NewRecorder()
	handler.ServeHTTP(rrAddJSON, reqAddJSON)

	if rrAddJSON.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for POST /add JSON, got %d", rrAddJSON.Code)
	}
	var addJSONResp map[string]interface{}
	_ = json.Unmarshal(rrAddJSON.Body.Bytes(), &addJSONResp)
	// tokenA was duplicate, tokenD was new -> 1 added
	if added, ok := addJSONResp["added"].(float64); !ok || int(added) != 1 {
		t.Errorf("Expected 1 newly added token from JSON payload, got %v", addJSONResp["added"])
	}
	if tp.ActiveCount() != 4 {
		t.Errorf("Expected active RAM tokens to be 4, got %d", tp.ActiveCount())
	}

	// 4. Test Optional Auth
	os.Setenv("HARVESTER_AUTH_TOKEN", "supersecret123")
	defer os.Unsetenv("HARVESTER_AUTH_TOKEN")

	// Unauthorized attempt
	reqUnauth := httptest.NewRequest(http.MethodPost, "/add", strings.NewReader("ghp_unauthorized"))
	rrUnauth := httptest.NewRecorder()
	handler.ServeHTTP(rrUnauth, reqUnauth)
	if rrUnauth.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized, got %d", rrUnauth.Code)
	}

	// Authorized attempt via X-Harvester-Token header
	reqAuthHeader := httptest.NewRequest(http.MethodPost, "/add", strings.NewReader("ghp_authorizedToken555555555555555555555555"))
	reqAuthHeader.Header.Set("X-Harvester-Token", "supersecret123")
	rrAuthHeader := httptest.NewRecorder()
	handler.ServeHTTP(rrAuthHeader, reqAuthHeader)
	if rrAuthHeader.Code != http.StatusOK {
		t.Errorf("Expected 200 OK with header, got %d", rrAuthHeader.Code)
	}

	// Authorized attempt via Bearer header
	reqBearer := httptest.NewRequest(http.MethodPost, "/add", strings.NewReader("ghp_authorizedToken666666666666666666666666"))
	reqBearer.Header.Set("Authorization", "Bearer supersecret123")
	rrBearer := httptest.NewRecorder()
	handler.ServeHTTP(rrBearer, reqBearer)
	if rrBearer.Code != http.StatusOK {
		t.Errorf("Expected 200 OK with Bearer token, got %d", rrBearer.Code)
	}

	// 5. Masking check
	masked := maskToken("ghp_1234567890abcdefghijklmnopqrstuvwxyz")
	if masked != "ghp_...wxyz" {
		t.Errorf("Unexpected masked result: %s", masked)
	}
}
