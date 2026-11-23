package main

import (
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
