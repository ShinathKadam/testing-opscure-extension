package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"net"
	"os/exec"
	"sync"
	"path/filepath"
	"unicode/utf16"

	"gopkg.in/yaml.v3"
)

//
// ================= CONFIG =================
//

type Config struct {
	Server *ServerConfig        `yaml:"server,omitempty"`
	Apps   map[string]AppConfig `yaml:"apps"`
}

type ServerConfig struct {
	Addr         string `yaml:"addr,omitempty"`
	DefaultLines int    `yaml:"default_lines,omitempty"`
	MaxLines     int    `yaml:"max_lines,omitempty"`
}

type AppConfig struct {
	Logs map[string]LogTarget `yaml:"logs"`
}

type LogTarget struct {
	Type    string `yaml:"type"`
	Path    string `yaml:"path,omitempty"`
	URL     string `yaml:"url,omitempty"`
	Service string `yaml:"service"`
}

var globalConfig *Config

var fixClients = make(map[chan string]bool)
var fixMu sync.Mutex
var lastRollback []string
var rollbackMu sync.Mutex

var allowedPrefixes = []string{
	"git ",
	"sed ",
	"echo ",
	"docker ",
	"kubectl ",
}

//
// ================= STREAM MANAGER =================
//

// var streamMgr = NewStreamManager(DefaultStreamConfig())
var streamMgr *StreamManager

//
// ================= STREAM STATUS =================
//

type StreamStatus struct {
	Active    bool      `json:"active"`
	AppName   string    `json:"app_name,omitempty"`
	LogType   string    `json:"log_type,omitempty"`
	Path      string    `json:"path,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
}

var currentStream = &StreamStatus{}

// ================= FOLDER LOG BUFFER =================

var folderLogBuffer = struct {
	sync.Mutex
	Logs []map[string]interface{}
}{}

func addToFolderBuffer(log map[string]interface{}) {
	folderLogBuffer.Lock()
	folderLogBuffer.Logs = append(folderLogBuffer.Logs, log)
	folderLogBuffer.Unlock()

	// ✅ NEW: immediately push to connected users
	broadcastFolderLog(log)
}

func readFolderBuffer() []map[string]interface{} {
	folderLogBuffer.Lock()
	defer folderLogBuffer.Unlock()
	cp := make([]map[string]interface{}, len(folderLogBuffer.Logs))
	copy(cp, folderLogBuffer.Logs)
	return cp
}

func clearFolderBuffer() {
	folderLogBuffer.Lock()
	defer folderLogBuffer.Unlock()
	folderLogBuffer.Logs = nil
}

// ================= FOLDER STREAM BROADCAST =================

var folderClients = make(map[chan map[string]interface{}]bool)
var folderClientsMu sync.Mutex

func broadcastFolderLog(log map[string]interface{}) {
	folderClientsMu.Lock()
	for ch := range folderClients {
		select {
		case ch <- log:
		default:
		}
	}
	folderClientsMu.Unlock()
}

//
// ================= OUTPUT SCHEMA =================
//

type LogOutput struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
}

//
// ================= UTF-16 FILE READER =================
//

func readFileAutoUTF(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	if len(data) > 2 && data[0] == 0xFF && data[1] == 0xFE {
		u16 := make([]uint16, (len(data)-2)/2)
		for i := range u16 {
			u16[i] = binary.LittleEndian.Uint16(data[2+i*2:])
		}
		return string(utf16.Decode(u16)), nil
	}

	if len(data) > 2 && data[0] == 0xFE && data[1] == 0xFF {
		u16 := make([]uint16, (len(data)-2)/2)
		for i := range u16 {
			u16[i] = binary.BigEndian.Uint16(data[2+i*2:])
		}
		return string(utf16.Decode(u16)), nil
	}

	return string(data), nil
}

func readLastNLines(path string, n int) ([]string, error) {
	data, err := readFileAutoUTF(path)
	if err != nil {
		return nil, err
	}

	data = strings.ReplaceAll(data, "\r\n", "\n")
	lines := strings.Split(data, "\n")

	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out, nil
}

// ================= FOLDER LIVE TAIL =================

func tailFileContinuously(path string) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("[OPSCURE] tail open error:", err)
		return
	}
	defer f.Close()

	f.Seek(0, io.SeekEnd)
	reader := bufio.NewReader(f)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		addToFolderBuffer(map[string]interface{}{
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"level":     "INFO",
			"service":   filepath.Base(path),
			"message":   line,
			"file":      path,
			"folder":    filepath.Dir(path),
		})
	}
}

//
// ================= LOG SOURCES =================
//

type LogSource interface {
	ReadLogs(ctx context.Context, lines int) (string, error)
}

type FileLogSource struct {
	Path string
}

func (f *FileLogSource) ReadLogs(ctx context.Context, lines int) (string, error) {
	content, err := readFileAutoUTF(f.Path)
	if err != nil {
		return "", err
	}

	var all []string
	sc := bufio.NewScanner(strings.NewReader(content))
	for sc.Scan() {
		all = append(all, sc.Text())
	}

	if lines <= 0 || lines > len(all) {
		lines = len(all)
	}

	return strings.Join(all[len(all)-lines:], "\n"), nil
}

type APILogSource struct {
	URL string
}

func (a *APILogSource) ReadLogs(ctx context.Context, lines int) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, a.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), nil
}

//
// ================= HELPERS =================
//

func parseLines(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("lines"))
	if err != nil || n <= 0 {
		return globalConfig.Server.DefaultLines
	}
	if n > globalConfig.Server.MaxLines {
		return globalConfig.Server.MaxLines
	}
	return n
}

func sourceFromConfig(app, key string) (LogSource, LogTarget, error) {
	a, ok := globalConfig.Apps[app]
	if !ok {
		return nil, LogTarget{}, fmt.Errorf("unknown app")
	}
	t, ok := a.Logs[key]
	if !ok {
		return nil, LogTarget{}, fmt.Errorf("unknown log key")
	}
	if t.Type == "file" {
		return &FileLogSource{Path: t.Path}, t, nil
	}
	return &APILogSource{URL: t.URL}, t, nil
}

//
// ================= RAW LOG PARSER =================
//

var springLogRegex = regexp.MustCompile(
	`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\s+` +
		`(TRACE|DEBUG|INFO|WARN|ERROR)\s+` +
		`\d+\s+---\s+\[.*?\]\s+` +
		`([^\s]+)\s*:\s*(.*)$`,
)

func parseRawLogLine(line, severity string) map[string]interface{} {
	m := springLogRegex.FindStringSubmatch(line)

	// default timestamp = now
	ts := time.Now().UTC()

	if len(m) == 0 {
		svc := "unknown"

		// Try to extract service from last token before colon
		if idx := strings.LastIndex(line, ":"); idx > 0 {
			parts := strings.Fields(line[:idx])
			if len(parts) > 0 {
				svc = parts[len(parts)-1]
			}
		}

		return map[string]interface{}{
			"timestamp": ts.Format(time.RFC3339),
			"level":     severity,
			"service":   svc,
			"message":   strings.TrimSpace(line),
		}
	}

	// parsed Spring timestamp
	parsedTs, err := time.Parse("2006-01-02 15:04:05.000", m[1])
	if err == nil {
		ts = parsedTs.UTC()
	}

	return map[string]interface{}{
		"timestamp": ts.Format(time.RFC3339),
		"level":     m[2],
		"service":   m[3],
		"message":   m[4],
	}
}

//
// ================= STREAM INGEST (JSON) =================
//

type IncomingLog struct {
	Severity  string 				 `json:"severity"`
	Timestamp string 				 `json:"timestamp"`
	Message   string 				 `json:"message"`
	Raw       string 				 `json:"raw"`
	Source    map[string]interface{} `json:"source,omitempty"`
}

type StreamIngestRequest struct {
	Logs []IncomingLog `json:"logs"`
	Service string        `json:"service"`
}

// In main.go: Replace the existing streamIngestHandler function
func streamIngestHandler(w http.ResponseWriter, r *http.Request) {
	if streamMgr == nil {
		http.Error(w, "agent warming up, retry in 1s", 503)
		return
	}

	var req StreamIngestRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", 400)
		return
	}

	if len(req.Logs) == 0 {
		http.Error(w, "logs array is empty", 400)
		return
	}

	accepted := 0
	errorTriggered := false

	for _, l := range req.Logs {
		raw := strings.TrimSpace(l.Raw)
		if raw == "" {
			raw = strings.TrimSpace(l.Message)
		}
		if raw == "" {
			continue
		}

		log := parseRawLogLine(raw, l.Severity)

		// ALWAYS prefer request timestamp if provided
		if l.Timestamp != "" {
			if ts, err := time.Parse(time.RFC3339, l.Timestamp); err == nil {
				log["timestamp"] = ts.UTC().Format(time.RFC3339)
			}
		}


		// preserve source if provided
		if l.Source != nil {
			log["source"] = l.Source
		}

		if req.Service != "" {
			log["service"] = req.Service
		}
		
		// Capture both the acceptance and the error trigger status
		ok, isErr := streamMgr.Ingest(log)
		if ok {
			accepted++
			if isErr {
				errorTriggered = true
			}
		} else {
			// If not accepted, check if it's because of expiration
			if streamMgr.IsExpired() {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusGone)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "stream_expired",
				})
				return
			}
		}
	}

	var bundle *CorrelationBundle
	flushed := false

	// Flush immediately if an ERROR was found OR if buffer limit reached
	if errorTriggered {
		bundle = streamMgr.FlushWithReason(FlushErrorDetected)
		flushed = bundle != nil
	} else if streamMgr.ShouldFlush() {
		bundle = streamMgr.FlushWithReason(FlushBufferFull)
		flushed = bundle != nil
	}

	w.Header().Set("Content-Type", "application/json")
	resp := StreamIngestResponse{
		Accepted: accepted,
	}

	if flushed && bundle != nil {
		resp.Status = "flushed"
		reason := bundle.Metadata["flush_reason"].(string)
		resp.FlushReason = &reason
		resp.Bundle = bundle.Metadata["api_bundle"].(*StreamBundleResponse)
	} else {
		resp.Status = "buffering"
	}

	json.NewEncoder(w).Encode(resp)

}

type FixApplyRequest struct {
    AIResponse map[string]interface{} `json:"ai_response"`
    Workspace  string                 `json:"workspace"`
    DryRun     bool                   `json:"dry_run"`
}

func fixApplyHandler(w http.ResponseWriter, r *http.Request) {
	var req FixApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", 400)
		return
	}

	analyze, ok := req.AIResponse["analyze_response"].(map[string]interface{})
	if !ok {
		http.Error(w, "missing analyze_response", 400)
		return
	}

	recBlock, ok := analyze["recommendation"].(map[string]interface{})
	if !ok {
		http.Error(w, "missing recommendation block", 400)
		return
	}

	if auto, ok := recBlock["auto_heal_candidate"].(bool); ok && !auto {
		http.Error(w, "AI marked fix unsafe", 403)
		return
	}

	recs, ok := recBlock["recommendations"].([]interface{})
	if !ok || len(recs) == 0 {
		http.Error(w, "no recommendations found", 400)
		return
	}

	first, _ := recs[0].(map[string]interface{})

	// ---- rollback extraction ----
	if rb, ok := first["rollback"].(map[string]interface{}); ok {
      if arr, ok := rb["commands"].([]interface{}); ok {
          rollbackMu.Lock()
          lastRollback = nil
          for _, c := range arr {
              lastRollback = append(lastRollback, fmt.Sprintf("%v", c))
          }
          rollbackMu.Unlock()
      }
  	}


	impl, ok := first["implementation"].(map[string]interface{})
	if !ok {
		http.Error(w, "no implementation found", 400)
		return
	}

	cmdsAny, ok := impl["commands"].([]interface{})
	if !ok || len(cmdsAny) == 0 {
		http.Error(w, "no commands found", 400)
		return
	}

	var cmds []string
	for _, c := range cmdsAny {
		cmds = append(cmds, fmt.Sprintf("%v", c))
	}

	go runFixWorkflow(cmds, req.Workspace, req.DryRun)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "started",
		"commands": cmds,
		"dry_run":  req.DryRun,
	})
}

func findGitBash() string {
	candidates := []string{
		`C:\Program Files\Git\bin\bash.exe`,
		`C:\Program Files\Git\usr\bin\bash.exe`,
		`C:\Program Files (x86)\Git\bin\bash.exe`,
		`C:\Program Files (x86)\Git\usr\bin\bash.exe`,
	}

	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func runGitOutput(workspace string, args ...string) (string, error) {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = workspace
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func detectDefaultBranch(workspace string) string {
	out, err := runGitOutput(workspace, "git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD")
	if err == nil && strings.Contains(out, "/") {
		parts := strings.Split(out, "/")
		return parts[len(parts)-1]
	}

	// fallback
	if _, err := runGitOutput(workspace, "git", "rev-parse", "--verify", "main"); err == nil {
		return "main"
	}
	return "master"
}

func hasUpstream(workspace string) bool {
	_, err := runGitOutput(workspace, "git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
	return err == nil
}

func hasGitHubAuth(workspace string) bool {
	// This checks if GitHub can be accessed with current credentials
	_, err := runGitOutput(workspace, "git", "ls-remote", "origin")
	return err == nil
}

func runFixWorkflow(cmds []string, workspace string, dry bool) {
	branch := detectDefaultBranch(workspace)
	broadcastFix("INFO: detected default branch → " + branch)

	for i, c := range cmds {

		c = strings.ReplaceAll(c, "git checkout master", "git checkout "+branch)
		c = strings.ReplaceAll(c, "git pull origin master", "git pull origin "+branch)
		c = strings.ReplaceAll(c, "git push origin master", "git push origin "+branch)
		c = strings.ReplaceAll(c, "git checkout main", "git checkout "+branch)
		c = strings.ReplaceAll(c, "git pull origin main", "git pull origin "+branch)
		c = strings.ReplaceAll(c, "git push origin main", "git push origin "+branch)

		// ✅ BRANCH SAFE HANDLING
		if strings.HasPrefix(strings.TrimSpace(c), "git checkout ") {
			parts := strings.Fields(c)
			if len(parts) == 3 && parts[1] == "checkout" {
				br := parts[2]

				if branchExists(workspace, br) {
					c = "git checkout " + br
				} else {
					c = "git checkout -b " + br
				}
			}
		}

		cmds[i] = c
	}

	// safety: git environment checks
	if !dry {

		if !hasGitHubAuth(workspace) {
			broadcastFix("ERROR: GitHub authentication failed. Please login (git push once manually).")
			broadcastFix("__CLOSE__")
			return
		}

		if !hasUpstream(workspace) {
			broadcastFix("INFO: No upstream branch found. Setting upstream to origin/" + branch)

			_, err := runGitOutput(workspace, "git", "branch", "--set-upstream-to=origin/"+branch)
			if err != nil {
				broadcastFix("ERROR: Failed to set upstream branch.")
				broadcastFix("__CLOSE__")
				return
			}
		}
	}

	if !filepath.IsAbs(workspace) {
		broadcastFix("INVALID WORKSPACE PATH")
		broadcastFix("__CLOSE__")
		return
	}

	logFile := filepath.Join(workspace, ".opscure-fix.log")
	f, _ := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	for _, cmd := range cmds {

		if !isAllowed(cmd) {
			broadcastFix("BLOCKED: " + cmd)
			return
		}

		broadcastFix("RUN: " + cmd)

		if dry {
			broadcastFix("DRY-RUN")
			continue
		}

		var c *exec.Cmd

		if os.PathSeparator == '\\' {
			gitBash := findGitBash()
			if gitBash == "" {
				broadcastFix("ERROR: Git Bash not found. Please install Git for Windows.")
				broadcastFix("__CLOSE__")
				return
			}

			// Git Bash
			c = exec.Command(gitBash, "-lc", cmd)

		} else {
			// Linux / macOS
			c = exec.Command("bash", "-lc", cmd)
		}


		c.Dir = workspace
		stdout, _ := c.StdoutPipe()
		stderr, _ := c.StderrPipe()

		c.Start()
		go streamPipe(stdout)
		go streamPipe(stderr)

		if err := c.Wait(); err != nil {
			broadcastFix("ERROR: " + err.Error())
			broadcastFix("AUTO-ROLLBACK")
			
			rollbackMu.Lock()
			rb := append([]string{}, lastRollback...)
			rollbackMu.Unlock()

			if len(rb) > 0 {
				runFixWorkflow(rb, workspace, false)
			}

			broadcastFix("WORKFLOW STOPPED")
			broadcastFix("__CLOSE__")
			return
		}

	}

	broadcastFix("DONE")
	broadcastFix("RESTART_APP")
	broadcastFix("__CLOSE__")
}

func broadcastFix(msg string) {
	fixMu.Lock()
	for ch := range fixClients {
		select {
		case ch <- msg:
		default:
		}
	}
	fixMu.Unlock()
}

func fixStreamHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 400)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch := make(chan string, 20)

	fixMu.Lock()
	fixClients[ch] = true
	fixMu.Unlock()

	defer func() {
		fixMu.Lock()
		delete(fixClients, ch)
		fixMu.Unlock()
		close(ch)
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func isAllowed(cmd string) bool {
	l := strings.ToLower(strings.TrimSpace(cmd))
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(l, p) {
			return true
		}
	}
	return false
}

func fixRollbackHandler(w http.ResponseWriter, r *http.Request) {
    rollbackMu.Lock()
    cmds := append([]string{}, lastRollback...)
    rollbackMu.Unlock()

    go runFixWorkflow(cmds, ".", false)
    w.Write([]byte(`{"status":"rollback_started"}`))
}

func streamPipe(r io.ReadCloser) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		broadcastFix(sc.Text())
	}
}

//
// ================= OTHER HANDLERS (UNCHANGED) =================
//

func folderLogsHandler(w http.ResponseWriter, r *http.Request) {

	raw := r.URL.Query().Get("paths")
	if raw == "" {
		http.Error(w, "paths are required", 400)
		return
	}

	folders := strings.Split(raw, "|")

	clearFolderBuffer()

	for _, f := range folders {
		go startFolderLogMonitoring(strings.TrimSpace(f))
	}

	// small warm-up
	time.Sleep(50 * time.Millisecond)

	logs := readFolderBuffer()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"folders": folders,
		// "files":  len(discoverWorkspaceLogs(folders)),
		"logs":   logs,
	})
}

func folderLogsStreamHandler(w http.ResponseWriter, r *http.Request) {

	raw := r.URL.Query().Get("paths")
	if raw == "" {
		http.Error(w, "paths are required", 400)
		return
	}

	folders := strings.Split(raw, "|")

	clearFolderBuffer()

	for _, f := range folders {
		go startFolderLogMonitoring(strings.TrimSpace(f))
	}


	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan map[string]interface{}, 20)

	// register client
	folderClientsMu.Lock()
	folderClients[ch] = true
	folderClientsMu.Unlock()

	// remove on disconnect
	defer func() {
		folderClientsMu.Lock()
		delete(folderClients, ch)
		folderClientsMu.Unlock()
		close(ch)
		fmt.Println("[OPSCURE] folder stream closed")
	}()

	// send already collected logs first
	for _, l := range readFolderBuffer() {
		b, _ := json.Marshal(l)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}

	// keep streaming new logs
	for {
		select {
		case <-r.Context().Done():
			return
		case log := <-ch:
			b, _ := json.Marshal(log)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func logsHandler(w http.ResponseWriter, r *http.Request) {
	app := r.URL.Query().Get("app")
	key := r.URL.Query().Get("log")

	src, target, err := sourceFromConfig(app, key)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	raw, _ := src.ReadLogs(r.Context(), parseLines(r))
	sc := bufio.NewScanner(strings.NewReader(raw))

	var out []LogOutput
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			out = append(out, LogOutput{
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Level:     "INFO",
				Service:   target.Service,
				Message:   line,
			})
		}
	}

	json.NewEncoder(w).Encode(out)
}

func streamStatusHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(currentStream)
}

func streamLiveHandler(w http.ResponseWriter, r *http.Request) {
	if streamMgr == nil {
		http.Error(w, "agent warming up, retry in 1s", 503)
		return
	}
	
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	flusher, _ := w.(http.Flusher)
	ch := streamMgr.Subscribe()
	defer streamMgr.Unsubscribe(ch)

	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-ch:
			if apiBundle, ok := b.Metadata["api_bundle"].(*StreamBundleResponse); ok {

				payload := map[string]interface{}{
					"bundle": apiBundle,
				}

				j, _ := json.Marshal(payload)
				fmt.Fprintf(w, "data: %s\n\n", j)
				flusher.Flush()
			}
		}
	}
}

//
// ================= RESPONSE SCHEMA =================
//

type AnalyzeResponse struct {
	Bundle AnalyzeBundle `json:"bundle"`
	UseRag bool          `json:"use_rag"`
	TopK   int           `json:"top_k"`
}

type AnalyzeBundle struct {
	ID                   string              `json:"id"`
	WindowStart          string              `json:"windowStart"`
	WindowEnd            string              `json:"windowEnd"`
	RootService          string              `json:"rootService"`
	AffectedServices     []string            `json:"affectedServices"`
	LogPatterns          []AnalyzeLogPattern `json:"logPatterns"`
	Events               []AnalyzeEvent      `json:"events"`
	Metrics              AnalyzeMetrics      `json:"metrics"`
	DependencyGraph      []string            `json:"dependencyGraph"`
	DerivedRootCauseHint string              `json:"derivedRootCauseHint"`
	GitConfig            interface{}         `json:"git_config,omitempty"`
}

type AnalyzeLogPattern struct {
	Pattern         string  `json:"pattern"`
	Count           int     `json:"count"`
	FirstOccurrence string  `json:"firstOccurrence"`
	LastOccurrence  string  `json:"lastOccurrence"`
	ErrorClass      *string `json:"errorClass"`
}

type AnalyzeEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"`
}

type AnalyzeMetrics struct {
	CPUZ       float64 `json:"cpuZ"`
	LatencyZ   float64 `json:"latencyZ"`
	ErrorRateZ float64 `json:"errorRateZ"`
}

type PreprocessCombinedResponse struct {
	PreprocessResponse AnalyzeResponse `json:"preprocess_response"`
	AnalyzeResponse    json.RawMessage `json:"analyze_response"`
}

type StreamIngestResponse struct {
	Status       string                 `json:"status"`
	Accepted     int                    `json:"accepted"`
	FlushReason  *string                `json:"flush_reason,omitempty"`
	Bundle       *StreamBundleResponse  `json:"bundle,omitempty"`
}

type StreamBundleResponse struct {
	ID               string                 `json:"id"`
	WindowStart      string                 `json:"windowStart"`
	WindowEnd        string                 `json:"windowEnd"`
	RootService      string                 `json:"rootService"`
	AffectedServices []string               `json:"affectedServices"`
	LogPatterns      []StreamLogPattern     `json:"logPatterns"`
	FlushMetadata    StreamFlushMetadata    `json:"flush_metadata"`
}

type StreamLogPattern struct {
	Pattern          string                 `json:"pattern"`
	Count            int                    `json:"count"`
	FirstOccurrence  string                 `json:"firstOccurrence"`
	LastOccurrence   string                 `json:"lastOccurrence"`
	ErrorClass       string                 `json:"errorClass"`
	LogSource        map[string]string      `json:"logSource"`
}

type StreamFlushMetadata struct {
	Reason     string `json:"reason"`
	LogCount   int    `json:"log_count"`
	FlushedAt string `json:"flushed_at"`
}

func firstNonEmpty(list []string) string {
    for _, v := range list {
        if v != "" {
            return v
        }
    }
    return ""
}

//
// ================= PREPROCESS HANDLER (FIXED DYNAMICALLY) =================
//

func preprocessHandler(w http.ResponseWriter, r *http.Request) {
	// Recover from any panic to ensure we always return a JSON response
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("preprocessHandler panic: %v\n", rec)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			errObj := map[string]string{"error": fmt.Sprintf("panic: %v", rec)}
			errB, _ := json.Marshal(errObj)
			fallback := PreprocessCombinedResponse{
				PreprocessResponse: AnalyzeResponse{UseRag: true, TopK: 0},
				AnalyzeResponse:    json.RawMessage(errB),
			}
			b, _ := json.Marshal(fallback)
			w.Write(b)
		}
	}()

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON body", 400)
		return
	}

	var gitConfig interface{}

	if bundle, ok := payload["bundle"].(map[string]interface{}); ok {
		if gc, ok := bundle["git_config"]; ok {
			gitConfig = gc
		}
	}


	var rawData []map[string]interface{}
	inputBundleID := ""

	if bundle, ok := payload["bundle"].(map[string]interface{}); ok {
		if id, ok := bundle["id"].(string); ok {
			inputBundleID = id
		}
		if seq, ok := bundle["Sequence"].([]interface{}); ok {
			for _, item := range seq {
				if itmMap, ok := item.(map[string]interface{}); ok {
					if data, ok := itmMap["Data"].(map[string]interface{}); ok {
						rawData = append(rawData, data)
					}
				}
			}
		}
	}

	if len(rawData) == 0 {
		if bundle, ok := payload["bundle"].(map[string]interface{}); ok {
			if patterns, ok := bundle["logPatterns"].([]interface{}); ok {
				for _, p := range patterns {
					if pm, ok := p.(map[string]interface{}); ok {
						rawData = append(rawData, map[string]interface{}{
							"timestamp": pm["firstOccurrence"],
							"level":     "ERROR",
							"service":   "unknown",
							"message":   pm["pattern"],
							"errorClass": pm["errorClass"],
						})
					}
				}
			}
		}
	}

	processor := NewLogPreprocessorFullGo()
	bundle, err := processor.Process(rawData, inputBundleID)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var patterns []AnalyzeLogPattern
	for _, p := range bundle.LogPatterns {
		patterns = append(patterns, AnalyzeLogPattern{
			Pattern:         p.Pattern,
			Count:           p.Count,
			FirstOccurrence: p.FirstOccurrence,
			LastOccurrence:  p.LastOccurrence,
			ErrorClass:      p.ErrorClass,
		})
	}

	// Convert bundle.Events ([]string) to []AnalyzeEvent
	var events []AnalyzeEvent
	if bundle.Events != nil {
		for i, e := range bundle.Events {
			events = append(events, AnalyzeEvent{
				ID:        fmt.Sprintf("evt_%d", i+1),
				Type:      "observation",
				Reason:    e,
				Service:   firstNonEmpty(bundle.AffectedServices),
				Timestamp: bundle.WindowStart,
			})
		}
	} else {
		events = []AnalyzeEvent{}
	}

	rootSvc := ""
	if bundle.RootService != nil {
		rootSvc = *bundle.RootService
	}



	// -------- EXISTING PREPROCESS RESPONSE (UNCHANGED) --------
	preprocessResponse := AnalyzeResponse{
		Bundle: AnalyzeBundle{
			ID:                   bundle.ID,
			WindowStart:          bundle.WindowStart,
			WindowEnd:            bundle.WindowEnd,
			RootService:          rootSvc,
			AffectedServices:     bundle.AffectedServices,
			LogPatterns:          patterns,
			Events:               events,
			Metrics: AnalyzeMetrics{
				CPUZ:       bundle.Metrics.CPUZ,
				LatencyZ:   bundle.Metrics.LatencyZ,
				ErrorRateZ: bundle.Metrics.ErrorRateZ,
			},
			DependencyGraph:      bundle.DependencyGraph,
			DerivedRootCauseHint: bundle.DerivedRootCauseHint,
			GitConfig:            gitConfig,

		},
		UseRag: true,
		TopK:   5,
	}

	// -------- CALL /ai/analyze WITH SAME BODY --------
	var analyzeRespRaw json.RawMessage

	reqBody, _ := json.Marshal(preprocessResponse)
	req, err := http.NewRequest(
		http.MethodPost,
		"http://18.191.159.155:8000/ai/analyze",
		strings.NewReader(string(reqBody)),
	)
	if err != nil {
		// Request construction failed — return an error payload (non-null)
		errObj := map[string]string{"error": fmt.Sprintf("request creation failed: %s", err.Error())}
		b, _ := json.Marshal(errObj)
		analyzeRespRaw = json.RawMessage(b)
		fmt.Printf("analyze request creation failed: %v\n", err)
	} else {
		req.Header.Set("Content-Type", "application/json")
		// Wait up to 2 minutes for the analyze service to respond
		client := &http.Client{Timeout: 2 * time.Minute}

		// Perform the request and wait for completion (blocking)
		resp, err := client.Do(req)
		if err != nil {
			// Network or other error — return an error payload (non-null)
			errObj := map[string]string{"error": fmt.Sprintf("analyze request failed: %s", err.Error())}
			b, _ := json.Marshal(errObj)
			analyzeRespRaw = json.RawMessage(b)
			fmt.Printf("analyze request failed: %v\n", err)
		} else {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if len(body) == 0 {
				analyzeRespRaw = json.RawMessage([]byte("{}"))
			} else {
				analyzeRespRaw = json.RawMessage(body)
			}
		}
	}

	// -------- COMBINED RESPONSE --------
	finalResponse := PreprocessCombinedResponse{
		PreprocessResponse: preprocessResponse,
		AnalyzeResponse:    analyzeRespRaw,
	}

	w.Header().Set("Content-Type", "application/json")
	// Use json.Marshal and explicit write so we can fallback and log on failure
	respBytes, err := json.Marshal(finalResponse)
	if err != nil {
		fmt.Printf("failed to marshal finalResponse: %v\n", err)
		respBytes = []byte(`{"preprocess_response":{"bundle":{"id":""},"use_rag":true,"top_k":5},"analyze_response":{"error":"marshal failed"}}`)
	}
	if _, err := w.Write(respBytes); err != nil {
		fmt.Printf("failed to write /logs/preprocess response: %v\n", err)
	}
}


// helper to safely convert slice of any to []string
func toStringSlice(src interface{}) []string {
	var out []string
	if src == nil {
		return out
	}
	switch v := src.(type) {
	case []string:
		return v
	case []interface{}:
		for _, s := range v {
			out = append(out, fmt.Sprintf("%v", s))
		}
	}
	return out
}

func listenWithAutoPort(addr string) (net.Listener, string, error) {

	host := "127.0.0.1"

	// most-users-free ports
	ports := []string{"5173", "3000", "5000", "8000", "8080"}

	// Try user provided addr first
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return ln, ln.Addr().String(), nil
	}

	fmt.Println("[OPSCURE] default addr busy → scanning common free ports")

	// Try common free ports (very fast)
	for _, p := range ports {
		tryAddr := host + ":" + p
		ln, err := net.Listen("tcp", tryAddr)
		if err == nil {
			fmt.Println("[OPSCURE] bound to free port:", p)
			return ln, ln.Addr().String(), nil
		}
	}

	// Fallback → OS picks a free port
	fmt.Println("[OPSCURE] all common ports busy → asking OS for free port")
	ln, err = net.Listen("tcp", host+":0")
	if err != nil {
		return nil, "", err
	}

	return ln, ln.Addr().String(), nil
}

func writeAgentPort(port string) {
	exe, _ := os.Executable()
	base := filepath.Dir(exe)

	// extension reads: server/agent.port
	p := filepath.Join(base, "agent.port")
	_ = os.WriteFile(p, []byte(port), 0644)
}

func branchExists(workspace, name string) bool {
	_, err := runGitOutput(workspace, "git", "show-ref", "--verify", "--quiet", "refs/heads/"+name)
	return err == nil
}

// --- NEW: Global Log Discovery ---

func discoverWorkspaceLogs(workspace string) []string {
	var files []string

	filepath.WalkDir(workspace, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}

		lower := strings.ToLower(d.Name())

		if strings.HasSuffix(lower, ".log") ||
			strings.HasSuffix(lower, ".out") ||
			strings.HasSuffix(lower, ".txt") ||
			strings.Contains(lower, "catalina") ||
			strings.Contains(lower, "access") ||
			strings.Contains(lower, "gc") {

			files = append(files, path)
		}

		return nil
	})

	return files
}

func startFolderLogMonitoring(folder string) {

	logFiles := discoverWorkspaceLogs(folder)

	fmt.Println("[OPSCURE] Monitoring folder:", folder)

	for _, file := range logFiles {

		// send last 10 lines first
		lastLines, err := readLastNLines(file, 10)
		if err == nil {
			for _, line := range lastLines {
				addToFolderBuffer(map[string]interface{}{
					"timestamp": time.Now().UTC().Format(time.RFC3339),
					"level":     "INFO",
					"service":   filepath.Base(file),
					"message":   line,
					"file":      file,
					"folder":    filepath.Dir(file),
				})
			}
		}

		// start live tail
		go tailFileContinuously(file)
	}
}

//
// ================= MAIN =================
//

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "")
	cfg := flag.String("config", "", "")
	flag.Parse()

	if *cfg != "" {
		b, _ := os.ReadFile(*cfg)
		yaml.Unmarshal(b, &globalConfig)
	}

	if globalConfig == nil {
		globalConfig = &Config{}
	}
	if globalConfig.Server == nil {
		globalConfig.Server = &ServerConfig{DefaultLines: 100, MaxLines: 1000}
	}

	http.HandleFunc("/logs", logsHandler)
	http.HandleFunc("/stream/ingest", streamIngestHandler)
	http.HandleFunc("/stream/status", streamStatusHandler)
	http.HandleFunc("/stream/live", streamLiveHandler)
	http.HandleFunc("/logs/preprocess", preprocessHandler)
	http.HandleFunc("/fix/apply", fixApplyHandler)
	http.HandleFunc("/fix/stream", fixStreamHandler)
	http.HandleFunc("/fix/rollback", fixRollbackHandler)
	http.HandleFunc("/logs/folder/read", folderLogsHandler)
	http.HandleFunc("/logs/folder/stream", folderLogsStreamHandler)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})


	// AUTO PORT LOGIC
	ln, actualAddr, err := listenWithAutoPort(*addr)
	if err != nil {
		panic(err)
	}

	_, port, _ := net.SplitHostPort(actualAddr)

	// write port so VS Code extension can read
	writeAgentPort(port)

	fmt.Println("[OPSCURE] Agent starting on", actualAddr)

	// Start server immediately (non-blocking)
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			panic(err)
		}
	}()

	fmt.Println("[OPSCURE] Agent READY on", actualAddr)
	// Heavy init in background (does NOT block startup)
	go func() {
		fmt.Println("[OPSCURE] Initializing stream manager...")
		streamMgr = NewStreamManager(DefaultStreamConfig())
		fmt.Println("[OPSCURE] Stream manager ready")
	}()

	// Block forever
	select {}

}
