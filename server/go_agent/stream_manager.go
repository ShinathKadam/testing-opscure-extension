package main

import (
	"encoding/json"
	"sync"
	"time"
	"regexp"
)

//
// ===================== PLACEHOLDERS / DEPENDENCIES =====================
//

type RawLog struct {
	Data      map[string]interface{}
	Timestamp time.Time
	Level     string
	Service   string
}

type CorrelationBundle struct {
	Sequence []RawLog
	Metadata map[string]interface{}
}

func (c *CorrelationBundle) ModelDumpJSON() string {
	b, _ := json.Marshal(c)
	return string(b)
}

type LogPreprocessor struct {
	Parser  *LogParser
	Factory *BundleFactory
}

func NewLogPreprocessor() *LogPreprocessor {
	return &LogPreprocessor{
		Parser:  &LogParser{},
		Factory: &BundleFactory{},
	}
}

type LogParser struct{}

func (p *LogParser) ParseLogs(logs []map[string]interface{}) ([]RawLog, error) {
	var parsed []RawLog
	for _, l := range logs {
		parsed = append(parsed, RawLog{Data: l})
	}
	return parsed, nil
}

type BundleFactory struct{}

func (f *BundleFactory) CreateBundle(logs []RawLog, patterns []string) *CorrelationBundle {
	return &CorrelationBundle{
		Sequence: logs,
		Metadata: map[string]interface{}{
			"patterns": patterns,
		},
	}
}

//
// ===================== CONFIG & STATS =====================
//

type FlushReason string

const (
	FlushErrorDetected FlushReason = "error_detected"
	FlushTimeElapsed   FlushReason = "time_elapsed"
	FlushManual        FlushReason = "manual"
	FlushBufferFull    FlushReason = "buffer_full"
	FlushShutdown      FlushReason = "shutdown"
	FlushExpired	   FlushReason = "expired"
)

type StreamConfig struct {
	MaxLogsPerSecond       int
	MaxBufferSize          int
	MaxTokensPerRun        int
	MaxStreamDurationMin   float64
	MinStreamDurationMin   float64
	WindowSeconds          int
	FlushIntervalSeconds   int
	ErrorTriggersImmediate bool
}

func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		MaxLogsPerSecond:       50,
		MaxBufferSize:          10,
		MaxTokensPerRun:        6000,
		MaxStreamDurationMin:   2,
		MinStreamDurationMin:   15,
		WindowSeconds:          60,
		FlushIntervalSeconds:   30,
		ErrorTriggersImmediate: true,
	}
}

type StreamStats struct {
	StartTime         time.Time
	TotalLogsIngested int
	BufferFlushCount  int
	DroppedLogs       int
}

//
// ===================== STREAM MANAGER =====================
//

type StreamManager struct {
	Config StreamConfig
	Buffer []RawLog
	Stats  StreamStats

	Preprocessor *LogPreprocessor

	checkWindowStart time.Time
	logsInWindow     int

	lastFlushTime time.Time
	stopChan      chan struct{}

	subscribers map[chan *CorrelationBundle]struct{}

	mu sync.Mutex
}

func NewStreamManager(config StreamConfig) *StreamManager {
	sm := &StreamManager{
		Config:           config,
		Buffer:           []RawLog{},
		Stats:            StreamStats{StartTime: time.Now()},
		Preprocessor:     NewLogPreprocessor(),
		checkWindowStart: time.Now(),
		logsInWindow:     0,
		lastFlushTime:    time.Now(),
		stopChan:         make(chan struct{}),
		subscribers:      make(map[chan *CorrelationBundle]struct{}),
	}

	go sm.backgroundFlushLoop()
	return sm
}

//
// ===================== WINDOW PRUNING =====================
//

func (s *StreamManager) pruneOldLogs() {
	cutoff := time.Now().Add(-time.Duration(s.Config.WindowSeconds) * time.Second)

	pruned := s.Buffer[:0]
	for _, log := range s.Buffer {
		if ts, ok := log.Data["timestamp"].(time.Time); ok {
			if ts.After(cutoff) {
				pruned = append(pruned, log)
			}
		} else {
			// keep log if timestamp missing
			pruned = append(pruned, log)
		}
	}
	s.Buffer = pruned
}

//
// ===================== INGEST =====================
//

// In stream_manager.go: Replace the existing Ingest function
func (s *StreamManager) Ingest(logDict map[string]interface{}) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if now.Sub(s.checkWindowStart) >= time.Second {
		s.checkWindowStart = now
		s.logsInWindow = 0
	}

	if s.logsInWindow >= s.Config.MaxLogsPerSecond {
		s.Stats.DroppedLogs++
		return false, false
	}

	ts, _ := time.Parse(time.RFC3339, logDict["timestamp"].(string))

	rl := RawLog{
		Data:      logDict,
		Timestamp: ts,
		Level:     logDict["level"].(string),
		Service:   logDict["service"].(string),
	}

	s.Buffer = append(s.Buffer, rl)
	s.pruneOldLogs()

	s.logsInWindow++
	s.Stats.TotalLogsIngested++

	isError := false
	if s.Config.ErrorTriggersImmediate && rl.Level == "ERROR" {
		isError = true
	}

	return true, isError
}

func (s *StreamManager) ShouldFlush() bool {
	return len(s.Buffer) >= s.Config.MaxBufferSize
}

func (s *StreamManager) IsExpired() bool {
	elapsedMinutes := time.Since(s.Stats.StartTime).Minutes()
	return elapsedMinutes > s.Config.MaxStreamDurationMin
}

//
// ===================== BACKGROUND FLUSH =====================
//

func (s *StreamManager) backgroundFlushLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			// Check for expiration first
			if s.IsExpired() {
				s.FlushWithReason(FlushExpired)

				// 🔁 reset stream window (do NOT stop manager)
				s.resetStreamWindow()

				continue
			}
			if time.Since(s.lastFlushTime).Seconds() >= float64(s.Config.FlushIntervalSeconds) {
				s.FlushWithReason(FlushTimeElapsed)
			}
		}
	}
}

//
// ===================== FLUSH =====================
//

func (s *StreamManager) FlushWithReason(reason FlushReason) *CorrelationBundle {
	s.mu.Lock()

	if len(s.Buffer) == 0 {
		s.mu.Unlock()
		return nil
	}

	internalPatterns := deriveInternalPatterns(s.Buffer)
	bundle := s.Preprocessor.Factory.CreateBundle(s.Buffer, internalPatterns)

	// ---------------- NEW: build API response bundle ----------------
	respBundle := &StreamBundleResponse{
		ID:               generateIncidentID(s.Buffer),
		WindowStart:      s.Buffer[0].Timestamp.UTC().Format(time.RFC3339),
		WindowEnd:        s.Buffer[len(s.Buffer)-1].Timestamp.UTC().Format(time.RFC3339),
		RootService:      firstNonEmpty(uniqueServices(s.Buffer)),
		AffectedServices: uniqueServices(s.Buffer),
		LogPatterns:      deriveStreamPatterns(s.Buffer),
		FlushMetadata: StreamFlushMetadata{
			Reason:     string(reason),
			LogCount:   len(s.Buffer),
			FlushedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}

	bundle.Metadata["api_bundle"] = respBundle
	bundle.Metadata["flush_reason"] = string(reason)
	bundle.Metadata["flushed_at"] = time.Now().UTC().Format(time.RFC3339)

	estimatedTokens := s.estimateTokens(bundle)
	bundle.Metadata["original_token_est"] = estimatedTokens

	s.Buffer = []RawLog{}
	s.Stats.BufferFlushCount++
	s.lastFlushTime = time.Now()

	s.mu.Unlock() // 🔓 unlock BEFORE publish

	s.publish(bundle)
	return bundle
}

func (s *StreamManager) Flush() *CorrelationBundle {
	return s.FlushWithReason(FlushManual)
}

func (s *StreamManager) Shutdown() {
	close(s.stopChan)
	s.FlushWithReason(FlushShutdown)
}

func (s *StreamManager) estimateTokens(bundle *CorrelationBundle) int {
	jsonStr := bundle.ModelDumpJSON()
	return len(jsonStr) / 4
}

//
// ===================== SSE SUPPORT =====================
//

func (s *StreamManager) Subscribe() chan *CorrelationBundle {
	ch := make(chan *CorrelationBundle, 1)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *StreamManager) Unsubscribe(ch chan *CorrelationBundle) {
	s.mu.Lock()
	delete(s.subscribers, ch)
	close(ch)
	s.mu.Unlock()
}

func (s *StreamManager) publish(bundle *CorrelationBundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- bundle:
		default:
		}
	}
}

//
// ===================== PATTERN DERIVATION =====================
//

func deriveStreamPatterns(logs []RawLog) []StreamLogPattern {
	type agg struct {
		count int
		first time.Time
		last  time.Time
		level string
		src   map[string]string
	}

	m := make(map[string]*agg)

	for _, l := range logs {
		msg, _ := l.Data["message"].(string)
		if msg == "" {
			continue
		}

		src := map[string]string{}
		if srcRaw, ok := l.Data["source"].(map[string]interface{}); ok {
			if c, ok := srcRaw["container"].(string); ok && c != "" {
				src["container"] = c
			}
			if n, ok := srcRaw["namespace"].(string); ok && n != "" {
				src["namespace"] = n
			}
		}

		if _, ok := m[msg]; !ok {
			m[msg] = &agg{
				count: 1,
				first: l.Timestamp,
				last:  l.Timestamp,
				level: l.Level,
				src:   src,
			}
		} else {
			m[msg].count++
			m[msg].last = l.Timestamp
		}
	}

	var out []StreamLogPattern
	for p, a := range m {
		out = append(out, StreamLogPattern{
			Pattern:         normalizePattern(p),
			Count:           a.count,
			FirstOccurrence: a.first.UTC().Format(time.RFC3339),
			LastOccurrence:  a.last.UTC().Format(time.RFC3339),
			ErrorClass:      a.level,
			LogSource:       a.src,
		})
	}

	return out
}

func deriveInternalPatterns(logs []RawLog) []string {
	unique := make(map[string]struct{})
	for _, l := range logs {
		if msg, ok := l.Data["message"].(string); ok && msg != "" {
			unique[msg] = struct{}{}
		}
	}
	var out []string
	for k := range unique {
		out = append(out, k)
	}
	return out
}

func generateIncidentID(logs []RawLog) string {
	if len(logs) == 0 {
		return ""
	}
	ts := logs[len(logs)-1].Timestamp.UTC().Format("20060102_150405")
	return "incident_" + ts
}

func uniqueServices(logs []RawLog) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, l := range logs {
		if l.Service == "" {
			continue
		}
		if _, ok := seen[l.Service]; !ok {
			seen[l.Service] = struct{}{}
			out = append(out, l.Service)
		}
	}
	return out
}

func normalizePattern(s string) string {
	re := regexp.MustCompile(`\d+`)
	return re.ReplaceAllString(s, "<NUM>")
}

func (s *StreamManager) resetStreamWindow() {
    s.Stats.StartTime = time.Now()
    s.checkWindowStart = time.Now()
    s.logsInWindow = 0
    s.lastFlushTime = time.Now()
}
