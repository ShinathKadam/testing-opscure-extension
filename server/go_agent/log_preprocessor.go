package main

import (
	"errors"
	"sort"
	"strings"
	"time"
)

type RawLogGo struct {
	Timestamp  string
	Level      string
	Service    string
	Pod        *string
	Message    string
	ErrorClass *string
}

type LogPatternGo struct {
	Pattern         string
	Count           int
	FirstOccurrence string
	LastOccurrence  string
	ErrorClass      *string
}

type MetricsGo struct {
	CPUZ       float64
	ErrorRateZ float64
	LatencyZ   float64
}

type CorrelationBundleGo struct {
	ID                   string
	WindowStart          string
	WindowEnd            string
	RootService          *string
	AffectedServices     []string
	LogPatterns          []LogPatternGo
	Events               []string
	Metrics              MetricsGo
	DependencyGraph      []string
	DerivedRootCauseHint string
}

type LogParserGo struct{}

func (p *LogParserGo) ParseLogs(rawData []map[string]interface{}) ([]RawLogGo, error) {
	if rawData == nil {
		return nil, errors.New("rawData is nil")
	}

	var parsed []RawLogGo

	for _, entry := range rawData {
		ts := time.Now().UTC().Format(time.RFC3339)
		if v, ok := entry["timestamp"].(string); ok && v != "" {
			ts = v
		}

		level := "INFO"
		if v, ok := entry["level"].(string); ok && v != "" {
			level = strings.ToUpper(v)
		}

		service := "unknown"
		if v, ok := entry["service"].(string); ok && v != "" {
			service = v
		}

		msg := ""
		if v, ok := entry["message"].(string); ok {
			msg = v
		}

		var ec *string
		lmsg := strings.ToLower(msg)

		if level == "ERROR" {
			s := "Error"
			ec = &s
		} else if level == "WARN" {
			s := "Warning"
			ec = &s
		}

		if strings.Contains(lmsg, "exception") ||
			strings.Contains(lmsg, "error") ||
			strings.Contains(lmsg, "failed") ||
			strings.Contains(lmsg, "panic") {
			s := "Exception"
			ec = &s
		}

		parsed = append(parsed, RawLogGo{
			Timestamp:  ts,
			Level:      level,
			Service:    service,
			Message:    msg,
			ErrorClass: ec,
		})
	}

	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].Timestamp < parsed[j].Timestamp
	})

	return parsed, nil
}

type LogPatternMinerGo struct{}

func (m *LogPatternMinerGo) MinePatterns(logs []RawLogGo) []LogPatternGo {
	patterns := make(map[string]LogPatternGo)

	for _, log := range logs {
		p := patterns[log.Message]
		if p.Pattern == "" {
			p.Pattern = log.Message
			p.FirstOccurrence = log.Timestamp
		}
		p.Count++
		p.LastOccurrence = log.Timestamp
		p.ErrorClass = log.ErrorClass
		patterns[log.Message] = p
	}

	var out []LogPatternGo
	for _, p := range patterns {
		out = append(out, p)
	}
	return out
}

type BundleFactoryGo struct{}

func (f *BundleFactoryGo) CreateBundle(logs []RawLogGo, patterns []LogPatternGo) (*CorrelationBundleGo, error) {
	if len(logs) == 0 {
		return nil, errors.New("empty logs")
	}

	start := logs[0].Timestamp
	end := logs[len(logs)-1].Timestamp

	serviceSet := map[string]struct{}{}
	errorCount := 0
	errorByService := map[string]int{}

	for _, l := range logs {
		serviceSet[l.Service] = struct{}{}
		if l.ErrorClass != nil {
			errorCount++
			errorByService[l.Service]++
		}
	}

	var services []string
	for s := range serviceSet {
		services = append(services, s)
	}

	// ✅ GENERIC ROOT SERVICE DERIVATION
	var rootService *string
	maxErr := 0
	for svc, cnt := range errorByService {
		if cnt > maxErr {
			maxErr = cnt
			tmp := svc
			rootService = &tmp
		}
	}

	var events []string
	if errorCount > 0 {
		events = append(events, "Errors observed in log window")
	}
	if len(patterns) > 10 {
		events = append(events, "High log pattern diversity detected")
	}

	cpuZ := float64(len(logs)) / 50.0

	return &CorrelationBundleGo{
		RootService:      rootService,
		WindowStart:      start,
		WindowEnd:        end,
		AffectedServices: services,
		LogPatterns:      patterns,
		Events:           events,
		Metrics: MetricsGo{
			CPUZ:       cpuZ,
			ErrorRateZ: float64(errorCount),
			LatencyZ:   float64(len(logs)) / 10,
		},
		DependencyGraph:      services,
		DerivedRootCauseHint: "Derived from runtime log patterns",
	}, nil
}

type LogPreprocessorFullGo struct {
	Parser  *LogParserGo
	Miner   *LogPatternMinerGo
	Factory *BundleFactoryGo
}

func NewLogPreprocessorFullGo() *LogPreprocessorFullGo {
	return &LogPreprocessorFullGo{
		Parser:  &LogParserGo{},
		Miner:   &LogPatternMinerGo{},
		Factory: &BundleFactoryGo{},
	}
}

func (p *LogPreprocessorFullGo) Process(rawData []map[string]interface{}, bundleID string) (*CorrelationBundleGo, error) {
	logs, err := p.Parser.ParseLogs(rawData)
	if err != nil {
		return nil, err
	}

	patterns := p.Miner.MinePatterns(logs)
	b, err := p.Factory.CreateBundle(logs, patterns)
	if err != nil {
		return nil, err
	}

	b.ID = bundleID
	return b, nil
}
