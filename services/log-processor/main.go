package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Service   string                 `json:"service"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Alert represents an alert notification
type Alert struct {
	Timestamp string                 `json:"timestamp"`
	Severity  string                 `json:"severity"`
	Source    string                 `json:"source"`
	Title     string                 `json:"title"`
	Message   string                 `json:"message"`
	Context   map[string]interface{} `json:"context,omitempty"`
}

// ProcessedMetrics represents aggregated metrics
type ProcessedMetrics struct {
	Timestamp     string         `json:"timestamp"`
	WindowSeconds int            `json:"window_seconds"`
	TotalLogs     int            `json:"total_logs"`
	ByLevel       map[string]int `json:"by_level"`
	ByService     map[string]int `json:"by_service"`
	ErrorRate     float64        `json:"error_rate"`
}

// MetricsCollector collects and aggregates log metrics
type MetricsCollector struct {
	mu         sync.Mutex
	totalLogs  int
	byLevel    map[string]int
	byService  map[string]int
	errorCount int
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		byLevel:   make(map[string]int),
		byService: make(map[string]int),
	}
}

func (m *MetricsCollector) Record(entry LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalLogs++
	m.byLevel[entry.Level]++
	m.byService[entry.Service]++

	if entry.Level == "ERROR" || entry.Level == "CRITICAL" {
		m.errorCount++
	}
}

func (m *MetricsCollector) Snapshot() ProcessedMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	errorRate := 0.0
	if m.totalLogs > 0 {
		errorRate = float64(m.errorCount) / float64(m.totalLogs)
	}

	metrics := ProcessedMetrics{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		WindowSeconds: 5,
		TotalLogs:     m.totalLogs,
		ByLevel:       make(map[string]int),
		ByService:     make(map[string]int),
		ErrorRate:     errorRate,
	}

	for k, v := range m.byLevel {
		metrics.ByLevel[k] = v
	}
	for k, v := range m.byService {
		metrics.ByService[k] = v
	}

	// Reset counters
	m.totalLogs = 0
	m.byLevel = make(map[string]int)
	m.byService = make(map[string]int)
	m.errorCount = 0

	return metrics
}

func main() {
	nc, js, err := connectNATS()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS, starting log processor...")

	// Initialize JetStream streams
	if err := initStreams(js); err != nil {
		log.Fatalf("Failed to initialize streams: %v", err)
	}

	collector := NewMetricsCollector()

	// Subscribe to all logs
	sub, err := js.Subscribe("logs.>", func(msg *nats.Msg) {
		var entry LogEntry
		if err := json.Unmarshal(msg.Data, &entry); err != nil {
			log.Printf("Failed to unmarshal log entry: %v", err)
			msg.Nak()
			return
		}

		// Record metrics
		collector.Record(entry)

		// Process based on log level
		if entry.Level == "ERROR" || entry.Level == "CRITICAL" {
			if err := publishAlert(js, entry); err != nil {
				log.Printf("Failed to publish alert: %v", err)
			}
		}

		msg.Ack()
	}, nats.Durable("log-processor"), nats.ManualAck())
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish metrics every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigCh:
			log.Println("Shutting down log processor...")
			return
		case <-ticker.C:
			metrics := collector.Snapshot()
			if metrics.TotalLogs > 0 {
				if err := publishMetrics(js, metrics); err != nil {
					log.Printf("Failed to publish metrics: %v", err)
				} else {
					log.Printf("Published metrics: %d logs, error_rate=%.2f%%",
						metrics.TotalLogs, metrics.ErrorRate*100)
				}
			}
		}
	}
}

func initStreams(js nats.JetStreamContext) error {
	streams := []struct {
		name     string
		subjects []string
		maxAge   time.Duration
		storage  nats.StorageType
	}{
		{"LOGS", []string{"logs.>"}, 24 * time.Hour, nats.FileStorage},
		{"ALERTS", []string{"alerts.>"}, 7 * 24 * time.Hour, nats.FileStorage},
		{"METRICS", []string{"metrics.>"}, time.Hour, nats.MemoryStorage},
		{"DASHBOARD", []string{"dashboard.>"}, 5 * time.Minute, nats.MemoryStorage},
	}

	for _, s := range streams {
		_, err := js.StreamInfo(s.name)
		if err != nil {
			log.Printf("Creating stream %s...", s.name)
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     s.name,
				Subjects: s.subjects,
				MaxAge:   s.maxAge,
				Storage:  s.storage,
			})
			if err != nil {
				return fmt.Errorf("failed to create stream %s: %w", s.name, err)
			}
			log.Printf("Stream %s created", s.name)
		} else {
			log.Printf("Stream %s already exists", s.name)
		}
	}
	return nil
}

func connectNATS() (*nats.Conn, nats.JetStreamContext, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	opts := []nats.Option{
		nats.Name("log-processor"),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
	}

	// TLS configuration
	caFile := os.Getenv("NATS_CA_FILE")
	certFile := os.Getenv("NATS_CLIENT_CERT")
	keyFile := os.Getenv("NATS_CLIENT_KEY")

	if caFile != "" && certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load client cert: %w", err)
		}

		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
			MinVersion:   tls.VersionTLS12,
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	// User credentials
	user := os.Getenv("NATS_USER")
	pass := os.Getenv("NATS_PASSWORD")
	if user != "" && pass != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}

	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, err
	}

	return nc, js, nil
}

func publishAlert(js nats.JetStreamContext, entry LogEntry) error {
	severity := "error"
	if entry.Level == "CRITICAL" {
		severity = "critical"
	}

	alert := Alert{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Severity:  severity,
		Source:    "log-processor",
		Title:     fmt.Sprintf("%s in %s", entry.Level, entry.Service),
		Message:   entry.Message,
		Context: map[string]interface{}{
			"service":       entry.Service,
			"original_time": entry.Timestamp,
			"original_meta": entry.Metadata,
		},
	}

	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("alerts.%s", severity)
	_, err = js.Publish(subject, data)

	log.Printf("[ALERT] %s: %s - %s", severity, entry.Service, entry.Message)
	return err
}

func publishMetrics(js nats.JetStreamContext, metrics ProcessedMetrics) error {
	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	_, err = js.Publish("metrics.processed", data)
	return err
}
