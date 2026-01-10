package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
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

var (
	levels   = []string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "CRITICAL"}
	services = []string{"user-api", "order-api", "payment-api", "notification-api", "inventory-api"}
	messages = map[string][]string{
		"DEBUG": {
			"Starting request processing",
			"Cache lookup completed",
			"Database query executed",
			"Response serialization started",
		},
		"INFO": {
			"User logged in successfully",
			"Order created",
			"Payment processed",
			"Email sent to user",
			"Inventory updated",
			"Request completed in 45ms",
		},
		"WARN": {
			"High latency detected: 500ms",
			"Rate limit threshold approaching",
			"Cache miss ratio above 30%",
			"Retry attempt 2 of 3",
		},
		"ERROR": {
			"Database connection failed",
			"External API timeout",
			"Invalid request payload",
			"Authentication failed",
			"Resource not found",
		},
		"CRITICAL": {
			"Service unavailable",
			"Data corruption detected",
			"Security breach attempt",
			"Out of memory",
		},
	}
)

func main() {
	nc, js, err := connectNATS()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS, starting log publisher...")

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			log.Println("Shutting down log publisher...")
			return
		case <-ticker.C:
			if err := publishLog(js); err != nil {
				log.Printf("Failed to publish log: %v", err)
			}
		}
	}
}

func connectNATS() (*nats.Conn, nats.JetStreamContext, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	opts := []nats.Option{
		nats.Name("log-publisher"),
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

func publishLog(js nats.JetStreamContext) error {
	entry := generateLogEntry()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("logs.app.%s", entry.Service)
	_, err = js.Publish(subject, data)
	if err != nil {
		return err
	}

	log.Printf("[%s] %s: %s", entry.Level, entry.Service, entry.Message)
	return nil
}

func generateLogEntry() LogEntry {
	level := levels[rand.Intn(len(levels))]
	service := services[rand.Intn(len(services))]
	msgs := messages[level]
	message := msgs[rand.Intn(len(msgs))]

	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   service,
		Level:     level,
		Message:   message,
	}

	// Add random metadata
	if rand.Float32() > 0.5 {
		entry.Metadata = map[string]interface{}{
			"request_id":  fmt.Sprintf("req-%d", rand.Intn(100000)),
			"user_id":     fmt.Sprintf("user-%d", rand.Intn(10000)),
			"duration_ms": rand.Intn(500),
		}
	}

	return entry
}
