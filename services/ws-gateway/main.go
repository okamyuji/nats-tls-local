package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// Hub maintains the set of active clients
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// Client represents a WebSocket connection
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client connected. Total: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("Client disconnected. Total: %d", len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 256),
	}

	hub.register <- client

	go client.writePump()
	go client.readPump()
}

func connectNATS() (*nats.Conn, nats.JetStreamContext, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	opts := []nats.Option{
		nats.Name("ws-gateway"),
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

func waitForStream(js nats.JetStreamContext, streamName string) error {
	for {
		_, err := js.StreamInfo(streamName)
		if err == nil {
			log.Printf("Stream %s found", streamName)
			return nil
		}
		if err.Error() == "nats: stream not found" || err.Error() == "stream not found" {
			log.Printf("Waiting for stream %s...", streamName)
			time.Sleep(2 * time.Second)
			continue
		}
		return err
	}
}

func subscribeToNATS(js nats.JetStreamContext, hub *Hub) error {
	// Wait for streams
	if err := waitForStream(js, "DASHBOARD"); err != nil {
		return err
	}
	if err := waitForStream(js, "ALERTS"); err != nil {
		return err
	}

	// Subscribe to dashboard updates
	_, err := js.Subscribe("dashboard.>", func(msg *nats.Msg) {
		// Wrap in WebSocket message format
		wsMsg := map[string]interface{}{
			"subject": msg.Subject,
			"data":    json.RawMessage(msg.Data),
		}

		data, err := json.Marshal(wsMsg)
		if err != nil {
			log.Printf("Failed to marshal message: %v", err)
			return
		}

		hub.broadcast <- data
		msg.Ack()
	}, nats.Durable("ws-gateway-dashboard"), nats.ManualAck())
	if err != nil {
		return fmt.Errorf("failed to subscribe to dashboard: %w", err)
	}

	// Subscribe to alerts for real-time display
	_, err = js.Subscribe("alerts.>", func(msg *nats.Msg) {
		wsMsg := map[string]interface{}{
			"subject": msg.Subject,
			"data":    json.RawMessage(msg.Data),
		}

		data, err := json.Marshal(wsMsg)
		if err != nil {
			log.Printf("Failed to marshal alert: %v", err)
			return
		}

		hub.broadcast <- data
		msg.Ack()
	}, nats.Durable("ws-gateway-alerts"), nats.ManualAck())
	if err != nil {
		return fmt.Errorf("failed to subscribe to alerts: %w", err)
	}

	log.Println("Subscribed to NATS subjects: dashboard.>, alerts.>")
	return nil
}

func main() {
	hub := newHub()
	go hub.run()

	// Connect to NATS
	nc, js, err := connectNATS()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	// Subscribe to NATS topics
	if err := subscribeToNATS(js, hub); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// HTTP handlers
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("WebSocket Gateway listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
