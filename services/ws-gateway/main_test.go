package main

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return &buf
}

func TestForwardToHub_broadcastsWrappedMessage(t *testing.T) {
	captureLog(t)
	hub := newHub()

	forwardToHub(hub, "message")(&nats.Msg{Subject: "dashboard.cpu", Data: []byte(`{"v":1}`)})

	got := <-hub.broadcast
	want := `{"data":{"v":1},"subject":"dashboard.cpu"}`
	if string(got) != want {
		t.Fatalf("broadcast = %s, want %s", got, want)
	}
}

func TestForwardToHub_logsAckFailure(t *testing.T) {
	logs := captureLog(t)
	hub := newHub()

	// A message that is not bound to a subscription makes Ack return nats.ErrMsgNotBound.
	forwardToHub(hub, "alert")(&nats.Msg{Subject: "alerts.high", Data: []byte(`{}`)})

	<-hub.broadcast
	if !strings.Contains(logs.String(), "Failed to ack alert") {
		t.Fatalf("log = %q, want ack failure", logs.String())
	}
}

func TestForwardToHub_dropsInvalidJSON(t *testing.T) {
	logs := captureLog(t)
	hub := newHub()

	forwardToHub(hub, "alert")(&nats.Msg{Subject: "alerts.high", Data: []byte(`not json`)})

	if len(hub.broadcast) != 0 {
		t.Fatalf("broadcast has %d messages, want 0", len(hub.broadcast))
	}
	if !strings.Contains(logs.String(), "Failed to marshal alert") {
		t.Fatalf("log = %q, want marshal failure", logs.String())
	}
}

func TestHealthHandler_returnsOK(t *testing.T) {
	rec := httptest.NewRecorder()

	healthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Fatalf("got %d %q, want 200 \"OK\"", rec.Code, rec.Body.String())
	}
}

type failingWriter struct{ httptest.ResponseRecorder }

func (*failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestHealthHandler_logsWriteFailure(t *testing.T) {
	logs := captureLog(t)

	healthHandler(&failingWriter{}, httptest.NewRequest(http.MethodGet, "/health", nil))

	if !strings.Contains(logs.String(), "Failed to write health response: broken pipe") {
		t.Fatalf("log = %q, want write failure", logs.String())
	}
}

func clientCount(h *Hub) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dialGateway(t *testing.T, hub *Hub) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveWs(hub, w, r) }))
	t.Cleanup(srv.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	waitFor(t, func() bool { return clientCount(hub) == 1 })
	return conn
}

func TestServeWs_deliversBroadcastAndUnregistersOnClose(t *testing.T) {
	captureLog(t)
	hub := newHub()
	go hub.run()
	conn := dialGateway(t, hub)

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	for _, want := range []string{"hello", "world"} {
		hub.broadcast <- []byte(want)
		_, got, err := conn.ReadMessage()
		if err != nil || string(got) != want {
			t.Fatalf("read = %q, %v; want %q", got, err, want)
		}
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	waitFor(t, func() bool { return clientCount(hub) == 0 })
}

func TestWritePump_sendsPingEveryPeriod(t *testing.T) {
	captureLog(t)
	prev := pingPeriod
	pingPeriod = 20 * time.Millisecond
	t.Cleanup(func() { pingPeriod = prev })
	hub := newHub()
	go hub.run()
	conn := dialGateway(t, hub)
	t.Cleanup(func() { _ = conn.Close() })

	pinged := make(chan struct{}, 1)
	conn.SetPingHandler(func(data string) error {
		select {
		case pinged <- struct{}{}:
		default:
		}
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	// ReadMessage runs the ping handler; it returns once the deadline passes.
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, _ = conn.ReadMessage()

	select {
	case <-pinged:
	default:
		t.Fatal("no ping received within 200ms")
	}
	// Each pong extends the gateway's read deadline, so the client stays registered.
	if n := clientCount(hub); n != 1 {
		t.Fatalf("clients = %d after pings, want 1", n)
	}
}

// fakeJetStream implements only the calls subscribeToNATS makes; any other call panics.
type fakeJetStream struct {
	nats.JetStreamContext
	streamErr    error
	subscribeErr error
	handlers     map[string]nats.MsgHandler
}

func (f *fakeJetStream) StreamInfo(string, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return &nats.StreamInfo{}, f.streamErr
}

func (f *fakeJetStream) Subscribe(subj string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	f.handlers[subj] = cb
	return &nats.Subscription{}, nil
}

func TestSubscribeToNATS_forwardsBothSubjectsToHub(t *testing.T) {
	captureLog(t)
	hub := newHub()
	js := &fakeJetStream{handlers: map[string]nats.MsgHandler{}}

	if err := subscribeToNATS(js, hub); err != nil {
		t.Fatalf("subscribeToNATS: %v", err)
	}

	for filter, subj := range map[string]string{"dashboard.>": "dashboard.cpu", "alerts.>": "alerts.high"} {
		cb, ok := js.handlers[filter]
		if !ok {
			t.Fatalf("no subscription for %s", filter)
		}
		cb(&nats.Msg{Subject: subj, Data: []byte(`1`)})
		if got := string(<-hub.broadcast); !strings.Contains(got, `"subject":"`+subj+`"`) {
			t.Fatalf("broadcast = %s, want subject %s", got, subj)
		}
	}
}

func TestSubscribeToNATS_returnsStreamLookupError(t *testing.T) {
	captureLog(t)
	js := &fakeJetStream{streamErr: errors.New("permission denied"), handlers: map[string]nats.MsgHandler{}}

	err := subscribeToNATS(js, newHub())

	if err == nil || err.Error() != "permission denied" {
		t.Fatalf("err = %v, want permission denied", err)
	}
}

func TestSubscribeToNATS_wrapsSubscribeError(t *testing.T) {
	captureLog(t)
	js := &fakeJetStream{subscribeErr: errors.New("no responders"), handlers: map[string]nats.MsgHandler{}}

	err := subscribeToNATS(js, newHub())

	if err == nil || err.Error() != "failed to subscribe to dashboard: no responders" {
		t.Fatalf("err = %v, want wrapped dashboard error", err)
	}
}
