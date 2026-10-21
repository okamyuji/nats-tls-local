package main

import (
	"bytes"
	"errors"
	"log"
	"maps"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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

// fakeJetStream implements only the calls run makes; any other call panics.
type fakeJetStream struct {
	nats.JetStreamContext
	publishErr    error
	published     []string
	streamInfoErr error
	addStreamErr  error
	added         []nats.StreamConfig
	subscribeErr  error
	mu            sync.Mutex
	handler       nats.MsgHandler
}

func (f *fakeJetStream) StreamInfo(string, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return &nats.StreamInfo{}, f.streamInfoErr
}

func (f *fakeJetStream) AddStream(cfg *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	f.added = append(f.added, *cfg)
	return &nats.StreamInfo{}, f.addStreamErr
}

func (f *fakeJetStream) Subscribe(_ string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handler = cb
	return &nats.Subscription{}, f.subscribeErr
}

func (f *fakeJetStream) handlerFunc() nats.MsgHandler {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handler
}

func (f *fakeJetStream) subscribed() bool { return f.handlerFunc() != nil }

func (f *fakeJetStream) Publish(subj string, _ []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	f.published = append(f.published, subj)
	return &nats.PubAck{}, f.publishErr
}

// Messages not bound to a subscription make Ack and Nak return nats.ErrMsgNotBound.
func logMsg(data string) *nats.Msg {
	return &nats.Msg{Subject: "logs.api", Data: []byte(data)}
}

func TestHandleLog_naksInvalidJSONAndLogsNakFailure(t *testing.T) {
	logs := captureLog(t)
	collector := NewMetricsCollector()

	handleLog(&fakeJetStream{}, collector)(logMsg(`not json`))

	if n := collector.Snapshot().TotalLogs; n != 0 {
		t.Fatalf("TotalLogs = %d, want 0", n)
	}
	if !strings.Contains(logs.String(), "Failed to nak log entry") {
		t.Fatalf("log = %q, want nak failure", logs.String())
	}
}

func TestHandleLog_recordsInfoWithoutAlert(t *testing.T) {
	logs := captureLog(t)
	js := &fakeJetStream{}
	collector := NewMetricsCollector()

	handleLog(js, collector)(logMsg(`{"service":"api","level":"INFO"}`))

	if n := collector.Snapshot().TotalLogs; n != 1 {
		t.Fatalf("TotalLogs = %d, want 1", n)
	}
	if len(js.published) != 0 {
		t.Fatalf("published = %v, want none", js.published)
	}
	if !strings.Contains(logs.String(), "Failed to ack log entry") {
		t.Fatalf("log = %q, want ack failure", logs.String())
	}
}

func TestHandleLog_publishesAlertForErrorAndCritical(t *testing.T) {
	captureLog(t)
	js := &fakeJetStream{}

	handleLog(js, NewMetricsCollector())(logMsg(`{"service":"api","level":"ERROR"}`))
	handleLog(js, NewMetricsCollector())(logMsg(`{"service":"api","level":"CRITICAL"}`))

	if got := strings.Join(js.published, ","); got != "alerts.error,alerts.critical" {
		t.Fatalf("published = %s, want alerts.error,alerts.critical", got)
	}
}

func TestHandleLog_logsPublishFailureAndStillAcks(t *testing.T) {
	logs := captureLog(t)
	js := &fakeJetStream{publishErr: errors.New("timeout")}

	handleLog(js, NewMetricsCollector())(logMsg(`{"service":"api","level":"ERROR"}`))

	for _, want := range []string{"Failed to publish alert: timeout", "Failed to ack log entry"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log = %q, want %q", logs.String(), want)
		}
	}
}

func TestMetricsCollector_snapshotAggregatesThenResets(t *testing.T) {
	collector := NewMetricsCollector()
	for _, e := range []LogEntry{
		{Service: "api", Level: "INFO"},
		{Service: "api", Level: "INFO"},
		{Service: "db", Level: "ERROR"},
		{Service: "db", Level: "CRITICAL"},
	} {
		collector.Record(e)
	}

	got := collector.Snapshot()

	if got.TotalLogs != 4 || got.ErrorRate != 0.5 {
		t.Fatalf("TotalLogs = %d, ErrorRate = %v; want 4, 0.5", got.TotalLogs, got.ErrorRate)
	}
	if !maps.Equal(got.ByLevel, map[string]int{"INFO": 2, "ERROR": 1, "CRITICAL": 1}) {
		t.Fatalf("ByLevel = %v", got.ByLevel)
	}
	if !maps.Equal(got.ByService, map[string]int{"api": 2, "db": 2}) {
		t.Fatalf("ByService = %v", got.ByService)
	}

	empty := collector.Snapshot()
	if empty.TotalLogs != 0 || empty.ErrorRate != 0 || len(empty.ByLevel) != 0 || len(empty.ByService) != 0 {
		t.Fatalf("second snapshot = %+v, want zero counts", empty)
	}
}

func TestPublishAlert_escapesNewlinesInLogLine(t *testing.T) {
	logs := captureLog(t)

	err := publishAlert(&fakeJetStream{}, LogEntry{Service: "api", Level: "ERROR", Message: "boom\n2026/01/01 00:00:00 [ALERT] critical: forged"})

	if err != nil {
		t.Fatalf("publishAlert: %v", err)
	}
	if n := strings.Count(logs.String(), "\n"); n != 1 {
		t.Fatalf("log has %d lines, want 1: %q", n, logs.String())
	}
}

func TestRun_returnsStreamCreationError(t *testing.T) {
	captureLog(t)
	js := &fakeJetStream{streamInfoErr: nats.ErrStreamNotFound, addStreamErr: errors.New("insufficient resources")}

	err := run(js, nil, nil)

	if err == nil || err.Error() != "failed to initialize streams: failed to create stream LOGS: insufficient resources" {
		t.Fatalf("err = %v", err)
	}
}

func TestRun_returnsSubscribeError(t *testing.T) {
	captureLog(t)
	js := &fakeJetStream{subscribeErr: errors.New("no responders")}

	err := run(js, nil, nil)

	if err == nil || err.Error() != "failed to subscribe: no responders" {
		t.Fatalf("err = %v", err)
	}
	if len(js.added) != 0 {
		t.Fatalf("added = %v, want none when streams exist", js.added)
	}
}

// startRun runs run in a goroutine; tick and stop are unbuffered, so each send returns only after run took it.
func startRun(t *testing.T, js *fakeJetStream) (tick chan time.Time, stop chan os.Signal, done chan error) {
	t.Helper()
	tick, stop, done = make(chan time.Time), make(chan os.Signal), make(chan error, 1)
	go func() { done <- run(js, tick, stop) }()
	return tick, stop, done
}

func waitHandler(t *testing.T, js *fakeJetStream, done chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("run returned early: %v", err)
		default:
		}
		if js.subscribed() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not subscribe within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRun_createsMissingStreamsPublishesMetricsAndStops(t *testing.T) {
	logs := captureLog(t)
	js := &fakeJetStream{streamInfoErr: nats.ErrStreamNotFound}
	tick, stop, done := startRun(t, js)
	waitHandler(t, js, done)

	tick <- time.Now() // no logs yet: nothing to publish
	js.handlerFunc()(logMsg(`{"service":"api","level":"INFO"}`))
	js.handlerFunc()(logMsg(`{"service":"api","level":"ERROR"}`))
	tick <- time.Now()
	stop <- os.Interrupt

	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []nats.StreamConfig{
		{Name: "LOGS", Subjects: []string{"logs.>"}, MaxAge: 24 * time.Hour, Storage: nats.FileStorage},
		{Name: "ALERTS", Subjects: []string{"alerts.>"}, MaxAge: 168 * time.Hour, Storage: nats.FileStorage},
		{Name: "METRICS", Subjects: []string{"metrics.>"}, MaxAge: time.Hour, Storage: nats.MemoryStorage},
		{Name: "DASHBOARD", Subjects: []string{"dashboard.>"}, MaxAge: 5 * time.Minute, Storage: nats.MemoryStorage},
	}
	if !reflect.DeepEqual(js.added, want) {
		t.Fatalf("added = %+v, want %+v", js.added, want)
	}
	if got := strings.Join(js.published, ","); got != "alerts.error,metrics.processed" {
		t.Fatalf("published = %s, want alerts.error,metrics.processed", got)
	}
	if !strings.Contains(logs.String(), "Published metrics: 2 logs, error_rate=50.00%") {
		t.Fatalf("log = %q, want metrics summary", logs.String())
	}
}

func TestRun_logsMetricsPublishFailure(t *testing.T) {
	logs := captureLog(t)
	js := &fakeJetStream{publishErr: errors.New("timeout")}
	tick, stop, done := startRun(t, js)
	waitHandler(t, js, done)

	js.handlerFunc()(logMsg(`{"service":"api","level":"INFO"}`))
	tick <- time.Now()
	stop <- os.Interrupt
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(logs.String(), "Failed to publish metrics: timeout") {
		t.Fatalf("log = %q, want publish failure", logs.String())
	}
}
