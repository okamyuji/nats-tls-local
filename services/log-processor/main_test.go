package main

import (
	"bytes"
	"errors"
	"log"
	"maps"
	"strings"
	"testing"

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

// fakeJetStream implements only Publish; any other call panics.
type fakeJetStream struct {
	nats.JetStreamContext
	publishErr error
	published  []string
}

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
