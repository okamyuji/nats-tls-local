#!/bin/bash
# JetStream初期化スクリプト

set -e

NATS_URL="${NATS_URL:-nats://localhost:4222}"
NATS_CREDS=""

if [ -n "$NATS_CA_FILE" ]; then
    NATS_CREDS="--tlsca=$NATS_CA_FILE"
fi
if [ -n "$NATS_CLIENT_CERT" ]; then
    NATS_CREDS="$NATS_CREDS --tlscert=$NATS_CLIENT_CERT"
fi
if [ -n "$NATS_CLIENT_KEY" ]; then
    NATS_CREDS="$NATS_CREDS --tlskey=$NATS_CLIENT_KEY"
fi

echo "Waiting for NATS server..."
until nats server check connection $NATS_CREDS -s $NATS_URL 2>/dev/null; do
    sleep 1
done
echo "NATS server is ready"

# LOGSストリーム作成
echo "Creating LOGS stream..."
nats stream add LOGS \
    --subjects "logs.>" \
    --retention limits \
    --max-age 24h \
    --max-bytes 1GB \
    --storage file \
    --replicas 1 \
    --discard old \
    --dupe-window 2m \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || \
nats stream update LOGS \
    --subjects "logs.>" \
    --max-age 24h \
    --max-bytes 1GB \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || true

# ALERTSストリーム作成
echo "Creating ALERTS stream..."
nats stream add ALERTS \
    --subjects "alerts.>" \
    --retention limits \
    --max-age 168h \
    --max-bytes 512MB \
    --storage file \
    --replicas 1 \
    --discard old \
    --dupe-window 2m \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || \
nats stream update ALERTS \
    --subjects "alerts.>" \
    --max-age 168h \
    --max-bytes 512MB \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || true

# METRICSストリーム作成
echo "Creating METRICS stream..."
nats stream add METRICS \
    --subjects "metrics.>" \
    --retention limits \
    --max-age 1h \
    --max-bytes 256MB \
    --storage memory \
    --replicas 1 \
    --discard old \
    --dupe-window 30s \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || \
nats stream update METRICS \
    --subjects "metrics.>" \
    --max-age 1h \
    --max-bytes 256MB \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || true

# DASHBOARDストリーム作成
echo "Creating DASHBOARD stream..."
nats stream add DASHBOARD \
    --subjects "dashboard.>" \
    --retention limits \
    --max-age 5m \
    --max-bytes 64MB \
    --storage memory \
    --replicas 1 \
    --discard old \
    --dupe-window 10s \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || \
nats stream update DASHBOARD \
    --subjects "dashboard.>" \
    --max-age 5m \
    --max-bytes 64MB \
    $NATS_CREDS -s $NATS_URL 2>/dev/null || true

echo "JetStream streams initialized successfully"
nats stream ls $NATS_CREDS -s $NATS_URL
