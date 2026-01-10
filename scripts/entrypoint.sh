#!/bin/bash
# scripts/entrypoint.sh

set -e

echo "🚀 NATS TLS環境を起動中..."

# 証明書が存在しない場合は生成
if [ ! -f /etc/nats/certs/ca.pem ]; then
    echo "📝 証明書が存在しないため、新規生成します"
    /usr/local/bin/generate-certs.sh
else
    echo "✅ 既存の証明書を使用します"
fi

# 証明書の検証
echo "🔍 証明書を検証中..."
openssl verify -CAfile /etc/nats/certs/ca.pem /etc/nats/certs/nats-server-cert.pem

if [ $? -eq 0 ]; then
    echo "✅ 証明書の検証成功"
else
    echo "❌ 証明書の検証失敗"
    exit 1
fi

# JetStreamディレクトリ作成
mkdir -p /data/jetstream

echo ""
echo "🎯 NATS設定:"
echo "  - TLS: 有効"
echo "  - ポート: 4222 (TLS)"
echo "  - モニタリング: 8222 (HTTP)"
echo "  - JetStream: 有効"
echo ""

# NATSサーバー起動
exec nats-server -c /etc/nats/nats-server.conf