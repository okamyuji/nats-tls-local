#!/bin/bash
# scripts/generate-certs.sh

set -e

CERT_DIR="/etc/nats/certs"
mkdir -p $CERT_DIR
cd $CERT_DIR

echo "🔐 自己署名証明書を生成中..."

# CA（認証局）の秘密鍵と証明書を生成
if [ ! -f ca-key.pem ]; then
    echo "📝 CA証明書を生成..."
    openssl genrsa -out ca-key.pem 4096
    
    openssl req -new -x509 -days 3650 -key ca-key.pem -out ca.pem \
        -subj "/C=JP/ST=Tokyo/L=Tokyo/O=LocalDev/CN=NATS-Local-CA"
    
    echo "✅ CA証明書生成完了"
fi

# NATSサーバー用の証明書を生成
if [ ! -f nats-server-cert.pem ]; then
    echo "📝 NATSサーバー証明書を生成..."
    
    # サーバー秘密鍵生成
    openssl genrsa -out nats-server-key.pem 4096
    
    # CSR作成
    openssl req -new -key nats-server-key.pem -out nats-server.csr \
        -subj "/C=JP/ST=Tokyo/L=Tokyo/O=LocalDev/CN=nats-server"
    
    # SAN設定ファイル
    cat > san.cnf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = nats-server
DNS.2 = localhost
DNS.3 = nats
IP.1 = 127.0.0.1
EOF
    
    # サーバー証明書に署名
    openssl x509 -req -days 365 -in nats-server.csr \
        -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
        -out nats-server-cert.pem -extensions v3_req -extfile san.cnf
    
    echo "✅ サーバー証明書生成完了"
fi

# クライアント用証明書を生成
if [ ! -f nats-client-cert.pem ]; then
    echo "📝 クライアント証明書を生成..."
    
    openssl genrsa -out nats-client-key.pem 4096
    
    openssl req -new -key nats-client-key.pem -out nats-client.csr \
        -subj "/C=JP/ST=Tokyo/L=Tokyo/O=LocalDev/CN=nats-client"
    
    openssl x509 -req -days 365 -in nats-client.csr \
        -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
        -out nats-client-cert.pem
    
    echo "✅ クライアント証明書生成完了"
fi

# 権限設定
chmod 644 *.pem
chmod 600 *-key.pem

# 証明書情報を表示
echo ""
echo "📋 生成された証明書:"
ls -lh $CERT_DIR/*.pem

echo ""
echo "🔍 サーバー証明書の詳細:"
openssl x509 -in nats-server-cert.pem -text -noout | grep -A1 "Subject Alternative Name"

echo ""
echo "✅ すべての証明書の生成が完了しました"