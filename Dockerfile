# Dockerfile

FROM nats:2.10.7-alpine

# 必要なツールをインストール
RUN apk add --no-cache \
    bash \
    openssl \
    curl \
    jq

# 証明書生成スクリプトをコピー
COPY scripts/generate-certs.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/generate-certs.sh

# NATS設定ファイルをコピー
COPY config/nats-server.conf /etc/nats/nats-server.conf

# エントリーポイントスクリプト
COPY scripts/entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# 証明書用のボリューム
VOLUME ["/etc/nats/certs", "/data/jetstream"]

# ポート公開
EXPOSE 4222 8222 6222

# エントリーポイント設定
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]