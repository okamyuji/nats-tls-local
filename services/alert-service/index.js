const { connect, AckPolicy, DeliverPolicy } = require('nats');
const fs = require('fs');
const path = require('path');

// Configuration
const config = {
  natsUrl: process.env.NATS_URL || 'nats://localhost:4222',
  caFile: process.env.NATS_CA_FILE,
  certFile: process.env.NATS_CLIENT_CERT,
  keyFile: process.env.NATS_CLIENT_KEY,
  user: process.env.NATS_USER || 'test-user',
  password: process.env.NATS_PASSWORD || 'test-password',
};

// Alert severity colors for console output
const severityColors = {
  warning: '\x1b[33m',  // Yellow
  error: '\x1b[31m',    // Red
  critical: '\x1b[35m', // Magenta
};
const resetColor = '\x1b[0m';

async function main() {
  console.log('Starting Alert Service...');

  // Build connection options
  const opts = {
    servers: config.natsUrl,
    name: 'alert-service',
    user: config.user,
    pass: config.password,
    reconnect: true,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 2000,
  };

  // TLS configuration
  if (config.caFile && config.certFile && config.keyFile) {
    opts.tls = {
      caFile: config.caFile,
      certFile: config.certFile,
      keyFile: config.keyFile,
    };
  }

  try {
    const nc = await connect(opts);
    console.log(`Connected to NATS at ${config.natsUrl}`);

    const js = nc.jetstream();
    const jsm = await nc.jetstreamManager();

    // Wait for ALERTS stream to exist
    console.log('Waiting for ALERTS stream...');
    let streamReady = false;
    while (!streamReady) {
      try {
        await jsm.streams.info('ALERTS');
        streamReady = true;
        console.log('ALERTS stream found');
      } catch (err) {
        if (err.message.includes('stream not found')) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
          throw err;
        }
      }
    }

    // Create durable consumer for alerts
    const consumerConfig = {
      durable_name: 'alert-service',
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.New,
    };

    try {
      await jsm.consumers.add('ALERTS', consumerConfig);
    } catch (err) {
      // Consumer may already exist
      if (!err.message.includes('already exists')) {
        console.warn('Consumer setup warning:', err.message);
      }
    }

    // Subscribe to all alerts
    const consumer = await js.consumers.get('ALERTS', 'alert-service');
    const messages = await consumer.consume();

    console.log('Listening for alerts on alerts.>');

    // Process alerts
    (async () => {
      for await (const msg of messages) {
        try {
          const alert = JSON.parse(msg.string());
          processAlert(alert);
          msg.ack();
        } catch (err) {
          console.error('Failed to process alert:', err);
          msg.nak();
        }
      }
    })();

    // Handle shutdown
    const shutdown = async () => {
      console.log('\nShutting down Alert Service...');
      await nc.drain();
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // Keep process alive
    await nc.closed();
  } catch (err) {
    console.error('Failed to connect to NATS:', err);
    process.exit(1);
  }
}

function processAlert(alert) {
  const color = severityColors[alert.severity] || '';
  const timestamp = new Date(alert.timestamp).toLocaleString();

  console.log('');
  console.log('═'.repeat(60));
  console.log(`${color}[${alert.severity.toUpperCase()}]${resetColor} ${alert.title}`);
  console.log('─'.repeat(60));
  console.log(`Time:    ${timestamp}`);
  console.log(`Source:  ${alert.source}`);
  console.log(`Message: ${alert.message}`);

  if (alert.context) {
    console.log('Context:');
    for (const [key, value] of Object.entries(alert.context)) {
      if (value !== null && value !== undefined) {
        console.log(`  ${key}: ${JSON.stringify(value)}`);
      }
    }
  }

  console.log('═'.repeat(60));

  // Here you could add integrations:
  // - sendToSlack(alert);
  // - sendToDiscord(alert);
  // - sendEmail(alert);
  // - sendToPagerDuty(alert);
}

main().catch(console.error);
