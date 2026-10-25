const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { execFileSync, spawn, spawnSync } = require('node:child_process');
const path = require('node:path');
const pkg = require('../package.json');

// サーバー操作はnats-boxのCLIで行い、テスト対象のクライアントライブラリに依存しない
const SERVER_IMAGE = 'nats:2.10.7-alpine';
const BOX_IMAGE = 'natsio/nats-box:0.18.1';
const suffix = `${process.pid}-${Date.now()}`;
const network = `alert-test-${suffix}`;
const server = `alert-test-nats-${suffix}`;
const auth = 'test-user:test-password';

const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8' }).trim();
const nats = (...args) =>
  docker('run', '--rm', '--network', network, BOX_IMAGE, 'nats', '-s', `nats://${auth}@${server}:4222`, ...args);

let service;
let output = '';

function poll(check, label, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      let value;
      try { value = check(); } catch { value = undefined; }
      if (value) return resolve(value);
      if (Date.now() - started > timeoutMs) return reject(new Error(`timed out waiting for ${label}`));
      setTimeout(tick, 200);
    };
    tick();
  });
}

const consumerInfo = () => JSON.parse(nats('consumer', 'info', 'ALERTS', 'alert-service', '--json'));

function waitFor(text, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (output.includes(text)) {
        clearInterval(timer);
        resolve();
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error(`timed out waiting for "${text}"\n--- output ---\n${output}`));
      }
    }, 100);
  });
}

before(async () => {
  docker('network', 'create', network);
  docker('run', '-d', '--name', server, '--network', network, '-p', '127.0.0.1::4222',
    SERVER_IMAGE, '-js', '--user', 'test-user', '--pass', 'test-password');
  // The client does not retry its first connect (waitOnFirstConnect defaults to false), so the
  // server must accept connections before the service starts.
  await poll(() => {
    const logs = spawnSync('docker', ['logs', server], { encoding: 'utf8' });
    return `${logs.stdout}${logs.stderr}`.includes('Server is ready');
  }, 'nats-server to be ready');
  const port = docker('port', server, '4222/tcp').split(':').pop();
  service = spawn(process.execPath, [path.join(__dirname, '..', 'index.js')], {
    env: { ...process.env, NATS_URL: `nats://127.0.0.1:${port}` },
  });
  service.stdout.on('data', (d) => { output += d; });
  service.stderr.on('data', (d) => { output += d; });
});

after(() => {
  if (service && service.exitCode === null) service.kill('SIGKILL');
  try { docker('rm', '-f', server); } catch {}
  try { docker('network', 'rm', network); } catch {}
});

test('does not depend on the deprecated nats package', () => {
  assert.equal(pkg.dependencies.nats, undefined);
});

test('waits for the ALERTS stream, then consumes from it', async () => {
  await waitFor('Waiting for ALERTS stream...');
  nats('stream', 'add', 'ALERTS', '--subjects', 'alerts.>', '--defaults');
  await waitFor('Listening for alerts on alerts.>');
});

test('prints an alert and acks it', async () => {
  const alert = {
    severity: 'critical',
    title: 'disk almost full',
    source: 'node-1',
    message: 'usage 97%',
    timestamp: '2026-10-25T00:00:00Z',
    context: { mount: '/data', ignored: null },
  };
  nats('pub', 'alerts.disk', JSON.stringify(alert));
  await waitFor('disk almost full');
  assert.match(output, /\[CRITICAL\]/);
  assert.match(output, /mount: "\/data"/);
  assert.doesNotMatch(output, /ignored:/);

  // ack() does not wait for the server, so poll until the ack is reflected in the consumer state.
  const info = await poll(() => {
    const current = consumerInfo();
    return current.num_ack_pending === 0 && current.delivered.stream_seq === 1 ? current : undefined;
  }, 'the alert to be acked');
  assert.equal(info.config.ack_policy, 'explicit');
  assert.equal(info.config.deliver_policy, 'new');
  assert.equal(info.delivered.stream_seq, 1);
  assert.equal(info.num_ack_pending, 0);
});

test('terminates a message that is not JSON instead of redelivering it', async () => {
  nats('pub', 'alerts.bad', 'not-json');
  await waitFor('Failed to process alert');
  await poll(() => consumerInfo().num_ack_pending === 0, 'the bad message to be settled');
  // A nak would redeliver immediately; give it the same window a redelivery would need.
  await new Promise((resolve) => setTimeout(resolve, 1000));
  assert.equal(consumerInfo().num_redelivered, 0);
  assert.equal(output.split('Failed to process alert').length - 1, 1);
});

test('drains and exits 0 on SIGTERM', async () => {
  assert.equal(service.exitCode, null, `service already exited\n--- output ---\n${output}`);
  const exited = new Promise((resolve) => service.once('exit', (code) => resolve(code)));
  service.kill('SIGTERM');
  assert.equal(await exited, 0);
  assert.match(output, /Shutting down Alert Service/);
});
