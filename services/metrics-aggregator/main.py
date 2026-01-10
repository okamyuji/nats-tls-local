#!/usr/bin/env python3
"""
Metrics Aggregator Service
Consumes processed metrics from log-processor and publishes dashboard updates.
"""

import asyncio
import json
import os
import signal
import ssl
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy


@dataclass
class AggregatedMetrics:
    """Aggregated metrics over a time window."""
    window_start: datetime = field(default_factory=datetime.utcnow)
    total_logs: int = 0
    by_level: Dict[str, int] = field(default_factory=dict)
    by_service: Dict[str, int] = field(default_factory=dict)
    error_rates: List[float] = field(default_factory=list)
    samples: int = 0

    def add_sample(self, metrics: dict) -> None:
        """Add a metrics sample to the aggregation."""
        self.total_logs += metrics.get("total_logs", 0)
        self.samples += 1

        # Aggregate by level
        for level, count in metrics.get("by_level", {}).items():
            self.by_level[level] = self.by_level.get(level, 0) + count

        # Aggregate by service
        for service, count in metrics.get("by_service", {}).items():
            self.by_service[service] = self.by_service.get(service, 0) + count

        # Track error rates
        error_rate = metrics.get("error_rate", 0)
        self.error_rates.append(error_rate)

    def to_dashboard_update(self) -> dict:
        """Convert to dashboard update format."""
        avg_error_rate = (
            sum(self.error_rates) / len(self.error_rates)
            if self.error_rates else 0
        )

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "type": "metrics_update",
            "data": {
                "total_logs": self.total_logs,
                "by_level": self.by_level,
                "by_service": self.by_service,
                "avg_error_rate": round(avg_error_rate, 4),
                "samples": self.samples,
                "window_seconds": 10,
            }
        }

    def reset(self) -> None:
        """Reset the aggregator for a new window."""
        self.window_start = datetime.utcnow()
        self.total_logs = 0
        self.by_level = {}
        self.by_service = {}
        self.error_rates = []
        self.samples = 0


class MetricsAggregator:
    """Main metrics aggregation service."""

    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.js = None
        self.running = True
        self.aggregator = AggregatedMetrics()

    async def connect(self) -> None:
        """Connect to NATS with TLS."""
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        ca_file = os.getenv("NATS_CA_FILE")
        cert_file = os.getenv("NATS_CLIENT_CERT")
        key_file = os.getenv("NATS_CLIENT_KEY")
        user = os.getenv("NATS_USER", "test-user")
        password = os.getenv("NATS_PASSWORD", "test-password")

        options = {
            "servers": [nats_url],
            "name": "metrics-aggregator",
            "user": user,
            "password": password,
            "reconnect_time_wait": 2,
            "max_reconnect_attempts": -1,
        }

        # TLS configuration
        if ca_file and cert_file and key_file:
            ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            ssl_ctx.load_verify_locations(ca_file)
            ssl_ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
            options["tls"] = ssl_ctx

        self.nc = await nats.connect(**options)
        self.js = self.nc.jetstream()
        print(f"Connected to NATS at {nats_url}")

    async def subscribe_metrics(self) -> None:
        """Subscribe to processed metrics."""
        # Wait for METRICS stream to exist
        print("Waiting for METRICS stream...")
        while True:
            try:
                await self.js.stream_info("METRICS")
                print("METRICS stream found")
                break
            except Exception as e:
                if "stream not found" in str(e).lower():
                    await asyncio.sleep(2)
                else:
                    raise

        consumer_config = ConsumerConfig(
            durable_name="metrics-aggregator",
            ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.NEW,
        )

        try:
            await self.js.consumer_info("METRICS", "metrics-aggregator")
        except Exception:
            await self.js.add_consumer("METRICS", consumer_config)

        sub = await self.js.pull_subscribe(
            "metrics.processed",
            durable="metrics-aggregator",
            stream="METRICS",
        )

        print("Listening for metrics on metrics.processed")

        while self.running:
            try:
                msgs = await sub.fetch(batch=10, timeout=1)
                for msg in msgs:
                    try:
                        metrics = json.loads(msg.data.decode())
                        self.aggregator.add_sample(metrics)
                        await msg.ack()
                    except Exception as e:
                        print(f"Failed to process metrics: {e}")
                        await msg.nak()
            except nats.errors.TimeoutError:
                pass
            except Exception as e:
                if self.running:
                    print(f"Subscription error: {e}")
                    await asyncio.sleep(1)

    async def publish_dashboard_updates(self) -> None:
        """Periodically publish aggregated metrics to dashboard."""
        while self.running:
            await asyncio.sleep(10)  # Publish every 10 seconds

            if self.aggregator.samples > 0:
                update = self.aggregator.to_dashboard_update()
                try:
                    await self.js.publish(
                        "dashboard.metrics",
                        json.dumps(update).encode()
                    )
                    print(
                        f"Published dashboard update: "
                        f"{self.aggregator.total_logs} logs, "
                        f"error_rate={update['data']['avg_error_rate']:.2%}"
                    )
                except Exception as e:
                    print(f"Failed to publish dashboard update: {e}")

                self.aggregator.reset()

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        print("Shutting down Metrics Aggregator...")
        self.running = False
        if self.nc:
            await self.nc.drain()

    async def run(self) -> None:
        """Main run loop."""
        await self.connect()

        # Set up signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Run subscription and publishing concurrently
        await asyncio.gather(
            self.subscribe_metrics(),
            self.publish_dashboard_updates(),
        )


async def main():
    print("Starting Metrics Aggregator...")
    aggregator = MetricsAggregator()
    await aggregator.run()


if __name__ == "__main__":
    asyncio.run(main())
