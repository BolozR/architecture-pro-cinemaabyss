import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("events-service")

PORT = int(os.getenv("PORT", "8082"))
BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events",
}

producer = None
producer_lock = threading.Lock()

try:
    from kafka import KafkaProducer, KafkaConsumer  # type: ignore
except Exception as exc:  # pragma: no cover
    KafkaProducer = None  # type: ignore
    KafkaConsumer = None  # type: ignore
    log.error("kafka-python is not available; event publishing is disabled: %s", exc)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_producer():
    global producer
    if KafkaProducer is None:
        return None
    with producer_lock:
        if producer is not None:
            return producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=[x.strip() for x in BROKERS.split(",") if x.strip()],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                retries=3,
                acks="all",
                linger_ms=10,
                request_timeout_ms=2000,
                api_version_auto_timeout_ms=1000,
            )
            log.info("Kafka producer connected to %s", BROKERS)
        except Exception as exc:
            log.warning("Kafka producer is not ready, event will be handled in fallback queue: %s", exc)
            producer = None
        return producer


def reset_producer():
    global producer
    with producer_lock:
        if producer is not None:
            try:
                producer.close(timeout=1)
            except Exception:
                pass
        producer = None


def publish(topic: str, event: Dict[str, Any]) -> Tuple[int, int]:
    """Publish an event only after Kafka acknowledges it.

    Returning success for an in-memory fallback made the API look healthy while
    the event was absent from Kafka. A temporary broker outage is now surfaced
    to the caller so it can retry without silently losing the event.
    """
    last_error = None
    for attempt in range(1, 6):
        prod = get_producer()
        if prod is None:
            last_error = RuntimeError("Kafka producer is not ready")
            time.sleep(attempt)
            continue
        try:
            meta = prod.send(topic, key=event["id"], value=event).get(timeout=10)
            prod.flush(timeout=5)
            partition, kafka_offset = int(meta.partition), int(meta.offset)
            log.info("published event_id=%s topic=%s partition=%s offset=%s", event["id"], topic, partition, kafka_offset)
            return partition, kafka_offset
        except Exception as exc:
            last_error = exc
            log.warning("Kafka publish attempt %s/5 failed: %s", attempt, exc)
            reset_producer()
            time.sleep(attempt)
    raise RuntimeError(f"Kafka publish failed after retries: {last_error}")


def kafka_consumer(topic: str):
    if KafkaConsumer is None:
        return
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[x.strip() for x in BROKERS.split(",") if x.strip()],
                group_id=f"events-service-{topic}",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,
                api_version_auto_timeout_ms=1000,
            )
            log.info("Kafka consumer started for topic=%s", topic)
            while True:
                for msg in consumer:
                    event = msg.value
                    log.info("processed kafka event topic=%s partition=%s offset=%s event_id=%s type=%s", msg.topic, msg.partition, msg.offset, event.get("id"), event.get("type"))
                time.sleep(1)
        except Exception as exc:
            log.warning("Kafka consumer for topic=%s is not ready: %s", topic, exc)
            time.sleep(5)


class Handler(BaseHTTPRequestHandler):
    server_version = "CinemaAbyssEvents/1.0"

    def _send_json(self, status: int, payload: Any):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/health":
            self._send_json(200, {"status": True, "service": "events-service"})
            return
        if path == "/api/events/health":
            kafka_ready = get_producer() is not None
            self._send_json(
                200 if kafka_ready else 503,
                {"status": kafka_ready, "service": "events-service", "kafka": kafka_ready},
            )
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?")[0]
        event_type = None
        if path == "/api/events/movie":
            event_type = "movie"
        elif path == "/api/events/user":
            event_type = "user"
        elif path == "/api/events/payment":
            event_type = "payment"
        else:
            self._send_json(404, {"error": "not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json(400, {"error": "invalid json"})
            return

        event = {
            "id": f"{event_type}-{uuid.uuid4()}",
            "type": event_type,
            "timestamp": now_iso(),
            "payload": payload,
        }
        topic = TOPICS[event_type]
        try:
            partition, off = publish(topic, event)
        except RuntimeError as exc:
            log.error("event rejected because Kafka is unavailable: %s", exc)
            self._send_json(503, {"error": "kafka unavailable", "event_id": event["id"]})
            return
        log.info("accepted event type=%s topic=%s event_id=%s", event_type, topic, event["id"])
        self._send_json(201, {"status": "success", "partition": partition, "offset": off, "event": event})

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)


def wait_dns(host: str, timeout_sec: int = 30):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            socket.gethostbyname(host)
            return
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    broker_host = BROKERS.split(",")[0].split(":")[0]
    threading.Thread(target=wait_dns, args=(broker_host,), daemon=True).start()
    for topic in TOPICS.values():
        threading.Thread(target=kafka_consumer, args=(topic,), daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("events-service listening on :%s, kafka=%s", PORT, BROKERS)
    server.serve_forever()
