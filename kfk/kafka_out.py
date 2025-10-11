from kafka import KafkaProducer
import json
import numpy as np


def clean_numpy(obj):
    """Recursively convert NumPy types into JSON-safe types."""
    if isinstance(obj, dict):
        return {k: clean_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_numpy(i) for i in obj]
    elif isinstance(obj, (np.generic,)):  # handles np.float64, np.int32, etc.
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


def json_serializer(v):
    """Robust JSON serializer that never hangs on NumPy or exotic types."""
    try:
        clean_v = clean_numpy(v)
        return json.dumps(clean_v, ensure_ascii=False).encode("utf-8")
    except Exception as e:
        # If serialization fails, send a minimal fallback payload
        print(f"⚠️ Serialization error for value {v}: {e}")
        return json.dumps({"serialization_error": str(e)}).encode("utf-8")


def make_producer(bootstrap="localhost:9092"):
    """Create a Kafka producer for JSON messages."""
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,  # FIXED: was "1" (string), now 1 (int)
        linger_ms=50,
        value_serializer=json_serializer,
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        request_timeout_ms=20000,
        retries=3,
    )


def send(producer, topic, key, value):
    """Send one telemetry event to Kafka (non-blocking)."""
    try:
        producer.send(topic, key=key, value=value)
    except Exception as e:
        print(f"⚠️ Kafka send error: {e}")


def close_producer(producer, timeout=10):
    """Flush remaining messages and close the producer cleanly."""
    try:
        producer.flush(timeout)
        producer.close(timeout)
        print("🧹 Kafka producer closed cleanly.")
    except Exception as e:
        print(f"⚠️ Producer close warning: {e}")