import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer
from threading import Thread
import uvicorn

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("radar_bridge")

app = FastAPI()
clients = set()
latest_positions = {}

# Track previous statuses to avoid spamming logs
last_known_status = {}


# === KAFKA CONSUMER ===
def consume_kafka():
    log.info("🚀 Starting Kafka consumer on localhost:9092, topic=telemetry.raw")
    try:
        consumer = KafkaConsumer(
            "telemetry.raw",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="radar-bridge-local",
            auto_offset_reset="earliest",
        )
        log.info("✅ Kafka consumer connected — waiting for telemetry...")
    except Exception as e:
        log.error(f"❌ Failed to connect to Kafka: {e}")
        return

    for msg in consumer:
        try:
            data = msg.value
            ship_id = data.get("ship_id") or "UNKNOWN"

            # === Final mission completion handling ===
            mission_status = data.get("mission_status")
            if mission_status in ("SUCCESS", "FAILURE"):
                log.info(f"🛰️ {ship_id} mission {mission_status}, removing from radar.")
                latest_positions.pop(ship_id, None)
                last_known_status.pop(ship_id, None)
                continue

            # === Update current telemetry ===
            latest_positions[ship_id] = data

            # Track and log state changes
            current_status = data.get("status", "OK")
            previous_status = last_known_status.get(ship_id)

            if previous_status != current_status:
                emoji = {
                    "OK": "🟨",
                    "REPAIR": "🟥",
                    "FAILURE": "⚫"
                }.get(current_status, "🟦")
                log.info(f"{emoji} {ship_id} now {current_status}")
                last_known_status[ship_id] = current_status

        except Exception as e:
            log.warning(f"⚠️ Bad message or parse error: {e} → {msg.value}")


# === WEBSOCKET BRIDGE ===
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    log.info(f"🌐 Client connected → total {len(clients)}")

    try:
        while True:
            payload = list(latest_positions.values())
            await ws.send_json(payload)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        clients.remove(ws)
        log.info(f"❎ Client disconnected → total {len(clients)}")


# === MAIN ===
if __name__ == "__main__":
    Thread(target=consume_kafka, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")