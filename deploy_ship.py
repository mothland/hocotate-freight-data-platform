import argparse
import asyncio
import os
import random
import requests
from datetime import datetime
from sim.ship_model import Ship, PLANET_COORDS
from kfk.kafka_out import make_producer, send
from reporting.ship_simulator import generate_report
from reporting.health_rules import compute_health

# === CONFIG ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telemetry.raw")
REGISTER_URL = os.getenv("MISSIONS_API", "http://localhost:8000/register_mission")
COMPLETE_URL = os.getenv("COMPLETE_API", "http://localhost:8000/complete_mission")


# === TELEMETRY LOOP ===
async def telemetry_loop(ship: Ship, producer, hz: float = 1.0):
    """Emit telemetry to Kafka, simulate malfunctions, and return mission status."""
    print("Telemetry started!")
    period = 1.0 / hz
    malfunction_odds = 0.005  # 0.5% per tick

    while True:
        # === REPAIR MODE ===
        if ship.repair_ticks_left > 0:
            event = ship.to_telemetry()
            event["status"] = "REPAIR"
            event["malfunctioned_component"] = ship.malfunctioned_component

            health = compute_health(event)
            event.update(health)
            send(producer, TOPIC, key=ship.ship_id, value=event)

            print(f"⏸️ {ship.ship_id} repairing {ship.malfunctioned_component} "
                  f"({ship.repair_ticks_left} ticks left, "
                  f"cond={health['ship_condition']}, "
                  f"health={health['normalized_health_total']:.2f})")

            # Attempt repair (can fail)
            if not ship.attempt_repair():
                event["status"] = "FAILURE"
                event["mission_status"] = "FAILURE"
                event.update(compute_health(event))
                send(producer, TOPIC, key=ship.ship_id, value=event)
                producer.flush()
                print(f"💀 {ship.ship_id} failed repair — mission failed.")
                return "FAILURE"

            # Fuel still burns during repairs
            if ship.fuel_prc > 0:
                ship.fuel_prc = max(0.0, ship.fuel_prc - 0.01)

            await asyncio.sleep(period)
            continue

        # === NORMAL OPERATION ===
        ship.update()
        event = ship.to_telemetry()
        event["status"] = "OK"
        event["malfunctioned_component"] = ship.malfunctioned_component

        health = compute_health(event)
        event.update(health)
        send(producer, TOPIC, key=ship.ship_id, value=event)

        cond = health["ship_condition"]
        tot = health["normalized_health_total"]

        print(f"[{datetime.utcnow().isoformat()}] {ship.ship_id} -> "
              f"fuel={ship.fuel_prc:.2f}%, cond={cond:<12} (health={tot:.2f}) "
              f"coords=({ship.pos[0]:.0f},{ship.pos[1]:.0f},{ship.pos[2]:.0f})")

        # Random malfunction trigger
        if random.random() < malfunction_odds and ship.malfunctioned_component is None:
            ship.start_malfunction()

        # Fuel depletion → failure
        if ship.fuel_prc <= 0:
            event["status"] = "FAILURE"
            event["mission_status"] = "FAILURE"
            event.update(compute_health(event))
            send(producer, TOPIC, key=ship.ship_id, value=event)
            producer.flush()
            print(f"💥 {ship.ship_id} ran out of fuel before reaching {ship.target}!")
            return "FAILURE"

        # Target reached → success
        if ship.reached_target():
            event["mission_status"] = "SUCCESS"
            event.update(compute_health(event))
            send(producer, TOPIC, key=ship.ship_id, value=event)
            producer.flush()
            print(f"🏁 {ship.ship_id} reached {ship.target}. Mission complete.")
            return "SUCCESS"

        await asyncio.sleep(period)


# === REPORT LOOP ===
async def report_loop(ship: Ship, freq_min: int):
    """Generate captain report every N minutes."""
    while not ship.reached_target():
        state = ship.to_telemetry()
        state["status"] = "REPAIR" if ship.repair_ticks_left > 0 else "OK"
        health = compute_health(state)
        state.update(health)
        generate_report(state, mission_id=ship.mission_id)
        print(f"📤 Captain report emitted for {ship.ship_id} (mission {ship.mission_id})")
        await asyncio.sleep(freq_min * 60)


# === FINALIZATION ===
def finalize_mission(ship: Ship, status: str):
    """Notify API and upload final report."""
    print(f"🧾 Finalizing mission {ship.mission_id} with status={status}")
    try:
        resp = requests.post(
            COMPLETE_URL,
            params={"mission_id": ship.mission_id, "status": status},
            timeout=10
        )
        print(f"📡 Mission completion sent → {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[WARN] Could not update mission status: {e}")

    # Generate one final report
    try:
        final_state = ship.to_telemetry()
        final_state["status"] = status
        health = compute_health(final_state)
        final_state.update(health)
        generate_report(final_state, mission_id=ship.mission_id)
        print("📤 Final report generated and uploaded.")
    except Exception as e:
        print(f"[WARN] Failed to generate final report: {e}")


# === MAIN ORCHESTRATION ===
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ship", required=True)
    parser.add_argument("--captain", required=True)
    parser.add_argument("--target", required=True, choices=list(PLANET_COORDS.keys()))
    parser.add_argument("--reportfreq", type=int, default=2, help="minutes between reports")
    args = parser.parse_args()

    # Register mission via API
    try:
        payload = {
            "ship_name": args.ship,
            "captain": args.captain,
            "target": args.target,
            "report_freq_min": args.reportfreq,
        }
        print(f"🚀 Registering mission via API: {REGISTER_URL}")
        response = requests.post(REGISTER_URL, params=payload)
        response.raise_for_status()
        mission_id = response.json()["mission_id"]
        print(f"🛰 Mission {mission_id} registered for ship '{args.ship}'")
    except Exception as e:
        print(f"❌ Failed to register mission via API: {e}")
        return

    # Build ship + Kafka producer
    ship = Ship(
        mission_id=str(mission_id),
        ship_id=args.ship,
        captain=args.captain,
        target=args.target,
        pos=PLANET_COORDS["Hocotate"].copy()
    )
    producer = make_producer(KAFKA_BOOTSTRAP)

    # Run telemetry and reports concurrently
    telemetry_task = asyncio.create_task(telemetry_loop(ship, producer))
    report_task = asyncio.create_task(report_loop(ship, args.reportfreq))

    status = await telemetry_task
    report_task.cancel()

    # Send final status and reports
    finalize_mission(ship, status)
    print(f"✅ Mission {mission_id} ended with status={status}.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMission aborted by operator.")