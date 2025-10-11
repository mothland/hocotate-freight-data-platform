from fastapi import FastAPI, HTTPException
import psycopg
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

app = FastAPI(title="Fleet Command API")

# === DB CONNECTION ===
def get_conn():
    return psycopg.connect(
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
    )

# === HELPER LOG ===
def log(msg: str):
    """Timestamped console logger."""
    print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)


# === ROOT ===
@app.get("/")
def root():
    return {"status": "Fleet Command online"}


# === REGISTER MISSION ===
@app.post("/register_mission")
def register_mission(ship_name: str, captain: str, target: str, report_freq_min: int):
    log(f"🛰 Registering mission: ship={ship_name}, captain={captain}, target={target}, freq={report_freq_min}")
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO params.missions (ship_name, captain, target, report_freq_min)
                VALUES (%s, %s, %s, %s)
                RETURNING mission_id;
            """, (ship_name, captain, target, report_freq_min))
            mission_id = cur.fetchone()[0]
            conn.commit()

        log(f"✅ Mission {mission_id} registered successfully.")
        return {
            "message": f"Mission {mission_id} registered successfully!",
            "mission_id": mission_id,
        }

    except Exception as e:
        log(f"❌ Database error during registration: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# === COMPLETE MISSION ===
@app.post("/complete_mission")
def complete_mission(mission_id: int, status: str):
    log(f"🧾 Completing mission {mission_id} with status={status.upper()}")
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Log DB identity (for multi-instance debugging)
            cur.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
            info = cur.fetchone()
            log(f"📡 Connected to DB={info[0]} user={info[1]} host={info[2]} port={info[3]}")

            cur.execute("""
                UPDATE params.missions
                   SET status = %s,
                       finished_at = NOW()
                 WHERE mission_id = %s;
            """, (status.upper(), mission_id))

            if cur.rowcount == 0:
                log(f"⚠️ Mission {mission_id} not found — no rows updated.")
                raise HTTPException(status_code=404, detail=f"Mission {mission_id} not found")

            conn.commit()
            log(f"✅ Mission {mission_id} marked as {status.upper()}")

        return {"message": f"Mission {mission_id} marked as {status.upper()}"}

    except Exception as e:
        log(f"❌ Database error during completion of mission {mission_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


# === UVICORN ENTRYPOINT ===
if __name__ == "__main__":
    import uvicorn
    log("🚀 Fleet Command API launching...")
    uvicorn.run(app, host="0.0.0.0", port=8000)