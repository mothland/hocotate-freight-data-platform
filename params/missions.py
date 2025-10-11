import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import traceback

# === Load .env ===
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True, encoding="utf-8")

API_URL = os.getenv("API_URL", "http://localhost:8000")

def register_mission(mission_id, ship, captain, target, freq):
    data = {
        "mission_id": mission_id,
        "ship": ship,
        "captain": captain,
        "target": target,
        "freq": freq,
    }

    print(f"\n🚀 Registering mission {mission_id} for ship '{ship}' → {target} via API ({API_URL})")

    try:
        resp = requests.post(f"{API_URL}/register_mission", params=data, timeout=10)
        resp.raise_for_status()
        print("✅ Mission registered:", resp.json())
        return resp.json()
    except requests.exceptions.RequestException as e:
        print("\n❌ Mission registration failed ❌")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {e}")
        traceback.print_exc()
        raise