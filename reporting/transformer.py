import pandas as pd
from pathlib import Path
import hashlib


def file_checksum(path: Path) -> str:
    """Compute SHA-256 checksum for data integrity verification."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def aggregate_logs(cfg, raw_path: Path, export_path: Path, mission_id: str, date_str: str):
    mission_csv = raw_path / f"mission_{mission_id}_{date_str}.csv"

    if not mission_csv.exists():
        raise FileNotFoundError(f"❌ Missing mission CSV: {mission_csv}")

    # === Load raw CSV ===
    mission_df = pd.read_csv(mission_csv)

    # === Drop duplicates ===
    mission_df = mission_df.drop_duplicates()

    # === Validation ===
    required_cols = {"mission_id", "item", "value", "timestamp"}
    if not required_cols.issubset(mission_df.columns):
        raise ValueError(f"❌ Missing mission columns: {required_cols - set(mission_df.columns)}")

    # === Type enforcement (keep timestamp as string for far-future years) ===
    mission_df["value"] = mission_df["value"].astype(float)
    mission_df["timestamp"] = mission_df["timestamp"].astype(str)

    # === Export Parquet ===
    export_path.mkdir(exist_ok=True, parents=True)
    mission_pq = f"mission_{mission_id}_{date_str}.parquet"
    pq_path = export_path / mission_pq
    mission_df.to_parquet(pq_path, index=False)

    # === Compute checksum for verification ===
    mission_hash = file_checksum(pq_path)

    print(f"🧾 Aggregated {len(mission_df)} mission records → {mission_pq}")
    print(f"   • SHA256: {mission_hash[:12]}…")

    return mission_pq