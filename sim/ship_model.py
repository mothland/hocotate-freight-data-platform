from dataclasses import dataclass, field
from datetime import datetime, timezone
import numpy as np
import random

# === PLANET COORDINATES (km) ===
PLANET_COORDS = {
    "Hocotate": np.array([0.0, 0.0, 0.0]),

    # inner band
    "PNF-404": np.array([0.7e8, -1.3e8, -1.5e8]),
    "Karut":   np.array([1.2e8, 0.8e8, 0.1e8]),
    "Giya":    np.array([-1.5e8, 0.3e8, -0.6e8]),
    "Nijo":    np.array([0.7e8, -1.1e8, 0.2e8]),
    "Sozor":   np.array([-0.9e8, -0.6e8, 0.5e8]),

    # mid band
    "Koppai":  np.array([6.8e8, 2.1e8, -1.2e8]),
    "Ohri":    np.array([5.2e8, -2.4e8, 2.9e8]),
    "Moyama":  np.array([-6.1e8, 3.5e8, 0.9e8]),
    "Flukuey": np.array([4.0e8, -3.8e8, -1.0e8]),
    "Enohay":  np.array([3.3e8, 1.9e8, 2.2e8]),
    "Mihama":  np.array([-3.7e8, -1.6e8, 2.6e8]),
    "Ooji":    np.array([7.5e8, 0.0e8, 0.8e8]),
    "Ogura":   np.array([-5.5e8, 2.2e8, -2.3e8]),

    # outer band
    "Conohan": np.array([9.4e8, -4.1e8, 1.6e8]),
    "Ocobo":   np.array([-1.0e9, 3.8e8, -1.9e8]),
    "Tagwa":   np.array([1.12e9, 1.1e8, -3.4e8]),
    "Enohee":  np.array([-1.25e9, -0.2e9, 2.7e8]),
    "Neechki": np.array([1.35e9, -3.0e8, 0.0e8]),
    "Koodgio": np.array([-1.10e9, 4.5e8, 3.2e8]),
    "Maxima":  np.array([1.45e9, 0.0e8, 4.0e8]),
}


# === SHIP MODEL ===
@dataclass
class Ship:
    mission_id: str
    ship_id: str
    captain: str
    target: str
    speed_km_per_sec: float = 2.4e6

    # dynamic state
    pos: np.ndarray = field(default_factory=lambda: np.array([0.0, 0.0, 0.0]))
    fuel_prc: float = 100.0

    # subsystem metrics — start healthy
    motor_temp_c: float = 90.0
    engine_pressure_kpa: float = 325.0
    power_core_temp_c: float = 600.0
    navigation_signal_db: float = -70.0

    # malfunction state
    malfunctioned_component: str = None
    repair_ticks_left: int = 0

    # === CORE UPDATE LOOP ===
    def update(self):
        """Advance ship position and subsystem metrics for one tick."""
        # Move toward target only if not under repair
        if self.repair_ticks_left <= 0:
            target = PLANET_COORDS[self.target]
            delta = target - self.pos
            step = np.clip(delta, -self.speed_km_per_sec, self.speed_km_per_sec)
            close_mask = np.abs(delta) <= np.abs(step)
            self.pos = np.where(close_mask, target, self.pos + step)

        # Always burn a bit of fuel
        if self.fuel_prc > 0:
            self.fuel_prc = max(0.0, self.fuel_prc - 0.01)

        # Skip normal subsystem drift if repairing (engines off)
        if self.repair_ticks_left > 0:
            return

        # Simulate subsystem drift
        self.motor_temp_c += random.uniform(-0.3, 0.3)
        self.engine_pressure_kpa += random.uniform(-0.8, 0.8)
        self.power_core_temp_c += random.uniform(-0.5, 0.5)
        self.navigation_signal_db += random.uniform(-0.2, 0.2)

        # Clamp values
        self.motor_temp_c = max(20, min(130, self.motor_temp_c))
        self.engine_pressure_kpa = max(100, min(600, self.engine_pressure_kpa))
        self.power_core_temp_c = max(300, min(950, self.power_core_temp_c))
        self.navigation_signal_db = max(-100, min(-50, self.navigation_signal_db))

    # === MALFUNCTION LOGIC ===
    def start_malfunction(self):
        """Trigger a random subsystem malfunction (not fuel)."""
        components = ["motor_temp_c", "engine_pressure_kpa",
                      "power_core_temp_c", "navigation_signal_db"]
        self.malfunctioned_component = random.choice(components)
        self.repair_ticks_left = 5
        print(f"⚠️ CRITICAL: {self.ship_id} subsystem '{self.malfunctioned_component}' malfunctioned! "
              "Repairs initiated (5 ticks).")

    def attempt_repair(self) -> bool:
        """
        Attempt one repair tick.
        Returns True if still alive/repairing, False if repair failed completely.
        """
        # Failure chance decreases each tick: 5%, 4%, 3%, 2%, 1%, 0%
        fail_chance = max(0.0, 0.05 - (0.01 * (5 - self.repair_ticks_left)))
        if random.random() < fail_chance:
            print(f"💀 {self.ship_id} repair failed on '{self.malfunctioned_component}' "
                  f"(fail chance {fail_chance*100:.1f}%). Ship lost!")
            return False

        self.repair_ticks_left -= 1
        if self.repair_ticks_left <= 0:
            print(f"🛠️ {self.ship_id} repairs completed successfully! "
                  f"Subsystem '{self.malfunctioned_component}' restored.")
            self.malfunctioned_component = None
        else:
            print(f"🔧 Repairing '{self.malfunctioned_component}'... "
                  f"{self.repair_ticks_left} ticks remaining "
                  f"(fail chance {fail_chance*100:.1f}%)")

        return True

    # === TELEMETRY ===
    def to_telemetry(self) -> dict:
        """Builds a clean telemetry event dict for Kafka."""
        now = datetime.now(timezone.utc).isoformat()
        origin = PLANET_COORDS["Hocotate"]
        dist = float(np.linalg.norm(self.pos - origin))
        return {
            "timestamp": now,
            "mission_id": self.mission_id,
            "ship_id": self.ship_id,
            "coord_x": float(self.pos[0]),
            "coord_y": float(self.pos[1]),
            "coord_z": float(self.pos[2]),
            "fuel_prc": round(self.fuel_prc, 2),
            "motor_temp_c": round(self.motor_temp_c, 2),
            "engine_pressure_kpa": round(self.engine_pressure_kpa, 2),
            "power_core_temp_c": round(self.power_core_temp_c, 2),
            "navigation_signal_db": round(self.navigation_signal_db, 2),
            "distance_from_hocotate_km": dist,
        }

    # === REACHED TARGET ===
    def reached_target(self) -> bool:
        """Returns True if ship reached target coordinates."""
        target = PLANET_COORDS[self.target]
        return np.allclose(self.pos, target, atol=self.speed_km_per_sec)