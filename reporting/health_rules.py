from math import exp

# === HELPERS ===
def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))

def sigmoid(x):
    return 1 / (1 + exp(-x))

# === NORMALIZATION FUNCTIONS ===
def normalize_motor_temp(temp):
    """Motor temperature (C). Ideal ~70–110°C."""
    if temp < 0:
        return 0.0, True
    score = clamp(1 - abs(temp - 90) / 90)
    return score, temp > 180 or temp < 30

def normalize_engine_pressure(p):
    """Engine pressure (kPa). Ideal ~250–400."""
    if p <= 0:
        return 0.0, True
    score = clamp(1 - abs(p - 325) / 325)
    return score, p < 100 or p > 600

def normalize_core_temp(temp):
    """Power core temperature (C). Ideal ~500–700."""
    if temp <= 0:
        return 0.0, True
    score = clamp(1 - abs(temp - 600) / 600)
    return score, temp < 300 or temp > 950

def normalize_fuel(fuel_prc):
    """Fuel percentage."""
    score = clamp(fuel_prc / 100)
    return score, fuel_prc <= 5.0

# === MAIN HEALTH FUNCTION ===
def compute_health(telemetry: dict):
    """
    Compute a coherent ship health state that reflects both physical telemetry
    and operational status (repair, malfunction, failure).
    """
    status = telemetry.get("status", "OK")
    malfunctioned = telemetry.get("malfunctioned_component")

    motor_score, motor_broken = normalize_motor_temp(telemetry.get("motor_temp_c", 0))
    engine_score, engine_broken = normalize_engine_pressure(telemetry.get("engine_pressure_kpa", 0))
    core_score, core_broken = normalize_core_temp(telemetry.get("power_core_temp_c", 0))
    fuel_score, fuel_broken = normalize_fuel(telemetry.get("fuel_prc", 0))

    # weighted average (biased toward engine and motor)
    total = 0.3*motor_score + 0.3*engine_score + 0.3*core_score + 0.1*fuel_score
    total = clamp(total)

    # determine condition coherently
    if status == "REPAIR":
        condition = "REPAIRING"
        total = min(total, 0.4)
    elif status == "FAILURE":
        condition = "UNOPERATIONAL"
        total = 0.0
    elif fuel_broken or motor_broken or engine_broken or core_broken:
        condition = "UNOPERATIONAL"
    elif malfunctioned:
        condition = "UNOPERATIONAL"
        total = min(total, 0.3)
    elif total >= 0.8:
        condition = "GOOD"
    elif total >= 0.5:
        condition = "MID"
    elif total >= 0.3:
        condition = "BAD"
    else:
        condition = "CRITICAL"

    return {
        "health_scores": {
            "motor": round(motor_score, 3),
            "engine": round(engine_score, 3),
            "core": round(core_score, 3),
            "fuel": round(fuel_score, 3),
        },
        "normalized_health_total": round(total, 3),
        "ship_condition": condition,
    }