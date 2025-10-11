import numpy as np
import json
from kafka_out import clean_numpy, json_serializer, make_producer

# Test the exact sample from tester.py
sample = {
    "timestamp": "2025-10-11T10:45:22.345Z",
    "mission_id": 3,
    "coord_x": np.float64(42.0),
    "coord_y": np.float64(24.0),
    "fuel_prc": 99.8,
}

print("Original sample:", sample)
print()

# Test clean_numpy
print("Testing clean_numpy()...")
try:
    cleaned = clean_numpy(sample)
    print("✅ clean_numpy worked:", cleaned)
except Exception as e:
    print("❌ clean_numpy failed:", e)
    import traceback
    traceback.print_exc()

print()

# Test json_serializer
print("Testing json_serializer()...")
try:
    result = json_serializer(sample)
    print("✅ json_serializer worked:", result)
except Exception as e:
    print("❌ json_serializer failed:", e)
    import traceback
    traceback.print_exc()

print()

# Test the wrapper dict from tester.py
print("Testing full wrapper...")
wrapper = {"SS_DOLPHIN": sample}
try:
    result = json_serializer(wrapper)
    print("✅ Full wrapper serialized:", result)
except Exception as e:
    print("❌ Full wrapper failed:", e)
    import traceback
    traceback.print_exc()

print()
print("=" * 60)
print("Now testing actual Kafka send with timeout...")
print("=" * 60)
print()

import threading

p = make_producer("localhost:9092")

print("Calling p.send()...")
try:
    future = p.send("telemetry.raw", {"SS_DOLPHIN": sample})
    print("✅ Send completed, waiting for result...")
    result = future.get(timeout=10)
    print("✅ Sent OK:", result)
except Exception as e:
    print("❌ Failed:", e)
    import traceback
    traceback.print_exc()
finally:
    p.close()
    print("Producer closed.")