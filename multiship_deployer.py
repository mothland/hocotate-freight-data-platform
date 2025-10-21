import random
import argparse
import subprocess
import sys
from pathlib import Path

PLANETS = [
    "Hocotate", "PNF-404", "Karut", "Giya", "Nijo", "Sozor",
    "Koppai", "Ohri", "Moyama", "Flukuey", "Enohay", "Mihama",
    "Ooji", "Ogura", "Conohan", "Ocobo", "Tagwa", "Enohee",
    "Neechki", "Koodgio", "Maxima"
]

CAPTAINS = [
    # Pikmin protagonists
    "Olimar", "Louie", "Alph", "Brittany", "Charlie",
    "Hocotate President", "Shepherd", "Collin", "Dingo", "Yonny", "Irin",
    "Auberjynn", "Raphf", "Adael", "Stace", "Miku", "Teto",
    # Pikmin fauna & plantlike crew
    "Red Pikmin", "Blue Pikmin", "Yellow Pikmin", "Rock Pikmin",
]

SHIP_NAMES = [
    "SS_DOLPHIN", "HOCOTATE_SHIP", "SS_MOTH", "SS_KITTY", "SS_NECTAR",
    "SS_DRAGONFLY", "SS_SCARAB", "SS_COMET", "SS_ORION", "SS_METEOR",
    "SS_SPACENECTAR", "SS_BULBBLOSSOM", "SS_GOLDENSTEM", "SS_RUSTBEETLE",
    "SS_MANDIBLAR", "ONION?!", "BLUE_FALCON", "ODYSSEY", "GALAXIA", "HYPERION", 
    "RAPHALZ", "SPACEBUS", "SS_BOING"
]


def generate_fleet(count: int):
    """Return 'count' unique (captain, ship, target) combos."""
    if count > 20:
        raise ValueError("❌ Max 20 concurrent ships allowed.")

    chosen_captains = random.sample(CAPTAINS, count)
    chosen_ships = random.sample(SHIP_NAMES, count)
    chosen_targets = random.choices([p for p in PLANETS if p != "Hocotate"], k=count)

    return [
        {"ship": ship, "captain": captain, "target": target}
        for ship, captain, target in zip(chosen_ships, chosen_captains, chosen_targets)
    ]


def main():
    parser = argparse.ArgumentParser(description="Launch a whimsical Hocotate Freight fleet.")
    parser.add_argument("--count", type=int, default=5, help="Number of ships to launch (max 20).")
    parser.add_argument("--reportfreq", type=int, default=1, help="Minutes between reports.")
    parser.add_argument("--dry-run", action="store_true", help="Show commands only.")
    args = parser.parse_args()

    fleet = generate_fleet(args.count)

    # Use sys.executable to get the venv's Python interpreter
    python_exe = sys.executable

    print(f"🌌 Deploying {args.count} unique vessels from Hocotate...\n")
    print(f"Using Python from: {python_exe}\n")
    
    for f in fleet:
        cmd = [
            python_exe,
            "deploy_ship.py",
            f'--ship={f["ship"]}',
            f'--captain={f["captain"]}',
            f'--target={f["target"]}',
            f'--reportfreq={args.reportfreq}',
        ]
        cmd_str = " ".join(cmd)
        print(cmd_str)
        if not args.dry_run:
            subprocess.Popen(cmd)

    print("\n✅ Fleet dispatched!  Each pilot and ship is unique and canon-friendly.")


if __name__ == "__main__":
    main()