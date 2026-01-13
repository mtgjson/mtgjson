import json
import pathlib
import sys
import random
import subprocess

CACHE_DIR = pathlib.Path(__file__).parent / "mtgjson5" / "resources" / ".cache"
BUILD_DIR = pathlib.Path(__file__).parent / "mtgjson_build_5.2.1+WindowsDev"


def load_official(set_code: str) -> dict:
    path = CACHE_DIR / f"official_{set_code.upper()}.json"
    if not path.exists():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cmd = ["curl", "-o", str(path), f"https://mtgjson.com/api/v5/{set_code.upper()}.json"]
        subprocess.run(cmd, check=True)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_pipeline(set_code: str) -> dict:
    path = BUILD_DIR / f"{set_code.upper()}.json"
    if not path.exists():
        cmd = f"python -m mtgjson5 --sets {set_code.upper()}"
        subprocess.run(cmd, check=True)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def select_card_object(set_code: str, num_cards: int = 1) -> dict:
    official = load_official(set_code)
    pipeline = load_pipeline(set_code)

    uuids = random.sample([c['uuid'] for c in official['data']['cards']], num_cards)
    sample_uuids = set(uuids)
    
    for uuid in sample_uuids:
        off_card = next((c for c in official['data']['cards'] if c['uuid'] == uuid), None)
        pip_card = next((c for c in pipeline['data']['cards'] if c['uuid'] == uuid), None)    
        if off_card and pip_card:
            json.dumps(off_card, indent=2)
            json.dumps(pip_card, indent=2)
    
    
def compare_json(set_code: str) -> None:
    official = load_official(set_code)
    pipeline = load_pipeline(set_code)

    # Match by name+number+side since UUIDs may differ
    def card_key(c):
        return (c['name'], c.get('number', ''), c.get('side', ''))

    official_cards = {card_key(c): c for c in official['data']['cards']}
    pipeline_cards = {card_key(c): c for c in pipeline['data']['cards']}

    print(f"Official cards: {len(official_cards)}")
    print(f"Pipeline cards: {len(pipeline_cards)}")

    # Check for missing/extra cards
    missing = set(official_cards.keys()) - set(pipeline_cards.keys())
    extra = set(pipeline_cards.keys()) - set(official_cards.keys())
    print(f"Missing from pipeline: {len(missing)}")
    print(f"Extra in pipeline: {len(extra)}")

    if missing:
        print("\nMissing cards:")
        for uuid in list(missing)[:5]:
            print(f"  - {official_cards[uuid]['name']}")

    # Compare fields on matching cards
    common_uuids = set(official_cards.keys()) & set(pipeline_cards.keys())
    field_diffs = {}
    sample_diffs = {}

    for uuid in common_uuids:
        off = official_cards[uuid]
        pip = pipeline_cards[uuid]

        all_keys = set(off.keys()) | set(pip.keys())
        for key in all_keys:
            off_val = off.get(key)
            pip_val = pip.get(key)

            # Normalize for comparison
            if off_val == [] and pip_val is None:
                continue
            if off_val is None and pip_val == []:
                continue
            if off_val == "" and pip_val is None:
                continue
            if pip_val == "" and off_val is None:
                continue

            if off_val != pip_val:
                if key not in field_diffs:
                    field_diffs[key] = 0
                    sample_diffs[key] = (off["name"], off_val, pip_val)
                field_diffs[key] += 1

    print(f"\n=== Field differences across {len(common_uuids)} cards ===")
    for key, count in sorted(field_diffs.items(), key=lambda x: -x[1])[:25]:
        name, off_val, pip_val = sample_diffs[key]
        off_str = str(off_val)[:60] if off_val else "None"
        pip_str = str(pip_val)[:60] if pip_val else "None"
        print(f"\n{key}: {count} diffs")
        print(f"  Example ({name}):")
        print(f"    Official: {off_str}")
        print(f"    Pipeline: {pip_str}")
        
def fast_compare(set_code: str, num: int = 1):
    set_code = set_code.upper()
    for _ in range(num):
        select_card_object(set_code, 1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parity_check.py <SET_CODE>")
        sys.exit(1)
    set_code = sys.argv[1]
    fast_compare(set_code, 1)
