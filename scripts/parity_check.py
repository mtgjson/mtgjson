"""
Parity check: compare generated MTGJSON files against CDN source.
Reports differences in a digestible format.

Compares:
- AllPrintings.json
- AtomicCards.json
- SetList.json
- Sample deck files
"""

import gzip
import json
import time
import urllib.request
from pathlib import Path


# Local paths
GENERATED_DIR = Path(
	r"C:\Users\rprat\projects\mtgjson-projects\mtgjson-v5.worktrees\master\mtgjson_build_5.2.1+WindowsDev"
)
REPORT_PATH = Path(r"C:\Users\rprat\projects\mtgjson-projects\mtgjson-v5.worktrees\master\scripts\comparison_report.md")
CACHE_DIR = Path(r"C:\Users\rprat\projects\mtgjson-projects\mtgjson-v5.worktrees\master\scripts\.cdn_cache")

# MTGJSON CDN base URL
MTGJSON_CDN = "https://mtgjson.com/api/v5"

# Cache freshness threshold (hours)
CACHE_MAX_AGE_HOURS = 48

# Sample decks to compare
SAMPLE_DECKS = [
	"CounterBlitzFinalFantasyX_FIC",
	"LifeBoost_8ED",
	"CoreSet2021Redemption_M21",
	"LilLegends_SLD",
	"Lightning1_JMP",
]


def load_json(path: Path) -> dict:
	with path.open("r", encoding="utf-8") as f:
		return json.load(f)


def is_cache_fresh(cache_path: Path) -> bool:
	if not cache_path.exists():
		return False
	age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
	return age_hours < CACHE_MAX_AGE_HOURS


def fetch_json(url: str, cache_name: str) -> dict | None:
	"""Fetch JSON from CDN with caching."""
	CACHE_DIR.mkdir(parents=True, exist_ok=True)
	cache_path = CACHE_DIR / f"{cache_name}.json"

	if is_cache_fresh(cache_path):
		print(f"  [cache] {cache_name}")
		return load_json(cache_path)

	try:
		fetch_url = url if url.endswith(".gz") else url + ".gz"
		print(f"  [fetch] {fetch_url}")
		req = urllib.request.Request(fetch_url, headers={"User-Agent": "MTGJSON-Compare/1.0"})

		with urllib.request.urlopen(req, timeout=120) as response:
			data = response.read()
			if fetch_url.endswith(".gz"):
				data = gzip.decompress(data)
			result = json.loads(data.decode("utf-8"))
			cache_path.write_text(json.dumps(result), encoding="utf-8")
			return result
	except Exception as e:
		print(f"  [error] {e}")
		return None


def diff_values(source, generated, path="") -> list[str]:
	"""Recursively diff two values, return list of differences."""
	diffs = []

	if type(source) is not type(generated):
		diffs.append(f"{path}: type mismatch (source={type(source).__name__}, gen={type(generated).__name__})")
		return diffs

	if isinstance(source, dict):
		src_keys = set(source.keys())
		gen_keys = set(generated.keys())

		for key in src_keys - gen_keys:
			diffs.append(f"{path}.{key}: missing from generated")
		for key in gen_keys - src_keys:
			diffs.append(f"{path}.{key}: extra in generated")

		for key in src_keys & gen_keys:
			diffs.extend(diff_values(source[key], generated[key], f"{path}.{key}"))

	elif isinstance(source, list):
		if len(source) != len(generated):
			diffs.append(f"{path}: length mismatch (source={len(source)}, gen={len(generated)})")
		else:
			for i, (s, g) in enumerate(zip(source, generated, strict=False)):
				diffs.extend(diff_values(s, g, f"{path}[{i}]"))

	elif source != generated:
		src_str = str(source)[:50]
		gen_str = str(generated)[:50]
		diffs.append(f"{path}: value mismatch (source={src_str!r}, gen={gen_str!r})")

	return diffs


def compare_file(name: str, source: dict | None, generated: dict | None) -> list[str]:
	"""Compare source vs generated, return report lines."""
	lines = [f"## {name}\n"]

	if source is None:
		lines.append("**ERROR:** Could not fetch from CDN\n")
		return lines

	if generated is None:
		lines.append("**ERROR:** Local file not found\n")
		return lines

	diffs = diff_values(source, generated)

	if not diffs:
		lines.append("**PASS:** Identical\n")
	else:
		lines.append(f"**FAIL:** {len(diffs)} differences\n")
		for diff in diffs[:100]:
			lines.append(f"- `{diff}`")
		if len(diffs) > 100:
			lines.append(f"- ... and {len(diffs) - 100} more")
		lines.append("")

	return lines


def main():
	report = ["# MTGJSON Parity Report\n"]
	report.append(f"**Source:** MTGJSON CDN (cache: {CACHE_MAX_AGE_HOURS}h)")
	report.append(f"**Generated:** {GENERATED_DIR.name}\n")

	# AllPrintings
	print("AllPrintings.json")
	source = fetch_json(f"{MTGJSON_CDN}/AllPrintings.json", "AllPrintings")
	gen_path = GENERATED_DIR / "AllPrintings.json"
	generated = load_json(gen_path) if gen_path.exists() else None
	report.extend(compare_file("AllPrintings.json", source, generated))

	# AtomicCards
	print("AtomicCards.json")
	source = fetch_json(f"{MTGJSON_CDN}/AtomicCards.json", "AtomicCards")
	gen_path = GENERATED_DIR / "AtomicCards.json"
	generated = load_json(gen_path) if gen_path.exists() else None
	report.extend(compare_file("AtomicCards.json", source, generated))

	# SetList
	print("SetList.json")
	source = fetch_json(f"{MTGJSON_CDN}/SetList.json", "SetList")
	gen_path = GENERATED_DIR / "SetList.json"
	generated = load_json(gen_path) if gen_path.exists() else None
	report.extend(compare_file("SetList.json", source, generated))

	# Decks
	for deck_name in SAMPLE_DECKS:
		print(f"Deck: {deck_name}")
		source = fetch_json(f"{MTGJSON_CDN}/decks/{deck_name}.json", f"deck_{deck_name}")
		gen_path = GENERATED_DIR / "decks" / f"{deck_name}.json"
		generated = load_json(gen_path) if gen_path.exists() else None
		report.extend(compare_file(f"decks/{deck_name}.json", source, generated))

	# Write report
	report_text = "\n".join(report)
	REPORT_PATH.write_text(report_text, encoding="utf-8")
	print(f"\nReport: {REPORT_PATH}")


if __name__ == "__main__":
	main()
