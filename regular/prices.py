import os
import csv
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv


# ----------------------------------------------------------------------
# Environment & Paths
# ----------------------------------------------------------------------

# Load .env for local development (GitHub Actions uses env vars directly)
load_dotenv()

# Absolute directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV will always be created/appended next to this script
CSV_PATH = os.path.join(BASE_DIR, "prices.csv")

API_KEY = os.getenv("CMC_API_KEY")
URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# Optional overrides via environment variables
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
CONVERT_ENV = os.getenv("CONVERT", "USD")


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

def utc_iso_z() -> str:
    """Generate a UTC timestamp like: 2025-02-27T05:57:38.000Z"""
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}" + "Z"


def normalize_symbols(symbols: list[str]) -> list[str]:
    """Normalize symbols: strip, uppercase, deduplicate (order preserved)."""
    cleaned: list[str] = []
    seen: set[str] = set()

    for s in symbols:
        if not s or not str(s).strip():
            continue
        sym = str(s).strip().upper()
        if sym not in seen:
            cleaned.append(sym)
            seen.add(sym)

    return cleaned


# ----------------------------------------------------------------------
# CoinMarketCap API
# ----------------------------------------------------------------------

def get_prices(symbols: list[str], convert: str = "USD") -> dict[str, float]:
    """
    Fetch latest prices for multiple symbols in a single API request.
    Returns: { SYMBOL: price }
    """
    if not API_KEY:
        raise RuntimeError(
            "CMC_API_KEY not found. Please set it via .env or environment variables."
        )

    symbols = normalize_symbols(symbols)
    if not symbols:
        raise RuntimeError("Symbol list is empty. Please configure at least one symbol.")

    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }

    params = {
        "symbol": ",".join(symbols),
        "convert": convert,
    }

    try:
        r = requests.get(URL, headers=headers, params=params, timeout=15)
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to call CoinMarketCap API (network/timeout): {e}") from e

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")

    data = r.json()
    status = data.get("status", {}) or {}
    if status.get("error_code", 0) != 0:
        raise RuntimeError(
            f"API error {status.get('error_code')}: {status.get('error_message')}"
        )

    returned = data.get("data", {}) or {}
    out: dict[str, float] = {}

    for sym in symbols:
        item = returned.get(sym)
        if not item:
            print(f"⚠️  Skipped {sym} (symbol not returned by API)")
            continue

        quote = (item.get("quote") or {}).get(convert) or {}
        price = quote.get("price")

        if price is None:
            print(f"⚠️  Skipped {sym} (price is None / missing)")
            continue

        try:
            out[sym] = float(price)
        except (TypeError, ValueError):
            print(f"⚠️  Skipped {sym} (invalid price value: {price!r})")

    return out


# ----------------------------------------------------------------------
# CSV Handling
# ----------------------------------------------------------------------

def ensure_header_and_append_row(
    csv_path: str,
    timestamp: str,
    symbols: list[str],
    price_map: dict[str, float],
) -> None:
    """
    Ensure CSV header exists and append a new row.
    - First column: timestamp
    - Header automatically expands if new symbols appear
    """
    symbols = normalize_symbols(symbols)
    desired_header = ["timestamp"] + symbols

    # Case 1: CSV does not exist
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=desired_header)
            writer.writeheader()
            row = {"timestamp": timestamp, **{s: price_map.get(s, "") for s in symbols}}
            writer.writerow(row)
        return

    # Read existing header
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        old_header = next(reader, None)

    # Case 2: empty file
    if not old_header:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=desired_header)
            writer.writeheader()
            row = {"timestamp": timestamp, **{s: price_map.get(s, "") for s in symbols}}
            writer.writerow(row)
        return

    # Merge headers
    old_cols = [c for c in old_header if c and c != "timestamp"]
    new_cols = [c for c in symbols if c not in old_cols]
    union_header = ["timestamp"] + old_cols + new_cols

    # Expand header if needed
    if union_header != old_header:
        rows: list[dict[str, str]] = []
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            dict_reader = csv.DictReader(f)
            for row in dict_reader:
                fixed = {h: row.get(h, "") for h in union_header}
                rows.append(fixed)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, fieldnames=union_header)
            dict_writer.writeheader()
            dict_writer.writerows(rows)

    # Append new row
    row_to_write: dict[str, object] = {"timestamp": timestamp}
    for col in union_header[1:]:
        row_to_write[col] = price_map.get(col, "")

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        dict_writer = csv.DictWriter(f, fieldnames=union_header)
        dict_writer.writerow(row_to_write)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def parse_symbols_from_env(default: list[str]) -> list[str]:
    """Parse SYMBOLS env var like 'BTC,ETH,ADA' if provided."""
    if SYMBOLS_ENV.strip():
        parts = [p.strip() for p in SYMBOLS_ENV.split(",")]
        symbols = normalize_symbols(parts)
        if symbols:
            return symbols
    return default


def main():
    default_symbols = ["BTC", "ETH", "ADA", "XRP", "SOL", "SUI", "AVAX", "POL", "API3"]
    symbols = parse_symbols_from_env(default_symbols)
    convert = (CONVERT_ENV or "USD").strip().upper()

    print("=== main() started ===")
    print(f"Symbols: {symbols} | Convert: {convert}")
    print(f"CSV output: {CSV_PATH}")

    prices = get_prices(symbols, convert)
    ts = utc_iso_z()
    ensure_header_and_append_row(CSV_PATH, ts, symbols, prices)

    print(f"✅ Appended 1 row @ {ts}")


if __name__ == "__main__":
    main()
