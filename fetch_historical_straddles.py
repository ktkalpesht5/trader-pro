import httpx
import json
from datetime import datetime

BASE_URL = "https://api.india.delta.exchange"

def fetch_expired_straddles():
    """Fetch all expired BTC move_options (straddles)."""
    url = f"{BASE_URL}/v2/products"
    params = {
        "status": "expired",
        "contract_types": "move_options",
        "underlying_asset_symbols": "BTC"
    }

    resp = httpx.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return []

    data = resp.json()
    straddles = data.get("result", [])
    print(f"✓ Fetched {len(straddles)} expired BTC straddles")
    return straddles

def extract_expiry_date(symbol):
    """Extract expiry date from symbol like MV-BTC-70600-200326 -> 2026-03-20"""
    try:
        parts = symbol.split('-')
        if len(parts) >= 4:
            date_str = parts[-1]  # DDMMYY format
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = int(date_str[4:6])
            # Handle 2-digit year
            if year < 50:
                year += 2000
            else:
                year += 1900
            return f"{year}-{month:02d}-{day:02d}"
    except:
        pass
    return None

def filter_fridays(straddles, start_date_str="2025-09-28", end_date_str="2026-04-17"):
    """Filter straddles that expire on Fridays within date range."""
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    friday_straddles = {}
    
    for straddle in straddles:
        symbol = straddle.get("symbol", "")
        expiry_date_str = extract_expiry_date(symbol)
        
        if not expiry_date_str:
            continue
            
        expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        
        # Filter: within date range and is Friday (weekday 4)
        if start <= expiry_date <= end and expiry_date.weekday() == 4:
            if expiry_date_str not in friday_straddles:
                friday_straddles[expiry_date_str] = []
            friday_straddles[expiry_date_str].append(straddle)
    
    return friday_straddles

def main():
    # Fetch all expired straddles
    straddles = fetch_expired_straddles()
    
    # Filter for Fridays in range
    friday_straddles = filter_fridays(straddles)
    
    print(f"✓ Found {len(friday_straddles)} Friday expirations with straddles")
    
    # Organize by date
    friday_data = []
    for date in sorted(friday_straddles.keys()):
        friday_data.append({
            "expiry_date": date,
            "count": len(friday_straddles[date]),
            "straddles": friday_straddles[date]
        })
    
    # Save to JSON
    with open("historical_straddles.json", "w") as f:
        json.dump(friday_data, f, indent=2)
    
    print(f"✓ Saved {len(friday_data)} Friday expirations to historical_straddles.json")
    
    # Print summary
    total_straddles = sum(d["count"] for d in friday_data)
    print(f"✓ Total straddles across all Fridays: {total_straddles}")
    print(f"\nFriday expirations:")
    for d in friday_data:
        print(f"  {d['expiry_date']}: {d['count']} straddles")

if __name__ == "__main__":
    main()
