from config import get_api
from datetime import datetime, timedelta, timezone
import sys

def main():
    if len(sys.argv) < 2:
        print("Uso: python -m scripts.fetch_devices_logs <DEVICE_ID> [dias]")
        raise SystemExit(1)
    

    device_id = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) >= 3 else 7


    api = get_api()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    items = api.get(
        "LogRecord",
        search={"device": device_id, "fromDate": start.isoformat(), "toDate": now.isoformat()},
        resultsLimit=200
    )

    print(f"Device: {device_id} | Logs (até {200}) últimos {days} dia(s): {len(items)}")
    for i, lr in enumerate(items[:10], start=1):
        print(f"\n-- {i} --")
        print("dateTime:", lr.get("dateTime"))
        print("lat:", lr.get("latitude"))
        print("lon:", lr.get("longitude"))
        print("speed:", lr.get("speed"))

if __name__ == "__main__":
    main()