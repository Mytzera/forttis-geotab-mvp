# scripts/save_exceptions.py
from config import get_api
from etl.pipeline import save_exception_events
from datetime import datetime, timedelta, timezone

if __name__ == "__main__":
    api = get_api()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    items = api.get("ExceptionEvent", search={"fromDate": start.isoformat(), "toDate": now.isoformat()}, resultsLimit=5000)
    print("ExceptionEvent (API):", len(items))
    saved = save_exception_events(items)
    print("Salvos (após filtro):", saved)