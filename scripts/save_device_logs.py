from config import get_api
from etl.pipeline import save_logrecords
from datetime import datetime, timedelta, timezone
from db.models import get_session, LogRecord
import sys

def main():
    if len(sys.argv) < 2:
        print("Uso: python -m scripts.save_device_logs <DEVICE_ID> [dias]")
        raise SystemExit(1)

    device_id = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) >= 3 else 30

    api = get_api()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    # busca crua na API
    items = api.get(
        "LogRecord",
        search={"device": device_id, "fromDate": start.isoformat(), "toDate": now.isoformat()},
        resultsLimit=5000,   # aumente se quiser mais
    )
    print(f"API retornou {len(items)} pontos; salvando no SQLite...")

    # persiste
    save_logrecords(items)

    # resumo do que ficou
    s = get_session()
    total = s.query(LogRecord).filter(LogRecord.device_id == device_id).count()
    first = s.query(LogRecord).filter(LogRecord.device_id == device_id).order_by(LogRecord.date_time.asc()).first()
    last  = s.query(LogRecord).filter(LogRecord.device_id == device_id).order_by(LogRecord.date_time.desc()).first()

    print(f"📦 No banco: {total} pontos do device {device_id}.")
    if first and last:
        print("⏱️  intervalo UTC:", first.date_time, "→", last.date_time)

if __name__ == "__main__":
    main()