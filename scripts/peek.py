from db.models import get_session, LogRecord, Device
from sqlalchemy import func

from sqlalchemy import select, func
from db.models import get_session, LogRecord, Device

def main():
    s = get_session()

    # total geral de linhas em LogRecord
    total = s.execute(
        select(func.count()).select_from(LogRecord)
    ).scalar_one()
    print(f"Total de LogRecords no banco: {total}")

    # por device: device_id, contagem, primeiro e último ponto
    stmt = (
        select(
            LogRecord.device_id,
            func.count().label("n"),
            func.min(LogRecord.date_time).label("first_dt"),
            func.max(LogRecord.date_time).label("last_dt"),
        )
        .group_by(LogRecord.device_id)
        .order_by(func.count().desc())
    )

    print("\nPor device:")
    for device_id, n, first_dt, last_dt in s.execute(stmt):
        dev = s.get(Device, device_id)  # busca por PK
        name = dev.name if dev else str(device_id)
        print(f"- {name} ({device_id}) -> {n} pts | {first_dt} → {last_dt}")

if __name__ == "__main__":
    main()
    