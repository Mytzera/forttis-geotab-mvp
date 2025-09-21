from datetime import datetime
from sqlalchemy.exc import IntegrityError
from db.models import get_session, Device, LogRecord, ExceptionEvent
from config import get_api
from db.models import get_session, Device, LogRecord, ExceptionEvent, OdometerSample
from config import get_api

def _upsert_devices(session, api, device_ids: set[str]) -> None:
    """Garante que os devices existem na tabela 'devices' (busca 1 a 1 para evitar erros)."""
    if not device_ids:
        return
    for did in device_ids:
        if not did:
            continue
        try:
            res = api.get("Device", search={"id": did})
        except Exception:
            continue
        if not res:
            continue
        d = res[0]
        obj = session.get(Device, d["id"]) or Device(id=d["id"])
        obj.name = d.get("name")
        # Se você tiver a coluna no banco:
        if hasattr(obj, "serial_number"):
            obj.serial_number = d.get("serialNumber")
        session.merge(obj)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()

def _parse_dt(dtval):
    if isinstance(dtval, datetime):
        return dtval
    if isinstance(dtval, str):
        return datetime.fromisoformat(dtval.replace("Z", "+00:00"))
    return None

def save_logrecords(items: list[dict]) -> int:
    """
    Salva uma lista de LogRecord vindos da API.
    - Garante devices na tabela 'devices'
    - Normaliza datetime/lat/lon/speed
    - Usa PK estável: f"{device_id}|{date_time_iso}"
    Retorna quantidade de registros gravados/atualizados.
    """
    s = get_session()


def _parse_dt_iso(dtval):
    from datetime import datetime
    if isinstance(dtval, datetime):
        return dtval
    if isinstance(dtval, str):
        return datetime.fromisoformat(dtval.replace("Z", "+00:00"))
    return None

def save_odometer_samples(items: list[dict]) -> int:
    """
    Salva samples de odômetro (em km) a partir de StatusData.
    Espera itens com campos: device, dateTime, data (numérico), diagnostic (opcional).
    """
    s = get_session()
    # garantir devices
    dev_ids = set()
    for r in items:
        dev = r.get("device")
        dev_ids.add(dev.get("id") if isinstance(dev, dict) else dev)
    dev_ids.discard(None)
    if dev_ids:
        _upsert_devices(s, get_api(), dev_ids)

    saved = 0
    for raw in items:
        dev = raw.get("device")
        device_id = dev.get("id") if isinstance(dev, dict) else dev
        if not device_id:
            continue
        dt = _parse_dt_iso(raw.get("dateTime") or raw.get("DateTime"))
        if not dt:
            continue

        # Valor do odômetro: muitos tenants trazem "data" como metros ou km.
        # Estratégia: tentamos converter para float e:
        #   - se valor muito grande (ex. >1e6), pode estar em metros -> km = v/1000
        #   - senão, assume km
        v = raw.get("data")
        try:
            v = float(v)
        except Exception:
            continue

        if v > 1_000_000:
            odo_km = v / 1000.0
        else:
            odo_km = v

        pk = f"{device_id}|{dt.isoformat()}"
        s.merge(OdometerSample(id=pk, device_id=device_id, date_time=dt, odometer_km=odo_km))
        saved += 1

    s.commit()
    return saved    

    # 1) garantir devices
    dev_ids = set()
    for r in items:
        dev = r.get("device")
        dev_ids.add(dev.get("id") if isinstance(dev, dict) else dev)
    dev_ids.discard(None)
    if dev_ids:
        _upsert_devices(s, get_api(), dev_ids)

    # 2) salvar logrecords
    saved = 0
    for raw in items:
        dev = raw.get("device")
        device_id = dev.get("id") if isinstance(dev, dict) else dev
        if not device_id:
            continue

        dt_utc = _parse_dt(raw.get("dateTime") or raw.get("DateTime"))
        if not dt_utc:
            continue

        lat = raw.get("latitude") or raw.get("Latitude")
        lon = raw.get("longitude") or raw.get("Longitude")
        spd = raw.get("speed") or raw.get("Speed")
        try:
            lat = float(lat) if lat is not None else None
            lon = float(lon) if lon is not None else None
            spd = float(spd) if spd is not None else None
        except Exception:
            continue

        # PK estável por device_id+date_time
        pk = f"{device_id}|{dt_utc.isoformat()}"

        rec = LogRecord(
            id=pk,
            device_id=device_id,
            date_time=dt_utc,
            latitude=lat,
            longitude=lon,
            speed=spd,
        )
        s.merge(rec)
        saved += 1

    s.commit()
    return saved