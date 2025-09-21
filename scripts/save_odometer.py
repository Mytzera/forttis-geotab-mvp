from datetime import datetime, timedelta, timezone
from config import get_api
from etl.pipeline import save_odometer_samples

def pick_odometer_diagnostic(api):
    """
    Tenta encontrar um diagnóstico de odômetro.
    Preferências por nome contendo 'Odometer' (case-insensitive).
    Fallback: o primeiro diagnóstico que tenha 'Odometer' no nome.
    """
    diags = api.get("Diagnostic", search={})
    # ordena para escolher estável:
    diags = sorted(diags, key=lambda d: d.get("name",""))
    for d in diags:
        name = (d.get("name") or "").lower()
        if "odometer" in name:
            return d
    # fallback simples: None
    return None

if __name__ == "__main__":
    api = get_api()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=90)

    diag = pick_odometer_diagnostic(api)
    if not diag:
        print("Não encontrei diagnóstico de odômetro; ajuste a função pick_odometer_diagnostic.")
        raise SystemExit(1)

    print("Usando diagnóstico:", diag.get("name"), diag.get("id"))
    items = api.get("StatusData", search={
        "diagnosticSearch": {"id": diag.get("id")},
        "fromDate": start.isoformat(),
        "toDate": now.isoformat(),
    }, resultsLimit=5000)

    print("StatusData (odômetro) recebido:", len(items))
    saved = save_odometer_samples(items)
    print("OdometerSample salvos:", saved)
    print("OK.")