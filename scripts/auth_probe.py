import mygeotab

USERNAME = "tecnica1@forttis.com.br"
PASSWORD = "costa514300"
DATABASE = "demo_demoforttis"
SERVER = "my23.geotab.com"


api = mygeotab.API(username=USERNAME, password=PASSWORD, database=DATABASE, server=SERVER)
api.authenticate()
print("Autenticou")
devs = api.get("Device", resultsLimit=1)
print("Exemplo de Device:", devs[0]["name"] if devs else "nenhum")

from datetime import datetime, timedelta, timezone

# Janela de busca: Ultimas 2h 

now = datetime.now(timezone.utc)
items = api.get("LogRecord", search={
    "fromDate": (now - timedelta(hours=2)).isoformat(),
    "toDate": now.isoformat()
})
print("LogRecord (Ultimas 2h):", len(items))

# Mostrar 1 exemplo rsumido (se existir)
if items:
    lr = items[0]
    # Pegar nome do device (se não veio no objeto)
    dev_id = lr.get("device", {}).get("id") if isinstance(lr.get("device"), dict) else lr.get("device")
    dev_name = None
    if dev_id:
        dev = api.get("Device", search={"id": dev_id})
        if dev:
            dev_name = dev[0].get("name")


    resumo = {
        "device": dev_name or dev_id,
        "dateTime": lr.get("dateTime"),
        "lat": lr.get("latitude"),
        "lon": lr.get("longitude"),
        "speed": lr.get("speed"),
    }
    print("Exemplo de LogRecord:", resumo)
else:
    print("Nenhum LogRecord encontrado nesse período - tente ampliar a janela.")


