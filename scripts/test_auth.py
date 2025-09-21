from config import get_api

if__name__ == "__main__":
    try:
    api = get_api()
devices = api.get("Device", resultsLimit=1)
print("OK. Autenticado. Exemplo de device:", devices[0]["name"] if devices lse "nenhum")
