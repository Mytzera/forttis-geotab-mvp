from config import get_api
from collections import Counter

def main():
    api = get_api()
    feed = api.get_feed("LogRecord", results_limit=500)  # últimos registros disponíveis
    items = feed.get("data") or []
    print("Itens no feed:", len(items))

    dev_ids = []
    for lr in items[:50]:  # mostra até 50 exemplos
        dev = lr.get("device")
        dev_id = dev["id"] if isinstance(dev, dict) else dev
        dev_ids.append(dev_id)
        print(
            lr.get("dateTime"),
            "| dev:", dev_id,
            "| lat/lon:", lr.get("latitude"), lr.get("longitude"),
            "| speed:", lr.get("speed")
        )

    print("\nTop devices no feed recente:")
    for did, cnt in Counter(dev_ids).most_common():
        print(did, cnt)

if __name__ == "__main__":
    main()