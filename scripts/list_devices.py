from config import get_api
from dotenv import load_dotenv

def main():
    api = get_api()
    # lista os devices 
    devices = api.get("Device", resultsLimit=20)
    print(f"Total devices retornados: {len(devices)}")
    for d in devices:
        print(f"- name: {d.get('name')!r} | id: {d.get('id')} | serialNumber: {d.get('serialNumber')}")

if __name__ == "__main__":
    main()             