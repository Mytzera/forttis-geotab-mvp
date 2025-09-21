from config import get_api
import sys
api = get_api()
dev = api.get("Device", search={"id": sys.argv[1]})
print(dev[0]["name"] if dev else "não achei")