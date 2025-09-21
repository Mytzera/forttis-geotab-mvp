import os 
from pathlib import Path
from dotenv import load_dotenv, dotenv_values


ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
print("ENV_PATH:", ENV_PATH)

ok = load_dotenv(dotenv_path=ENV_PATH, override=True, encoding="utf-8")
raw = dotenv_values(ENV_PATH, encoding="utf-8")

print("load_dotenv OK?", ok)
print("dotenv_values (bruto):", raw)

print("USER:", os.getenv("tecnica1@forttis.com.br"))
print("PWD :", os.getenv("costa514300"))
print("DB  :", os.getenv("demo_demoforttis"))
print("SRV :", os.getenv("my23.geotab.com"))


