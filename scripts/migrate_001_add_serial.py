from sqlalchemy import create_engine, text
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "forttis.db"
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

with engine.begin() as con:
    try:
        con.execute(text("ALTER TABLE devices ADD COLUMN serial_number VARCHAR"))
        print("OK: coluna 'serial_number' adicionada em devices.")
    except Exception as e:
        # Se já existir, o SQLite pode reclamar; tratamos como idempotente
        print("Aviso:", e)
        print("Prosseguindo (provavelmente a coluna já existe).")