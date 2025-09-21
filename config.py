import os
from pathlib import Path
import mygeotab

# 1) tenta .env (se existir)
def _load_from_env():
    try:
        from dotenv import load_dotenv
        ENV_PATH = Path(__file__).resolve().with_name(".env")
        load_dotenv(dotenv_path=ENV_PATH, override=True, encoding="utf-8")
    except Exception:
        pass
    user   = (os.getenv("tecnica1@forttis.com.br") or "").strip()
    pwd    = (os.getenv("costa514300") or "").strip()
    db     = (os.getenv("demo_demoforttis") or "").strip()
    server = (os.getenv("my23.geotab.com") or "").strip()
    return user, pwd, db, server

# 2) tenta config_local.py (que você acabou de criar)
def _load_from_local():
    try:
        import config_local as cl  # arquivo local, não versionado
        user   = getattr(cl, "USER", "").strip()
        pwd    = getattr(cl, "PWD", "").strip()
        db     = getattr(cl, "DB", "").strip()
        server = getattr(cl, "SERVER", "").strip()
        return user, pwd, db, server
    except Exception:
        return "", "", "", ""

# 3) se ainda faltar algo, pergunta no terminal
def _load_interactive(user, pwd, db, server):
    if user and pwd and (db or True):  # db pode ser vazio
        return user, pwd, db, server
    try:
        import getpass
        if not user:
            user = input("MYGEOTAB_USERNAME: ").strip()
        if not pwd:
            pwd = getpass.getpass("MYGEOTAB_PASSWORD: ").strip()
        # DB pode ser vazio se seu usuário tem 1 banco só
        if db is None:
            db = ""
        if not server:
            server = input("MYGEOTAB_SERVER (ex.: my.geotab.com ou my23.geotab.com) [opcional]: ").strip()
    except Exception:
        pass
    return user, pwd, db, server

def _load_creds():
    u1, p1, d1, s1 = _load_from_env()
    if not (u1 and p1):
        u2, p2, d2, s2 = _load_from_local()
    else:
        u2, p2, d2, s2 = "", "", "", ""
    user   = (u1 or u2).strip()
    pwd    = (p1 or p2).strip()
    db     = (d1 or d2).strip() or None
    server = (s1 or s2).strip() or None
    # último fallback: interativo
    user, pwd, db, server = _load_interactive(user, pwd, db, server)
    return user, pwd, db, server

def get_api():
    user, pwd, db, server = _load_creds()

    # diagnóstico mínimo (sem senha)
    # print(f"[config] USER={user} DB={db} SERVER={server}")

    if not (user and pwd):
        raise ValueError("Credenciais ausentes. Defina no .env ou config_local.py, ou informe interativamente.")
    api = mygeotab.API(username=user, password=pwd, database=db, server=server)
    api.authenticate()
    return api