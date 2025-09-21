from db.models import create_all

if __name__ == "__main__":
    create_all()
    print("Sqlite pronto (tabelas criadas).")
    