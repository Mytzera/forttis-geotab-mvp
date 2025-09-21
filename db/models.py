from sqlalchemy import (
    Column, String, Float, DateTime, Integer, ForeignKey, create_engine
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from pathlib import Path

# ===== Base única =====
Base = declarative_base()

# ===== Models =====
class Device(Base):
    __tablename__ = "devices"
    id = Column(String, primary_key=True)
    name = Column(String)
    serial_number = Column(String, nullable=True)

class LogRecord(Base):
    __tablename__ = "log_records"
    # PK: usamos string estável "deviceId|dateTime" OU o id nativo, conforme seu ETL
    id = Column(String, primary_key=True)
    device_id = Column(String, ForeignKey("devices.id"), index=True, nullable=False)
    date_time = Column(DateTime(timezone=True), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    speed = Column(Float)

    device = relationship("Device", backref="log_records")

class ExceptionEvent(Base):
    __tablename__ = "exception_events"
    id = Column(String, primary_key=True)  # id do evento na Geotab
    device_id = Column(String, ForeignKey("devices.id"), index=True, nullable=False)
    rule_name = Column(String, index=True)     # ex.: Harsh Braking
    severity  = Column(String, index=True)     # ex.: High, Critical
    date_time = Column(DateTime(timezone=True), index=True)

    device = relationship("Device", backref="exception_events")

class SyncState(Base):
    __tablename__ = "sync_state"
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity = Column(String, unique=True, nullable=False)  # "LogRecord", "StatusData", etc.
    to_version = Column(String, nullable=True)

class OdometerSample(Base):
    __tablename__ = "odometer_samples"
    id = Column(String, primary_key=True)            # pk estável: f"{device_id}|{date_time_iso}"
    device_id = Column(String, ForeignKey("devices.id"), index=True, nullable=False)
    date_time = Column(DateTime(timezone=True), index=True, nullable=False)
    odometer_km = Column(Float, nullable=False)      # valor absoluto de hodômetro (km)

    device = relationship("Device", backref="odometer_samples")

# ===== Engine & Session =====
DB_PATH = Path(__file__).resolve().parents[1] / "forttis.db"
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_session():
    return SessionLocal()

def create_all():
    Base.metadata.create_all(engine)