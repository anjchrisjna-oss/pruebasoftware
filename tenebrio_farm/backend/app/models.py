import uuid
from datetime import datetime, date
from sqlalchemy import (
    String, Integer, Float, DateTime, Date, ForeignKey, UniqueConstraint, Boolean
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .database import Base


def uuid_str() -> str:
    return str(uuid.uuid4())


class Room(Base):
    __tablename__ = "rooms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)

    target_temp_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_temp_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_rh_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_rh_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_co2_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_co2_max: Mapped[float | None] = mapped_column(Float, nullable=True)

    pallets = relationship("Pallet", back_populates="room", cascade="all,delete")
    env_readings = relationship("EnvReading", back_populates="room", cascade="all,delete")


class BatchMonth(Base):
    __tablename__ = "batch_months"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(7), unique=True, index=True)  # YYYY-MM
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)


class Pallet(Base):
    __tablename__ = "pallets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    code: Mapped[str] = mapped_column(String(20), unique=True, index=True)  # PAL-000123

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    # active / cleaning / quarantine / disabled / empty
    status: Mapped[str] = mapped_column(String(20), default="active", index=True)

    # Nº de bandejas (todas iguales dentro del palet)
    tray_count: Mapped[int] = mapped_column(Integer, default=26)

    # --- Datos de trazabilidad (CSV MAESTRO_PALETS / HISTORICO_LOTES)
    origin_lot: Mapped[str | None] = mapped_column(String(60), nullable=True, index=True)  # Lote Origen
    parent_lot: Mapped[str | None] = mapped_column(String(60), nullable=True, index=True)  # Lote Padre (texto)
    kg_per_tray: Mapped[float | None] = mapped_column(Float, nullable=True)  # Kg por Bandeja (estimado/medido)
    extraction_count: Mapped[int] = mapped_column(Integer, default=0)  # Nº Extracción acumulada
    logistic_status: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)  # Estado Logístico

    notes: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # ---- Cierre de ciclo (Priority 3)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    closed_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    # ACTIVE / EGGS / LARVAE / PUPA / DONE (puedes ajustar más adelante)
    cycle_stage: Mapped[str] = mapped_column(String(30), default="ACTIVE", index=True)

    room_id: Mapped[int] = mapped_column(ForeignKey("rooms.id"))
    batch_month_id: Mapped[int] = mapped_column(ForeignKey("batch_months.id"))

    room = relationship("Room", back_populates="pallets")
    batch_month = relationship("BatchMonth")

    moves = relationship("PalletMove", back_populates="pallet", cascade="all,delete")
    feed_events = relationship("FeedEvent", back_populates="pallet", cascade="all,delete")
    sieve_events = relationship("SieveEvent", back_populates="pallet", cascade="all,delete")

    # Registro PRO (ProductionTask)
    production_tasks = relationship("ProductionTask", back_populates="pallet", cascade="all,delete")


class PalletMove(Base):
    __tablename__ = "pallet_moves"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pallet_id: Mapped[str] = mapped_column(ForeignKey("pallets.id"), index=True)

    from_room_id: Mapped[int | None] = mapped_column(ForeignKey("rooms.id"), nullable=True)
    to_room_id: Mapped[int] = mapped_column(ForeignKey("rooms.id"))

    moved_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    pallet = relationship("Pallet", back_populates="moves")
    from_room = relationship("Room", foreign_keys=[from_room_id])
    to_room = relationship("Room", foreign_keys=[to_room_id])


class EnvReading(Base):
    __tablename__ = "env_readings"
    __table_args__ = (UniqueConstraint("room_id", "day", name="uq_env_room_day"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    room_id: Mapped[int] = mapped_column(ForeignKey("rooms.id"), index=True)
    day: Mapped[date] = mapped_column(Date, index=True)

    temp_c: Mapped[float] = mapped_column(Float)
    rh_pct: Mapped[float] = mapped_column(Float)
    co2_ppm: Mapped[float] = mapped_column(Float)

    source: Mapped[str] = mapped_column(String(20), default="manual")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    room = relationship("Room", back_populates="env_readings")


class Item(Base):
    """
    Items include:
    - Feed items (alimentos)
    - Frass item (producto frass)
    """
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(String(20), index=True)  # feed / frass / other
    name: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    unit: Mapped[str] = mapped_column(String(10), default="kg")


class StockMove(Base):
    """
    Stock changes always happen via movements:
    - in: purchase, sieve
    - out: feed, sale
    - adjust: correction
    """
    __tablename__ = "stock_moves"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    item_id: Mapped[int] = mapped_column(ForeignKey("items.id"), index=True)
    move_type: Mapped[str] = mapped_column(String(10), index=True)  # in/out/adjust
    qty_kg: Mapped[float] = mapped_column(Float)

    ref_type: Mapped[str] = mapped_column(String(30), index=True)  # purchase/feed/sieve/adjust
    ref_id: Mapped[str | None] = mapped_column(String(50), nullable=True)

    note: Mapped[str | None] = mapped_column(String(255), nullable=True)

    item = relationship("Item")


class FeedEvent(Base):
    """
    Alimentación:
    - Puedes registrar por bandeja o total, pero siempre guardamos:
      qty_total_kg = lo que descuenta stock
      qty_per_tray_kg = si se registró por bandeja (opcional)
      tray_count_used = bandejas usadas en el cálculo
    """
    __tablename__ = "feed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    pallet_id: Mapped[str] = mapped_column(ForeignKey("pallets.id"), index=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("items.id"), index=True)

    qty_total_kg: Mapped[float] = mapped_column(Float)
    qty_per_tray_kg: Mapped[float | None] = mapped_column(Float, nullable=True)
    tray_count_used: Mapped[int] = mapped_column(Integer, default=0)

    note: Mapped[str | None] = mapped_column(String(255), nullable=True)

    pallet = relationship("Pallet", back_populates="feed_events")
    item = relationship("Item")


class SieveEvent(Base):
    """
    CRIBAR:
    - generates frass into stock
    """
    __tablename__ = "sieve_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    pallet_id: Mapped[str] = mapped_column(ForeignKey("pallets.id"), index=True)
    frass_item_id: Mapped[int] = mapped_column(ForeignKey("items.id"), index=True)

    frass_kg: Mapped[float] = mapped_column(Float)
    residue_kg: Mapped[float | None] = mapped_column(Float, nullable=True)
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)

    pallet = relationship("Pallet", back_populates="sieve_events")
    frass_item = relationship("Item")


class TaskTemplate(Base):
    __tablename__ = "task_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(80))
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)


class TaskInstance(Base):
    __tablename__ = "task_instances"
    __table_args__ = (
        UniqueConstraint("task_template_id", "pallet_id", "due_day", name="uq_task_pallet_day"),
        UniqueConstraint("task_template_id", "room_id", "due_day", name="uq_task_room_day"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_template_id: Mapped[int] = mapped_column(ForeignKey("task_templates.id"), index=True)

    due_day: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)

    pallet_id: Mapped[str | None] = mapped_column(ForeignKey("pallets.id"), nullable=True, index=True)
    room_id: Mapped[int | None] = mapped_column(ForeignKey("rooms.id"), nullable=True, index=True)

    note: Mapped[str | None] = mapped_column(String(255), nullable=True)

    template = relationship("TaskTemplate")
    pallet = relationship("Pallet")
    room = relationship("Room")