from __future__ import annotations

import uuid
from datetime import datetime, date

from sqlalchemy import String, Integer, Float, DateTime, Date, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


class ProductionTask(Base):
    """Registro de producción unificado (PRO).

    Diseñado para reflejar el CSV/Excel **REGISTRO_TAREAS**:

      - Fecha -> day
      - Tipo Tarea -> task_name
      - ID Palet -> pallet_id
      - Responsable -> responsible
      - Tiempo -> minutes
      - Alimento 1 / Cant 1 Kg/bandeja -> feed1_item_id / feed1_qty_per_tray_kg
      - Alimento 2 / Cant 2 Kg/bandeja -> feed2_item_id / feed2_qty_per_tray_kg
      - Frass Kg total -> frass_kg
      - Nueva Ubicación -> location (texto) y opcionalmente room_id (FK sala actual)
      - Anotaciones -> note
      - Peso Total Larva (Kg) -> larvae_total_kg
      - Peso por Bandeja (Kg) -> larvae_per_tray_kg

    Nota:
      - El stock se descuenta/entra mediante StockMove al registrar en /ui/production/record.
      - room_id guarda la sala del palet en el momento del registro (para informes).
    """

    __tablename__ = "production_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    # Fecha (día de trabajo)
    day: Mapped[date] = mapped_column(Date, index=True)

    # Palet y sala
    pallet_id: Mapped[str] = mapped_column(ForeignKey("pallets.id"), index=True)
    room_id: Mapped[int | None] = mapped_column(ForeignKey("rooms.id"), nullable=True, index=True)

    # Tipo/Nombre de tarea
    task_name: Mapped[str] = mapped_column(String(80), index=True)

    # Responsable / tiempo invertido (minutos)
    responsible: Mapped[str | None] = mapped_column(String(60), nullable=True)
    minutes: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Campo libre de ubicación (por ejemplo: "Aula 2", "Zona limpieza"...)
    location: Mapped[str | None] = mapped_column(String(80), nullable=True)

    # Alimentos (FK a items)
    feed1_item_id: Mapped[int | None] = mapped_column(ForeignKey("items.id"), nullable=True)
    feed1_qty_per_tray_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    feed2_item_id: Mapped[int | None] = mapped_column(ForeignKey("items.id"), nullable=True)
    feed2_qty_per_tray_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Frass total (kg)
    frass_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Pesos de larva
    larvae_total_kg: Mapped[float | None] = mapped_column(Float, nullable=True)
    larvae_per_tray_kg: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Anotaciones
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Relaciones
    pallet = relationship("Pallet", back_populates="production_tasks")
    room = relationship("Room")
    feed1_item = relationship("Item", foreign_keys=[feed1_item_id])
    feed2_item = relationship("Item", foreign_keys=[feed2_item_id])
