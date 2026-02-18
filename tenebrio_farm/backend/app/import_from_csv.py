from __future__ import annotations

import csv
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from .database import SessionLocal, Base, engine
from . import models
from . import models_production  # noqa: F401 (register tables)
from .models_production import ProductionTask


def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    # Accept YYYY-MM-DD or DD/MM/YYYY
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Accept ISO or DD/MM/YYYY
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            # If only date, interpret as midnight
            return dt
        except Exception:
            pass
    return None


def _to_float(s: str) -> Optional[float]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _get_or_create_room(db: Session, name: str) -> models.Room:
    name = (name or "").strip()
    if not name:
        name = "SIN_SALA"
    room = db.query(models.Room).filter(models.Room.name == name).first()
    if room:
        return room
    room = models.Room(name=name)
    db.add(room)
    db.flush()
    return room


def _get_or_create_item(db: Session, name: str, category: str = "feed", unit: str = "kg") -> models.Item:
    name = (name or "").strip()
    if not name:
        name = "UNKNOWN"
    item = db.query(models.Item).filter(models.Item.name == name).first()
    if item:
        return item
    item = models.Item(name=name, category=category, unit=unit)
    db.add(item)
    db.flush()
    return item


def import_maestro_pallets(db: Session, csv_path: str | Path) -> int:
    """Importa MAESTRO_PALETS.csv.

    Idempotente por código de palet: si existe, actualiza campos.
    """
    csv_path = Path(csv_path)
    created = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            code = (row.get("ID Palet") or "").strip().upper()
            if not code:
                continue

            room_name = row.get("Aula Actual") or ""
            room = _get_or_create_room(db, room_name)

            created_at = _parse_dt(row.get("Fecha Creación") or "")
            status = (row.get("Estado Actual") or "active").strip().lower()
            origin_lot = (row.get("Lote Origen") or "").strip() or None
            tray_count = _to_int(row.get("Num. Bandejas") or "") or 26
            kg_per_tray = _to_float(row.get("Kg por Bandeja") or "")
            parent_lot = (row.get("Lote Padre") or "").strip() or None
            extraction_count = _to_int(row.get("Nº Extracción") or "") or 0
            logistic_status = (row.get("Estado Logístico") or "").strip() or None
            notes = None

            p = db.query(models.Pallet).filter(models.Pallet.code == code).first()
            if not p:
                # batch month from created_at or today
                day = (created_at.date() if created_at else date.today())
                bm = models.BatchMonth(code=f"{day.year:04d}-{day.month:02d}", start_date=day.replace(day=1), end_date=day.replace(day=28))
                # use crud helper if available
                try:
                    from . import crud
                    bm = crud.get_or_create_batch_month(db, day)
                except Exception:
                    db.add(bm); db.flush()
                p = models.Pallet(
                    code=code,
                    room_id=room.id,
                    batch_month_id=bm.id,
                    tray_count=tray_count,
                    status=status if status else "active",
                    notes=notes,
                )
                if created_at:
                    p.created_at = created_at
                db.add(p)
                db.flush()
                created += 1

            # update fields
            p.room_id = room.id
            if created_at:
                p.created_at = created_at
            if status:
                p.status = status
            p.origin_lot = origin_lot
            p.parent_lot = parent_lot
            p.tray_count = tray_count
            p.kg_per_tray = kg_per_tray
            p.extraction_count = extraction_count
            p.logistic_status = logistic_status

    return created


def import_registro_tareas(db: Session, csv_path: str | Path) -> int:
    """Importa REGISTRO_TAREAS.csv a production_tasks.

    Idempotente aproximado: no duplica si ya existe una fila igual (day, pallet, task_name, minutes, note).
    """
    csv_path = Path(csv_path)
    created = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            day = _parse_date(row.get("Fecha") or "")
            if not day:
                continue

            code = (row.get("ID Palet") or "").strip().upper()
            if not code:
                continue

            pallet = db.query(models.Pallet).filter(models.Pallet.code == code).first()
            if not pallet:
                # create minimal pallet in unknown room
                room = _get_or_create_room(db, "SIN_SALA")
                try:
                    from . import crud
                    bm = crud.get_or_create_batch_month(db, day)
                except Exception:
                    bm = models.BatchMonth(code=f"{day.year:04d}-{day.month:02d}", start_date=day.replace(day=1), end_date=day.replace(day=28))
                    db.add(bm); db.flush()
                pallet = models.Pallet(code=code, room_id=room.id, batch_month_id=bm.id)
                db.add(pallet); db.flush()

            task_name = (row.get("Tipo Tarea") or "").strip() or "PRO"
            responsible = (row.get("Responsable") or "").strip() or None
            minutes = _to_float(row.get("Tiempo") or "")
            note = (row.get("Anotaciones") or "").strip() or None
            location = (row.get("Nueva Ubicación") or "").strip() or None

            # Feed / frass / larvae
            feed1_name = (row.get("Alimento 1") or "").strip()
            feed1_qty = _to_float(row.get("Cant 1 Kg/bandeja") or "")
            feed2_name = (row.get("Alimento 2") or "").strip()
            feed2_qty = _to_float(row.get("Cant 2 Kg/bandeja") or "")
            frass_kg = _to_float(row.get("Frass Kg total") or "")
            larvae_total = _to_float(row.get("Peso Total Larva (Kg)") or "")
            larvae_per_tray = _to_float(row.get("Peso por Bandeja (Kg)") or "")

            feed1_item_id = None
            feed2_item_id = None
            if feed1_name:
                feed1_item_id = _get_or_create_item(db, feed1_name, category="feed").id
            if feed2_name:
                feed2_item_id = _get_or_create_item(db, feed2_name, category="feed").id

            # idempotency check
            exists = (
                db.query(ProductionTask)
                .filter(
                    ProductionTask.day == day,
                    ProductionTask.pallet_id == pallet.id,
                    ProductionTask.task_name == task_name,
                    ProductionTask.minutes.is_(minutes) if minutes is None else ProductionTask.minutes == minutes,
                    ProductionTask.note.is_(note) if note is None else ProductionTask.note == note,
                )
                .first()
            )
            if exists:
                continue

            pt = ProductionTask(
                day=day,
                pallet_id=pallet.id,
                room_id=pallet.room_id,
                task_name=task_name,
                responsible=responsible,
                minutes=minutes,
                location=location,
                feed1_item_id=feed1_item_id,
                feed1_qty_per_tray_kg=feed1_qty,
                feed2_item_id=feed2_item_id,
                feed2_qty_per_tray_kg=feed2_qty,
                frass_kg=frass_kg,
                larvae_total_kg=larvae_total,
                larvae_per_tray_kg=larvae_per_tray,
                note=note,
            )
            db.add(pt)
            created += 1

    return created


def import_inventario_as_snapshot(db: Session, csv_path: str | Path) -> int:
    """Importa INVENTARIO.csv creando items y dejando StockMove 'adjust' para fijar Stock Actual.

    OJO: si lo ejecutas varias veces, no duplica si ya existe un adjust con ref_type='import_snapshot' y ref_id=item_id.
    """
    csv_path = Path(csv_path)
    created = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            name = (row.get("Producto") or "").strip()
            if not name:
                continue

            stock_actual = _to_float(row.get("Stock Actual") or "")
            if stock_actual is None:
                continue

            # category from config-like sheets is not present; default feed
            item = _get_or_create_item(db, name, category="feed", unit="kg")

            # if we already imported snapshot, skip
            exists = (
                db.query(models.StockMove)
                .filter(models.StockMove.ref_type == "import_snapshot", models.StockMove.ref_id == str(item.id))
                .first()
            )
            if exists:
                continue

            # set current stock by adjusting to desired value
            current = 0.0
            try:
                from . import crud
                current = crud.get_stock_qty(db, item.id)
            except Exception:
                pass
            delta = float(stock_actual) - float(current)

            if abs(delta) < 1e-9:
                continue

            mv = models.StockMove(
                item_id=item.id,
                move_type="adjust",
                qty_kg=delta,
                ref_type="import_snapshot",
                ref_id=str(item.id),
                note="Import INVENTARIO snapshot",
            )
            db.add(mv)
            created += 1

    return created


def run_all(
    maestro_pallets: str | None = None,
    registro_tareas: str | None = None,
    inventario: str | None = None,
) -> dict:
    # Ensure all tables exist (SQLite doesn't auto-migrate; create_all is safe here)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        res = {}
        if maestro_pallets:
            res["maestro_pallets_created"] = import_maestro_pallets(db, maestro_pallets)
        if registro_tareas:
            res["registro_tareas_created"] = import_registro_tareas(db, registro_tareas)
        if inventario:
            res["inventario_moves_created"] = import_inventario_as_snapshot(db, inventario)

        db.commit()
        return res


if __name__ == "__main__":
    # Example:
    # python -m app.import_from_csv --maestro "MAESTRO_PALETS.csv" --tareas "REGISTRO_TAREAS.csv" --inventario "INVENTARIO.csv"
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--maestro", dest="maestro", default=None)
    p.add_argument("--tareas", dest="tareas", default=None)
    p.add_argument("--inventario", dest="inventario", default=None)
    args = p.parse_args()

    out = run_all(args.maestro, args.tareas, args.inventario)
    print(out)
