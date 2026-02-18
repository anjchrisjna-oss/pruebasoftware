from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..database import get_db
from .. import models
from ..models_production import ProductionTask

router = APIRouter(tags=["History UI"])
templates = Jinja2Templates(directory="app/templates")


def _parse_dt(dt: Any) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt
    try:
        return datetime.fromisoformat(str(dt))
    except Exception:
        return None


def _fmt_date(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.strftime("%d/%m/%Y")


def _fmt_time(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.strftime("%H:%M")


@router.get("/ui/history", response_class=HTMLResponse)
def ui_history(request: Request, db: Session = Depends(get_db)):
    """
    Historial global estilo Excel basado en ProductionTask (PRO),
    usando el esquema ACTUAL de tu production.py:
      day, task_name, responsible, minutes, location, feed*_*, frass_kg, larvae_*, note
    """
    # Últimas 700 tareas (ajusta si quieres)
    tasks = (
        db.query(ProductionTask)
        .order_by(ProductionTask.day.desc(), ProductionTask.id.desc())
        .limit(700)
        .all()
    )

    # Mapas rápidos
    pallets = {p.id: p.code for p in db.query(models.Pallet).all()}

    rows = []
    for t in tasks:
        created = _parse_dt(getattr(t, "created_at", None))
        day = getattr(t, "day", None)
        fecha = ""
        if day:
            try:
                # day puede ser date
                fecha = day.strftime("%d/%m/%Y")
            except Exception:
                fecha = str(day)

        rows.append({
            "fecha": fecha,
            "hora": _fmt_time(created),
            "tipo": getattr(t, "task_name", "") or "",
            "pallet_id": getattr(t, "pallet_id", ""),
            "pallet_code": pallets.get(getattr(t, "pallet_id", ""), getattr(t, "pallet_id", "")),
            "responsable": getattr(t, "responsible", "") or "",
            "tiempo": getattr(t, "minutes", None),

            "alimento1": getattr(getattr(t, "feed1_item", None), "name", "") if hasattr(t, "feed1_item") else "",
            "cant1": getattr(t, "feed1_qty_per_tray_kg", None),

            "alimento2": getattr(getattr(t, "feed2_item", None), "name", "") if hasattr(t, "feed2_item") else "",
            "cant2": getattr(t, "feed2_qty_per_tray_kg", None),

            "frass": getattr(t, "frass_kg", None),
            "new_loc": getattr(t, "location", "") or "",
            "anot": getattr(t, "note", "") or "",

            "peso_total": getattr(t, "larvae_total_kg", None),
            "peso_bandeja": getattr(t, "larvae_per_tray_kg", None),
        })

    return templates.TemplateResponse(
        "history.html",
        {"request": request, "rows": rows, "title": "Historial (PRO)"},
    )


@router.get("/ui/pallet/{pallet_id}/history", response_class=HTMLResponse)
def ui_pallet_history(pallet_id: str, request: Request, db: Session = Depends(get_db)):
    pallet = db.query(models.Pallet).get(pallet_id)
    if not pallet:
        return templates.TemplateResponse(
            "history.html",
            {"request": request, "rows": [], "title": f"Historial palet (no existe): {pallet_id}"},
        )

    tasks = (
        db.query(ProductionTask)
        .filter(ProductionTask.pallet_id == pallet_id)
        .order_by(ProductionTask.day.desc(), ProductionTask.id.desc())
        .limit(400)
        .all()
    )

    rows = []
    for t in tasks:
        created = _parse_dt(getattr(t, "created_at", None))
        day = getattr(t, "day", None)
        fecha = ""
        if day:
            try:
                fecha = day.strftime("%d/%m/%Y")
            except Exception:
                fecha = str(day)

        rows.append({
            "fecha": fecha,
            "hora": _fmt_time(created),
            "tipo": getattr(t, "task_name", "") or "",
            "pallet_id": pallet_id,
            "pallet_code": pallet.code,
            "responsable": getattr(t, "responsible", "") or "",
            "tiempo": getattr(t, "minutes", None),

            "alimento1": getattr(getattr(t, "feed1_item", None), "name", "") if hasattr(t, "feed1_item") else "",
            "cant1": getattr(t, "feed1_qty_per_tray_kg", None),

            "alimento2": getattr(getattr(t, "feed2_item", None), "name", "") if hasattr(t, "feed2_item") else "",
            "cant2": getattr(t, "feed2_qty_per_tray_kg", None),

            "frass": getattr(t, "frass_kg", None),
            "new_loc": getattr(t, "location", "") or "",
            "anot": getattr(t, "note", "") or "",

            "peso_total": getattr(t, "larvae_total_kg", None),
            "peso_bandeja": getattr(t, "larvae_per_tray_kg", None),
        })

    return templates.TemplateResponse(
        "history.html",
        {"request": request, "rows": rows, "title": f"Historial palet: {pallet.code}"},
    )