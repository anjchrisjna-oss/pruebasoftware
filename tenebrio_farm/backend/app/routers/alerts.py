from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..database import get_db
from .. import models, crud
from ..models_production import ProductionTask

router = APIRouter(tags=["UI"])
templates = Jinja2Templates(directory="app/templates")


# --- Semáforo (ajústalo cuando quieras)
PRO_RED_DAYS = 10
PRO_YELLOW_DAYS = 5

STOCK_RED_KG = 0.0      # < 0 => rojo siempre; 0 exacto => amarillo (sin stock)
STOCK_YELLOW_KG = 1.0   # <= 1kg => amarillo

ENV_RED_IF_OUT_OF_RANGE = True  # si fuera de rango => rojo
ENV_YELLOW_IF_MISSING = True    # sin lectura => amarillo


def _badge(level: str) -> str:
    level = (level or "").upper()
    if level in ("RED", "ROJO"):
        return "RED"
    if level in ("YELLOW", "AMARILLO"):
        return "YELLOW"
    if level in ("GREEN", "VERDE"):
        return "GREEN"
    return "GRAY"


def _order(level: str) -> int:
    return {"RED": 0, "YELLOW": 1, "GREEN": 2, "GRAY": 9}.get(_badge(level), 9)


@router.get("/ui/alerts", response_class=HTMLResponse)
def ui_alerts(request: Request, db: Session = Depends(get_db)):
    now = datetime.utcnow()
    alerts: list[dict[str, Any]] = []

    # -------------------------
    # 1) AVISOS por SALA (ENV)
    # -------------------------
    # última lectura por sala
    last_day_sq = (
        db.query(models.EnvReading.room_id, func.max(models.EnvReading.day).label("max_day"))
        .group_by(models.EnvReading.room_id)
        .subquery()
    )
    last_env = (
        db.query(models.EnvReading)
        .join(last_day_sq, (models.EnvReading.room_id == last_day_sq.c.room_id) & (models.EnvReading.day == last_day_sq.c.max_day))
        .all()
    )
    last_env_map = {e.room_id: e for e in last_env}

    rooms = db.query(models.Room).order_by(models.Room.name.asc()).all()
    for r in rooms:
        env = last_env_map.get(r.id)
        env_alerts = []
        if env is None and ENV_YELLOW_IF_MISSING:
            env_alerts = ["Sin lectura ambiental registrada"]
            level = "YELLOW"
        else:
            # usa reglas ya existentes en ui.py si están (fallback aquí)
            level = "GREEN"
            if env is not None:
                # target ranges: si no están, no avisamos por ese campo
                if r.target_temp_min is not None and r.target_temp_max is not None and env.temp_c is not None:
                    if env.temp_c < r.target_temp_min or env.temp_c > r.target_temp_max:
                        env_alerts.append(f"Temperatura fuera de rango ({env.temp_c}°C)")
                if r.target_rh_min is not None and r.target_rh_max is not None and env.rh_pct is not None:
                    if env.rh_pct < r.target_rh_min or env.rh_pct > r.target_rh_max:
                        env_alerts.append(f"Humedad fuera de rango ({env.rh_pct}%)")
                if r.target_co2_min is not None and r.target_co2_max is not None and env.co2_ppm is not None:
                    if env.co2_ppm < r.target_co2_min or env.co2_ppm > r.target_co2_max:
                        env_alerts.append(f"CO₂ fuera de rango ({env.co2_ppm} ppm)")
            if env_alerts:
                level = "RED" if ENV_RED_IF_OUT_OF_RANGE else "YELLOW"

        if env_alerts:
            alerts.append({
                "level": _badge(level),
                "scope": "SALA",
                "code": r.name,
                "message": " · ".join(env_alerts),
                "link": "/ui/rooms",
            })

    # -------------------------
    # 2) AVISOS por PALET (PRO + ciclo)
    # -------------------------
    last_pro_sq = (
        db.query(ProductionTask.pallet_id, func.max(ProductionTask.created_at).label("max_dt"))
        .group_by(ProductionTask.pallet_id)
        .subquery()
    )
    last_pro_rows = (
        db.query(ProductionTask.pallet_id, ProductionTask.created_at)
        .join(last_pro_sq, (ProductionTask.pallet_id == last_pro_sq.c.pallet_id) & (ProductionTask.created_at == last_pro_sq.c.max_dt))
        .all()
    )
    last_pro_map = {pid: dt for pid, dt in last_pro_rows}

    pallets = db.query(models.Pallet).order_by(models.Pallet.code.asc()).all()
    for p in pallets:
        # si está cerrado, normalmente no queremos “rojos” por falta de PRO
        if getattr(p, "is_closed", False):
            alerts.append({
                "level": "GREEN",
                "scope": "PALET",
                "code": p.code,
                "message": "Ciclo cerrado",
                "link": f"/ui/pallets/{p.id}",
            })
            continue

        dt = last_pro_map.get(p.id)
        if dt is None:
            level = "YELLOW"
            msg = "Sin PRO registrada aún"
        else:
            days = (now - dt).days
            if days >= PRO_RED_DAYS:
                level = "RED"
                msg = f"Sin PRO hace {days} días"
            elif days >= PRO_YELLOW_DAYS:
                level = "YELLOW"
                msg = f"Sin PRO hace {days} días"
            else:
                level = "GREEN"
                msg = f"OK (última PRO hace {days} días)"

        if level != "GREEN":
            alerts.append({
                "level": _badge(level),
                "scope": "PALET",
                "code": p.code,
                "message": msg,
                "link": f"/ui/pallets/{p.id}",
            })

    # -------------------------
    # 3) AVISOS de STOCK
    # -------------------------
    items = db.query(models.Item).order_by(models.Item.category.asc(), models.Item.name.asc()).all()
    for it in items:
        qty = crud.get_stock_qty(db, it.id)

        if qty < 0:
            level = "RED"
            msg = f"Stock NEGATIVO: {qty:.3f} {it.unit}"
        elif qty <= STOCK_RED_KG:
            level = "YELLOW"
            msg = f"Sin stock: {qty:.3f} {it.unit}"
        elif qty <= STOCK_YELLOW_KG:
            level = "YELLOW"
            msg = f"Stock bajo: {qty:.3f} {it.unit}"
        else:
            level = "GREEN"
            msg = f"OK: {qty:.3f} {it.unit}"

        if level != "GREEN":
            alerts.append({
                "level": _badge(level),
                "scope": "STOCK",
                "code": it.name,
                "message": msg,
                "link": "/ui/stock",
            })

    alerts.sort(key=lambda a: (_order(a["level"]), a["scope"], a["code"]))

    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "alerts": alerts,
            "now": now,
            "rules": {
                "PRO_YELLOW_DAYS": PRO_YELLOW_DAYS,
                "PRO_RED_DAYS": PRO_RED_DAYS,
                "STOCK_YELLOW_KG": STOCK_YELLOW_KG,
            },
        },
    )
