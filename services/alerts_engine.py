from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from sqlalchemy.orm import Session

from app import models
from app import crud

@dataclass
class AlertSpec:
    code: str
    severity: str  # "info" | "warn" | "critical"
    title: str
    message: str
    room_id: int | None = None
    pallet_id: int | None = None
    item_id: int | None = None

def _upsert_alert(db: Session, spec: AlertSpec) -> models.Alert:
    # Alert único por code (no duplicar)
    a = db.query(models.Alert).filter(models.Alert.code == spec.code).first()
    if a is None:
        a = models.Alert(code=spec.code)
        db.add(a)

    a.severity = spec.severity
    a.title = spec.title
    a.message = spec.message
    a.room_id = spec.room_id
    a.pallet_id = spec.pallet_id
    a.item_id = spec.item_id
    a.is_resolved = False
    return a

def generate_alerts(db: Session) -> dict:
    created_or_updated = 0
    resolved = 0

    # --- 1) Stock bajo/crítico ---
    items = db.query(models.Item).all()
    for it in items:
        qty = crud.get_stock_qty(db, it.id)  # neto por movimientos
        min_th = getattr(it, "min_threshold", None) or 0.0
        crit_th = getattr(it, "critical_threshold", None) or 0.0

        # Si no hay umbrales definidos, saltamos (o pon global)
        if min_th <= 0 and crit_th <= 0:
            continue

        if crit_th > 0 and qty <= crit_th:
            spec = AlertSpec(
                code=f"STOCK_CRIT_{it.id}",
                severity="critical",
                title=f"Stock CRÍTICO: {it.name}",
                message=f"Stock actual {qty:.3f} {it.unit}. Umbral crítico {crit_th:.3f}.",
                item_id=it.id,
            )
            _upsert_alert(db, spec)
            created_or_updated += 1
        elif min_th > 0 and qty <= min_th:
            spec = AlertSpec(
                code=f"STOCK_LOW_{it.id}",
                severity="warn",
                title=f"Stock bajo: {it.name}",
                message=f"Stock actual {qty:.3f} {it.unit}. Umbral mínimo {min_th:.3f}.",
                item_id=it.id,
            )
            _upsert_alert(db, spec)
            created_or_updated += 1
        else:
            # si había alertas previas de ese item, las resolvemos
            for code in (f"STOCK_CRIT_{it.id}", f"STOCK_LOW_{it.id}"):
                a = db.query(models.Alert).filter(models.Alert.code == code, models.Alert.is_resolved == False).first()
                if a:
                    a.is_resolved = True
                    resolved += 1

    # --- 2) Ambiente fuera de rango (por sala) ---
    # Usamos el último EnvironmentReading por sala (si existe)
    rooms = db.query(models.Room).all()
    for r in rooms:
        last = (
            db.query(models.EnvironmentReading)
            .filter(models.EnvironmentReading.room_id == r.id)
            .order_by(models.EnvironmentReading.created_at.desc())
            .first()
        )
        if not last:
            continue

        # Temp
        if r.target_temp_min is not None and last.temp_c is not None and last.temp_c < r.target_temp_min:
            _upsert_alert(db, AlertSpec(
                code=f"ENV_TEMP_LOW_{r.id}",
                severity="warn",
                title=f"Temperatura baja en {r.name}",
                message=f"{last.temp_c:.1f}°C < mínimo {r.target_temp_min:.1f}°C",
                room_id=r.id
            ))
            created_or_updated += 1
        else:
            a = db.query(models.Alert).filter(models.Alert.code == f"ENV_TEMP_LOW_{r.id}", models.Alert.is_resolved == False).first()
            if a: a.is_resolved = True; resolved += 1

        if r.target_temp_max is not None and last.temp_c is not None and last.temp_c > r.target_temp_max:
            _upsert_alert(db, AlertSpec(
                code=f"ENV_TEMP_HIGH_{r.id}",
                severity="warn",
                title=f"Temperatura alta en {r.name}",
                message=f"{last.temp_c:.1f}°C > máximo {r.target_temp_max:.1f}°C",
                room_id=r.id
            ))
            created_or_updated += 1
        else:
            a = db.query(models.Alert).filter(models.Alert.code == f"ENV_TEMP_HIGH_{r.id}", models.Alert.is_resolved == False).first()
            if a: a.is_resolved = True; resolved += 1

        # RH (humedad)
        if r.target_rh_min is not None and last.rh_pct is not None and last.rh_pct < r.target_rh_min:
            _upsert_alert(db, AlertSpec(
                code=f"ENV_RH_LOW_{r.id}",
                severity="warn",
                title=f"Humedad baja en {r.name}",
                message=f"{last.rh_pct:.1f}% < mínimo {r.target_rh_min:.1f}%",
                room_id=r.id
            ))
            created_or_updated += 1
        else:
            a = db.query(models.Alert).filter(models.Alert.code == f"ENV_RH_LOW_{r.id}", models.Alert.is_resolved == False).first()
            if a: a.is_resolved = True; resolved += 1

        if r.target_rh_max is not None and last.rh_pct is not None and last.rh_pct > r.target_rh_max:
            _upsert_alert(db, AlertSpec(
                code=f"ENV_RH_HIGH_{r.id}",
                severity="warn",
                title=f"Humedad alta en {r.name}",
                message=f"{last.rh_pct:.1f}% > máximo {r.target_rh_max:.1f}%",
                room_id=r.id
            ))
            created_or_updated += 1
        else:
            a = db.query(models.Alert).filter(models.Alert.code == f"ENV_RH_HIGH_{r.id}", models.Alert.is_resolved == False).first()
            if a: a.is_resolved = True; resolved += 1

    db.commit()
    return {"alerts_upserted": created_or_updated, "alerts_resolved": resolved}