from datetime import date, datetime
from io import StringIO
import csv

from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func
from urllib.parse import quote

from ..database import get_db
from .. import models, crud
from ..models_production import ProductionTask  # <- NUEVO
from ..tx import smart_begin
from app.services.alerts_engine import generate_alerts

router = APIRouter(tags=["UI"])
templates = Jinja2Templates(directory="app/templates")


ALERT_RULES = {
    "temp_c": {"min": 26.0, "max": 30.0},
    "rh_pct": {"min": 55.0, "max": 70.0},
    "co2_ppm": {"min": 800.0, "max": 2000.0},
}


def compute_env_alerts(env: models.EnvReading | None) -> list[str]:
    if env is None:
        return ["Sin lectura ambiental registrada"]
    alerts = []
    if env.temp_c is not None:
        if env.temp_c < ALERT_RULES["temp_c"]["min"] or env.temp_c > ALERT_RULES["temp_c"]["max"]:
            alerts.append(f"Temperatura fuera de rango ({env.temp_c}°C)")
    if env.rh_pct is not None:
        if env.rh_pct < ALERT_RULES["rh_pct"]["min"] or env.rh_pct > ALERT_RULES["rh_pct"]["max"]:
            alerts.append(f"Humedad fuera de rango ({env.rh_pct}%)")
    if env.co2_ppm is not None:
        if env.co2_ppm < ALERT_RULES["co2_ppm"]["min"] or env.co2_ppm > ALERT_RULES["co2_ppm"]["max"]:
            alerts.append(f"CO₂ fuera de rango ({env.co2_ppm} ppm)")
    return alerts


def env_status(env_today_exists: bool, latest_env: models.EnvReading | None) -> str:
    if not env_today_exists:
        return "warn"
    alerts = compute_env_alerts(latest_env)
    if any(("fuera de rango" in a) for a in alerts):
        return "danger"
    return "ok"


@router.get("/ui", response_class=HTMLResponse)
def ui_home(request: Request, db: Session = Depends(get_db)):
    rooms = db.query(models.Room).order_by(models.Room.name).all()

    items_feed = db.query(models.Item).filter(models.Item.category == "feed").order_by(models.Item.name).all()
    frass_item = db.query(models.Item).filter(models.Item.category == "frass").order_by(models.Item.id).first()

    room_filter = request.query_params.get("room_id")
    q = (request.query_params.get("q") or "").strip().upper()

    pallets_q = db.query(models.Pallet).order_by(models.Pallet.code)
    if room_filter and room_filter.isdigit():
        pallets_q = pallets_q.filter(models.Pallet.room_id == int(room_filter))
    if q:
        pallets_q = pallets_q.filter(models.Pallet.code.like(f"%{q}%"))
    pallets = pallets_q.all()

    stock_feed = [{"item": it, "qty": crud.get_stock_qty(db, it.id)} for it in items_feed]
    frass_qty = crud.get_stock_qty(db, frass_item.id) if frass_item else 0.0

    today = date.today()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "rooms": rooms,
            "pallets": pallets,
            "items_feed": items_feed,
            "today": today.isoformat(),
            "room_filter": room_filter or "",
            "q": q,
            "stock_feed": stock_feed,
            "frass_qty": frass_qty,
        },
    )


@router.get("/ui/rooms", response_class=HTMLResponse)
def ui_rooms_board(request: Request, db: Session = Depends(get_db)):
    rooms = db.query(models.Room).order_by(models.Room.name).all()

    pallets = db.query(models.Pallet).order_by(models.Pallet.code).all()
    pallets_by_room: dict[int, list[models.Pallet]] = {}
    for p in pallets:
        pallets_by_room.setdefault(p.room_id, []).append(p)

    stats = {}
    for r in rooms:
        ps = pallets_by_room.get(r.id, [])
        stats[r.id] = {
            "total": len(ps),
            "active": sum(1 for x in ps if x.status == "active"),
            "cleaning": sum(1 for x in ps if x.status == "cleaning"),
            "quarantine": sum(1 for x in ps if x.status == "quarantine"),
            "disabled": sum(1 for x in ps if x.status == "disabled"),
            "empty": sum(1 for x in ps if x.status == "empty"),
        }

    today = date.today()

    env_today = db.query(models.EnvReading.room_id).filter(models.EnvReading.day == today).all()
    env_today_room_ids = {rid for (rid,) in env_today}

    latest_by_room = {}
    for r in rooms:
        latest = (
            db.query(models.EnvReading)
            .filter(models.EnvReading.room_id == r.id)
            .order_by(models.EnvReading.day.desc())
            .first()
        )
        latest_by_room[r.id] = latest

    room_env_status = {}
    room_env_alerts = {}
    for r in rooms:
        today_ok = (r.id in env_today_room_ids)
        latest = latest_by_room.get(r.id)
        room_env_status[r.id] = env_status(today_ok, latest)
        room_env_alerts[r.id] = compute_env_alerts(latest)

    items_feed = db.query(models.Item).filter(models.Item.category == "feed").order_by(models.Item.name).all()

    return templates.TemplateResponse(
        "rooms_board.html",
        {
            "request": request,
            "rooms": rooms,
            "pallets_by_room": pallets_by_room,
            "stats": stats,
            "items_feed": items_feed,
            "env_today_room_ids": env_today_room_ids,
            "today": today.isoformat(),
            "room_env_status": room_env_status,
            "room_env_alerts": room_env_alerts,
            "alert_rules": ALERT_RULES,
        },
    )


@router.post("/ui/rooms/batch")
def ui_rooms_batch_action(
    action: str = Form(...),
    pallet_ids: list[str] = Form([]),
    feed_item_id: int | None = Form(None),
    feed_mode: str = Form("per_tray"),
    feed_qty_per_tray_kg: float | None = Form(None),
    feed_qty_total_kg: float | None = Form(None),
    feed_note: str = Form(""),
    sieve_frass_kg: float | None = Form(None),
    sieve_residue_kg: float | None = Form(None),
    sieve_note: str = Form(""),
    move_to_room_id: int | None = Form(None),
    move_reason: str = Form(""),
    new_status: str = Form(""),
    db: Session = Depends(get_db),
):
    if not pallet_ids:
        return RedirectResponse(url="/ui/rooms?error=No has seleccionado pallets", status_code=303)

    pallets = db.query(models.Pallet).filter(models.Pallet.id.in_(pallet_ids)).all()
    if not pallets:
        return RedirectResponse(url="/ui/rooms?error=Pallets no encontrados", status_code=303)

    if action == "status":
        allowed = {"active", "cleaning", "quarantine", "disabled", "empty"}
        if new_status not in allowed:
            return RedirectResponse(url="/ui/rooms?error=Estado inválido", status_code=303)
        try:
            with smart_begin(db):
                for p in pallets:
                    p.status = new_status
            return RedirectResponse(url=f"/ui/rooms?ok=Estado actualizado en {len(pallets)} pallets", status_code=303)
        except Exception as e:
            return RedirectResponse(
                url="/ui/rooms?error=" + quote(f"Falló status batch: {type(e).__name__}: {e}"),
                status_code=303,
            )

    if action == "move":
        if not move_to_room_id:
            return RedirectResponse(url="/ui/rooms?error=Falta sala destino", status_code=303)
        to_room = db.query(models.Room).filter(models.Room.id == move_to_room_id).first()
        if not to_room:
            return RedirectResponse(url="/ui/rooms?error=Sala destino no encontrada", status_code=303)

        try:
            with smart_begin(db):
                for p in pallets:
                    if p.room_id == move_to_room_id:
                        continue
                    db.add(
                        models.PalletMove(
                            pallet_id=p.id,
                            from_room_id=p.room_id,
                            to_room_id=move_to_room_id,
                            reason=(move_reason or None),
                        )
                    )
                    p.room_id = move_to_room_id

            return RedirectResponse(url=f"/ui/rooms?ok=Movidos (o ya estaban) {len(pallets)} pallets", status_code=303)
        except Exception as e:
            return RedirectResponse(
                url="/ui/rooms?error=" + quote(f"Falló move batch: {type(e).__name__}: {e}"),
                status_code=303,
            )

    if action == "feed":
        if not feed_item_id:
            return RedirectResponse(url="/ui/rooms?error=Falta alimento", status_code=303)

        item = db.query(models.Item).filter(models.Item.id == feed_item_id).first()
        if not item or item.category != "feed":
            return RedirectResponse(url="/ui/rooms?error=Alimento inválido", status_code=303)

        totals = []
        for p in pallets:
            if p.status != "active":
                totals.append(0.0)
                continue
            if feed_mode == "per_tray":
                if feed_qty_per_tray_kg is None or feed_qty_per_tray_kg <= 0:
                    return RedirectResponse(url="/ui/rooms?error=Cantidad por bandeja inválida", status_code=303)
                totals.append(float(feed_qty_per_tray_kg) * int(p.tray_count))
            else:
                if feed_qty_total_kg is None or feed_qty_total_kg <= 0:
                    return RedirectResponse(url="/ui/rooms?error=Cantidad total inválida", status_code=303)
                totals.append(float(feed_qty_total_kg))

        total_to_discount = sum(totals)
        current = crud.get_stock_qty(db, feed_item_id)
        if current < total_to_discount:
            return RedirectResponse(
                url=f"/ui/rooms?error=Stock insuficiente ({current} kg) para total {total_to_discount} kg",
                status_code=303,
            )

        created = 0
        try:
            with smart_begin(db):
                for p, total in zip(pallets, totals):
                    if p.status != "active":
                        continue

                    ev = models.FeedEvent(
                        pallet_id=p.id,
                        item_id=feed_item_id,
                        qty_total_kg=total,
                        qty_per_tray_kg=(feed_qty_per_tray_kg if feed_mode == "per_tray" else None),
                        tray_count_used=int(p.tray_count),
                        note=(feed_note or None),
                    )
                    db.add(ev)
                    db.flush()  # asigna ev.id sin hacer commit

                    sm = models.StockMove(
                        item_id=feed_item_id,
                        move_type="out",
                        qty_kg=total,
                        ref_type="feed",
                        ref_id=str(ev.id),
                        note=f"Batch feed {p.code} (total {total} kg)",
                    )
                    crud.add_stock_move(db, sm, commit=False)
                    created += 1

            return RedirectResponse(url=f"/ui/rooms?ok=Alimentados {created} pallets (solo activos)", status_code=303)
        except Exception as e:
            return RedirectResponse(
                url="/ui/rooms?error=" + quote(f"Falló feed batch: {type(e).__name__}: {e}"),
                status_code=303,
            )

    if action == "sieve":
        if sieve_frass_kg is None or sieve_frass_kg <= 0:
            return RedirectResponse(url="/ui/rooms?error=Frass kg inválido", status_code=303)

        frass_item = db.query(models.Item).filter(models.Item.category == "frass").order_by(models.Item.id).first()
        if not frass_item:
            return RedirectResponse(url="/ui/rooms?error=No existe el item Frass", status_code=303)

        created = 0
        try:
            with smart_begin(db):
                for p in pallets:
                    ev = models.SieveEvent(
                        pallet_id=p.id,
                        frass_item_id=frass_item.id,
                        frass_kg=float(sieve_frass_kg),
                        residue_kg=float(sieve_residue_kg) if sieve_residue_kg is not None else None,
                        note=(sieve_note or None),
                    )
                    db.add(ev)
                    db.flush()

                    sm = models.StockMove(
                        item_id=frass_item.id,
                        move_type="in",
                        qty_kg=float(sieve_frass_kg),
                        ref_type="sieve",
                        ref_id=str(ev.id),
                        note=f"Batch frass from pallet {p.code}",
                    )
                    crud.add_stock_move(db, sm, commit=False)
                    created += 1

            return RedirectResponse(url=f"/ui/rooms?ok=Cribados {created} pallets", status_code=303)
        except Exception as e:
            return RedirectResponse(
                url="/ui/rooms?error=" + quote(f"Falló sieve batch: {type(e).__name__}: {e}"),
                status_code=303,
            )

    return RedirectResponse(url="/ui/rooms?error=Acción desconocida", status_code=303)


@router.get("/ui/room/{room_id}", response_class=HTMLResponse)
def ui_room_detail(room_id: int, request: Request, db: Session = Depends(get_db)):
    room = db.query(models.Room).filter(models.Room.id == room_id).first()
    if not room:
        return RedirectResponse(url="/ui/rooms?error=Sala no encontrada", status_code=303)

    pallets = db.query(models.Pallet).filter(models.Pallet.room_id == room_id).order_by(models.Pallet.code).all()

    today = date.today()
    env_today = (
        db.query(models.EnvReading)
        .filter(models.EnvReading.room_id == room_id, models.EnvReading.day == today)
        .first()
    )

    env_rows = (
        db.query(models.EnvReading)
        .filter(models.EnvReading.room_id == room_id)
        .order_by(models.EnvReading.day.desc())
        .limit(30)
        .all()
    )

    latest = env_rows[0] if env_rows else None
    alerts = compute_env_alerts(latest)
    status = env_status(env_today is not None, latest)

    return templates.TemplateResponse(
        "room.html",
        {
            "request": request,
            "room": room,
            "today": today.isoformat(),
            "env_today": env_today,
            "latest": latest,
            "env_rows": env_rows,
            "pallets": pallets,
            "alerts": alerts,
            "status": status,
            "alert_rules": ALERT_RULES,
        },
    )


@router.get("/ui/pallet/{pallet_id}", response_class=HTMLResponse)
def ui_pallet_detail(pallet_id: str, request: Request, db: Session = Depends(get_db)):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        return RedirectResponse(url="/ui?error=Pallet no encontrado", status_code=303)

    room = db.query(models.Room).filter(models.Room.id == pallet.room_id).first()
    batch = db.query(models.BatchMonth).filter(models.BatchMonth.id == pallet.batch_month_id).first()

    rooms = db.query(models.Room).order_by(models.Room.name).all()
    items_feed = db.query(models.Item).filter(models.Item.category == "feed").order_by(models.Item.name).all()

    moves = db.query(models.PalletMove).filter(models.PalletMove.pallet_id == pallet_id).order_by(
        models.PalletMove.moved_at.desc()
    ).limit(50).all()

    feeds = db.query(models.FeedEvent).filter(models.FeedEvent.pallet_id == pallet_id).order_by(
        models.FeedEvent.created_at.desc()
    ).limit(50).all()

    sieves = db.query(models.SieveEvent).filter(models.SieveEvent.pallet_id == pallet_id).order_by(
        models.SieveEvent.created_at.desc()
    ).limit(50).all()

    feed_summary = (
        db.query(models.Item.name, func.sum(models.FeedEvent.qty_total_kg))
        .join(models.Item, models.Item.id == models.FeedEvent.item_id)
        .filter(models.FeedEvent.pallet_id == pallet_id)
        .group_by(models.Item.name)
        .order_by(func.sum(models.FeedEvent.qty_total_kg).desc())
        .all()
    )

    today = date.today()
    tasks_recent = db.query(models.TaskInstance).filter(models.TaskInstance.pallet_id == pallet_id).order_by(
        models.TaskInstance.due_day.desc(), models.TaskInstance.id.desc()
    ).limit(60).all()

    tasks_upcoming = db.query(models.TaskInstance).filter(
        models.TaskInstance.pallet_id == pallet_id,
        models.TaskInstance.status != "done",
        models.TaskInstance.due_day >= today,
    ).order_by(models.TaskInstance.due_day.asc(), models.TaskInstance.id.asc()).limit(30).all()

    stock_feed = [{"item": it, "qty": crud.get_stock_qty(db, it.id)} for it in items_feed]

    # NUEVO: Registro PRO (ProductionTask) vinculado a este pallet
    pro_tasks = (
        db.query(ProductionTask)
        .filter(ProductionTask.pallet_id == pallet_id)
        .order_by(ProductionTask.day.desc(), ProductionTask.id.desc())
        .limit(80)
        .all()
    )

    return templates.TemplateResponse(
        "pallet.html",
        {
            "request": request,
            "today": today.isoformat(),
            "pallet": pallet,
            "room": room,
            "batch": batch,
            "rooms": rooms,
            "items_feed": items_feed,
            "stock_feed": stock_feed,
            "moves": moves,
            "feeds": feeds,
            "sieves": sieves,
            "feed_summary": feed_summary,
            "tasks_recent": tasks_recent,
            "tasks_upcoming": tasks_upcoming,
            "pro_tasks": pro_tasks,  # <- NUEVO
        },
    )


@router.get("/ui/pallet/{pallet_id}/export.csv")
def ui_pallet_export_csv(pallet_id: str, db: Session = Depends(get_db)):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        raise HTTPException(status_code=404, detail="Pallet not found")

    room = db.query(models.Room).filter(models.Room.id == pallet.room_id).first()
    batch = db.query(models.BatchMonth).filter(models.BatchMonth.id == pallet.batch_month_id).first()

    moves = db.query(models.PalletMove).filter(models.PalletMove.pallet_id == pallet_id).order_by(
        models.PalletMove.moved_at.asc()
    ).all()
    feeds = db.query(models.FeedEvent).filter(models.FeedEvent.pallet_id == pallet_id).order_by(
        models.FeedEvent.created_at.asc()
    ).all()
    sieves = db.query(models.SieveEvent).filter(models.SieveEvent.pallet_id == pallet_id).order_by(
        models.SieveEvent.created_at.asc()
    ).all()
    tasks = db.query(models.TaskInstance).filter(models.TaskInstance.pallet_id == pallet_id).order_by(
        models.TaskInstance.due_day.asc(), models.TaskInstance.id.asc()
    ).all()

    # NUEVO: export también del registro PRO
    pro_tasks = (
        db.query(ProductionTask)
        .filter(ProductionTask.pallet_id == pallet_id)
        .order_by(ProductionTask.day.asc(), ProductionTask.id.asc())
        .all()
    )

    sio = StringIO()
    w = csv.writer(sio)

    w.writerow(["PALLET"])
    w.writerow(["code", pallet.code])
    w.writerow(["status", pallet.status])
    w.writerow(["tray_count", pallet.tray_count])
    w.writerow(["room", room.name if room else pallet.room_id])
    w.writerow(["batch", batch.code if batch else ""])
    w.writerow(["created_at", pallet.created_at.isoformat() if pallet.created_at else ""])
    w.writerow([])

    w.writerow(["FEED_EVENTS"])
    w.writerow(["created_at", "item", "qty_per_tray_kg", "tray_count_used", "qty_total_kg", "note"])
    for f in feeds:
        w.writerow([
            f.created_at.isoformat() if f.created_at else "",
            f.item.name if f.item else f.item_id,
            f.qty_per_tray_kg if f.qty_per_tray_kg is not None else "",
            f.tray_count_used,
            f.qty_total_kg,
            f.note or "",
        ])
    w.writerow([])

    w.writerow(["SIEVE_EVENTS"])
    w.writerow(["created_at", "frass_kg", "residue_kg", "note"])
    for s in sieves:
        w.writerow([
            s.created_at.isoformat() if s.created_at else "",
            s.frass_kg,
            s.residue_kg if s.residue_kg is not None else "",
            s.note or "",
        ])
    w.writerow([])

    w.writerow(["PRODUCTION_TASKS"])
    w.writerow(["day", "task_name", "responsible", "minutes", "location", "feed1", "kg_per_tray1", "feed2", "kg_per_tray2",
                "frass_kg", "larvae_total_kg", "larvae_per_tray_kg", "note"])
    for pt in pro_tasks:
        w.writerow([
            pt.day.isoformat() if pt.day else "",
            pt.task_name,
            pt.responsible or "",
            pt.minutes if pt.minutes is not None else "",
            pt.location or "",
            pt.feed1_item.name if pt.feed1_item else "",
            pt.feed1_qty_per_tray_kg if pt.feed1_qty_per_tray_kg is not None else "",
            pt.feed2_item.name if pt.feed2_item else "",
            pt.feed2_qty_per_tray_kg if pt.feed2_qty_per_tray_kg is not None else "",
            pt.frass_kg if pt.frass_kg is not None else "",
            pt.larvae_total_kg if pt.larvae_total_kg is not None else "",
            pt.larvae_per_tray_kg if pt.larvae_per_tray_kg is not None else "",
            pt.note or "",
        ])
    w.writerow([])

    w.writerow(["PALLET_MOVES"])
    w.writerow(["moved_at", "from_room", "to_room", "reason"])
    for m in moves:
        w.writerow([
            m.moved_at.isoformat() if m.moved_at else "",
            m.from_room.name if m.from_room else "",
            m.to_room.name if m.to_room else "",
            m.reason or "",
        ])
    w.writerow([])

    w.writerow(["TASKS"])
    w.writerow(["due_day", "template_code", "template_name", "status", "note"])
    for t in tasks:
        w.writerow([
            t.due_day.isoformat() if t.due_day else "",
            t.template.code if t.template else "",
            t.template.name if t.template else "",
            t.status,
            t.note or "",
        ])

    sio.seek(0)
    filename = f"{pallet.code}_export.csv"
    return StreamingResponse(
        iter([sio.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/ui/rooms/create")
def ui_create_room(name: str = Form(...), db: Session = Depends(get_db)):
    name = name.strip()
    if not name:
        return RedirectResponse(url="/ui?error=Nombre de sala vacío", status_code=303)

    exists = db.query(models.Room).filter(models.Room.name == name).first()
    if exists:
        return RedirectResponse(url="/ui?error=Ya existe esa sala", status_code=303)

    db.add(models.Room(name=name))
    db.commit()
    return RedirectResponse(url="/ui?ok=Sala creada", status_code=303)


@router.post("/ui/pallets/create")
def ui_create_pallet(
    room_id: int = Form(...),
    code: str = Form(...),
    tray_count: int = Form(26),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    code = code.strip().upper()
    if not code:
        return RedirectResponse(url="/ui?error=Código de pallet vacío", status_code=303)
    if tray_count <= 0:
        return RedirectResponse(url="/ui?error=El número de bandejas debe ser > 0", status_code=303)

    room = db.query(models.Room).filter(models.Room.id == room_id).first()
    if not room:
        return RedirectResponse(url="/ui?error=Sala no encontrada", status_code=303)

    bm = crud.get_or_create_batch_month(db, date.today())

    dup = db.query(models.Pallet).filter(models.Pallet.code == code).first()
    if dup:
        return RedirectResponse(url="/ui?error=Ya existe ese código de pallet", status_code=303)

    p = models.Pallet(
        room_id=room_id,
        batch_month_id=bm.id,
        code=code,
        tray_count=tray_count,
        status="active",
        notes=(notes or None),
    )
    db.add(p)
    db.commit()
    return RedirectResponse(url="/ui?ok=Pallet creado", status_code=303)


@router.post("/ui/pallets/move")
def ui_move_pallet(
    pallet_id: str = Form(...),
    to_room_id: int = Form(...),
    reason: str = Form(""),
    db: Session = Depends(get_db),
):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        return RedirectResponse(url="/ui?error=Pallet no encontrado", status_code=303)

    to_room = db.query(models.Room).filter(models.Room.id == to_room_id).first()
    if not to_room:
        return RedirectResponse(url="/ui?error=Sala destino no encontrada", status_code=303)

    if pallet.room_id == to_room_id:
        return RedirectResponse(url="/ui?error=El pallet ya está en esa sala", status_code=303)

    move = models.PalletMove(
        pallet_id=pallet.id,
        from_room_id=pallet.room_id,
        to_room_id=to_room_id,
        reason=(reason or None),
    )
    try:
        with smart_begin(db):
            db.add(move)
            pallet.room_id = to_room_id
    except Exception as e:
        return RedirectResponse(
            url=f"/ui/pallet/{pallet.id}?error=" + quote(f"Falló mover pallet: {type(e).__name__}: {e}"),
            status_code=303,
        )

    return RedirectResponse(url=f"/ui/pallet/{pallet.id}?ok=Pallet movido de sala", status_code=303)


@router.post("/ui/pallets/status")
def ui_set_pallet_status(
    pallet_id: str = Form(...),
    status: str = Form(...),
    db: Session = Depends(get_db),
):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        return RedirectResponse(url="/ui?error=Pallet no encontrado", status_code=303)

    allowed = {"active", "cleaning", "quarantine", "disabled", "empty"}
    if status not in allowed:
        return RedirectResponse(url="/ui?error=Estado inválido", status_code=303)

    try:
        with smart_begin(db):
            pallet.status = status
    except Exception as e:
        return RedirectResponse(
            url=f"/ui/pallet/{pallet.id}?error=" + quote(f"Falló estado: {type(e).__name__}: {e}"),
            status_code=303,
        )
    return RedirectResponse(url=f"/ui/pallet/{pallet.id}?ok=Estado actualizado", status_code=303)


# ------------------- Priority 3: Cierre / Reapertura de ciclo -------------------


@router.post("/ui/pallet/{pallet_id}/close")
def ui_close_pallet(
    pallet_id: str,
    reason: str = Form(""),
    db: Session = Depends(get_db),
):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        return RedirectResponse(url="/ui?error=Pallet no encontrado", status_code=303)

    reason = (reason or "").strip()
    try:
        with smart_begin(db):
            pallet.is_closed = True
            pallet.closed_at = datetime.utcnow()
            pallet.closed_reason = reason[:200] if reason else None
            pallet.cycle_stage = "DONE"
    except Exception as e:
        return RedirectResponse(
            url=f"/ui/pallet/{pallet.id}?error=" + quote(f"Falló cierre: {type(e).__name__}: {e}"),
            status_code=303,
        )

    return RedirectResponse(url=f"/ui/pallet/{pallet.id}?ok=" + quote("Ciclo cerrado"), status_code=303)


@router.post("/ui/pallet/{pallet_id}/reopen")
def ui_reopen_pallet(
    pallet_id: str,
    db: Session = Depends(get_db),
):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == pallet_id).first()
    if not pallet:
        return RedirectResponse(url="/ui?error=Pallet no encontrado", status_code=303)

    try:
        with smart_begin(db):
            pallet.is_closed = False
            pallet.closed_at = None
            pallet.closed_reason = None
            pallet.cycle_stage = "ACTIVE"
    except Exception as e:
        return RedirectResponse(
            url=f"/ui/pallet/{pallet.id}?error=" + quote(f"Falló reapertura: {type(e).__name__}: {e}"),
            status_code=303,
        )

    return RedirectResponse(url=f"/ui/pallet/{pallet.id}?ok=" + quote("Ciclo reabierto"), status_code=303)


@router.get("/ui/stock", response_class=HTMLResponse)
def ui_stock(request: Request, db: Session = Depends(get_db)):
    items = db.query(models.Item).order_by(models.Item.category, models.Item.name).all()
    rows = [{"item": it, "qty": crud.get_stock_qty(db, it.id)} for it in items]
    recent_moves = db.query(models.StockMove).order_by(models.StockMove.created_at.desc()).limit(50).all()

    return templates.TemplateResponse(
        "stock.html",
        {"request": request, "rows": rows, "recent_moves": recent_moves, "today": date.today().isoformat()},
    )


@router.post("/ui/stock/purchase")
def ui_stock_purchase(
    item_id: int = Form(...),
    qty_kg: float = Form(...),
    ref_id: str = Form(""),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    item = db.query(models.Item).filter(models.Item.id == item_id).first()
    if not item:
        return RedirectResponse(url="/ui/stock?error=Item no encontrado", status_code=303)
    if qty_kg <= 0:
        return RedirectResponse(url="/ui/stock?error=Cantidad debe ser > 0", status_code=303)

    move = models.StockMove(
        item_id=item_id,
        move_type="in",
        qty_kg=qty_kg,
        ref_type="purchase",
        ref_id=(ref_id or None),
        note=(note or "Compra"),
    )
    crud.add_stock_move(db, move)
    return RedirectResponse(url="/ui/stock?ok=Compra registrada", status_code=303)


@router.post("/ui/stock/adjust")
def ui_stock_adjust(
    item_id: int = Form(...),
    qty_kg: float = Form(...),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    item = db.query(models.Item).filter(models.Item.id == item_id).first()
    if not item:
        return RedirectResponse(url="/ui/stock?error=Item no encontrado", status_code=303)
    if qty_kg == 0:
        return RedirectResponse(url="/ui/stock?error=El ajuste no puede ser 0", status_code=303)

    current = crud.get_stock_qty(db, item_id)
    if qty_kg < 0 and (current + qty_kg) < 0:
        return RedirectResponse(url=f"/ui/stock?error=Stock insuficiente (actual {current} kg)", status_code=303)

    move = models.StockMove(
        item_id=item_id,
        move_type="adjust",
        qty_kg=qty_kg,
        ref_type="adjust",
        ref_id=None,
        note=(note or "Ajuste"),
    )
    crud.add_stock_move(db, move)
    return RedirectResponse(url="/ui/stock?ok=Ajuste registrado", status_code=303)


@router.post("/ui/stock/thresholds")
def ui_stock_thresholds(
    item_id: int = Form(...),
    min_threshold: float = Form(0.0),
    critical_threshold: float = Form(0.0),
    db: Session = Depends(get_db),
):
    """Configura umbrales de avisos por item (Paso A2)."""
    item = db.query(models.Item).filter(models.Item.id == item_id).first()
    if not item:
        return RedirectResponse(url="/ui/stock?error=Item no encontrado", status_code=303)

    # Normaliza: crítico nunca debe ser mayor que mínimo (si ambos >0)
    if critical_threshold and min_threshold and critical_threshold > min_threshold:
        return RedirectResponse(
            url="/ui/stock?error=El umbral crítico no puede ser mayor que el mínimo",
            status_code=303,
        )

    item.min_threshold = float(min_threshold or 0.0)
    item.critical_threshold = float(critical_threshold or 0.0)
    db.commit()
    return RedirectResponse(url="/ui/stock?ok=Umbrales actualizados", status_code=303)


@router.post("/ui/environment")
def ui_environment(
    room_id: int = Form(...),
    day: str = Form(...),
    temp_c: float = Form(...),
    rh_pct: float = Form(...),
    co2_ppm: float = Form(...),
    source: str = Form("manual"),
    db: Session = Depends(get_db),
):
    y, m, d = day.split("-")
    day_date = date(int(y), int(m), int(d))

    reading = models.EnvReading(
        room_id=room_id,
        day=day_date,
        temp_c=temp_c,
        rh_pct=rh_pct,
        co2_ppm=co2_ppm,
        source=source,
    )
    db.add(reading)
    try:
        db.commit()
    except Exception:
        db.rollback()
        return RedirectResponse(url="/ui?error=Ya existe registro ambiental para esa sala y día", status_code=303)

    return RedirectResponse(url="/ui?ok=Ambiente registrado", status_code=303)


@router.get("/ui/tasks", response_class=HTMLResponse)
def ui_tasks(request: Request, db: Session = Depends(get_db)):
    today = date.today()
    tasks = (
        db.query(models.TaskInstance)
        .filter(models.TaskInstance.due_day == today)
        .order_by(models.TaskInstance.status.asc(), models.TaskInstance.id.asc())
        .all()
    )
    return templates.TemplateResponse(
        "tasks.html",
        {"request": request, "tasks": tasks, "today": today.isoformat()},
    )