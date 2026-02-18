from datetime import date as dt_date

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from contextlib import contextmanager
from sqlalchemy.exc import InvalidRequestError

from ..database import get_db
from .. import models, crud
from ..models_production import ProductionTask

router = APIRouter(tags=["Production UI"])
templates = Jinja2Templates(directory="app/templates")

@contextmanager
def smart_begin(db):
    """
    Abre una transacción normal si no existe, o un SAVEPOINT (nested)
    si ya hay una transacción abierta en la Session.
    """
    if db.in_transaction():
        with db.begin_nested():
            yield
    else:
        with db.begin():
            yield

def _to_int_or_none(v: str | None) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _to_float_or_none(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if not s:
        return None
    try:
        f = float(s)
        return f
    except Exception:
        return None


@router.get("/ui/production", response_class=HTMLResponse)
def ui_production_home(request: Request, db: Session = Depends(get_db)):
    rooms = db.query(models.Room).order_by(models.Room.name).all()

    items_feed = (
        db.query(models.Item)
        .filter(models.Item.category == "feed")
        .order_by(models.Item.name)
        .all()
    )

    room_filter = request.query_params.get("room_id") or ""
    q = (request.query_params.get("q") or "").strip().upper()

    pallets_q = db.query(models.Pallet).order_by(models.Pallet.code)
    if room_filter.isdigit():
        pallets_q = pallets_q.filter(models.Pallet.room_id == int(room_filter))
    if q:
        pallets_q = pallets_q.filter(models.Pallet.code.like(f"%{q}%"))
    pallets = pallets_q.all()

    today = dt_date.today()

    return templates.TemplateResponse(
        "production.html",
        {
            "request": request,
            "rooms": rooms,
            "items_feed": items_feed,
            "pallets": pallets,
            "today": today.isoformat(),
            "room_filter": room_filter,
            "q": q,
        },
    )


@router.post("/ui/production/record")
def ui_production_record(
    # cabecera
    day: str = Form(...),
    task_name: str = Form(...),
    responsible: str = Form(""),
    minutes: str = Form("0"),
    location: str = Form(""),
    note: str = Form(""),

    # alimentos (opcionales) - vienen como string (puede ser "")
    feed1_item_id: str = Form(""),
    feed1_qty_per_tray_kg: str = Form(""),
    feed2_item_id: str = Form(""),
    feed2_qty_per_tray_kg: str = Form(""),

    # criba/limpieza (opcionales)
    frass_kg: str = Form(""),
    larvae_total_kg: str = Form(""),

    # palets seleccionados
    pallet_ids: list[str] = Form([]),

    db: Session = Depends(get_db),
):
    if not pallet_ids:
        return RedirectResponse(url="/ui/production?error=No has seleccionado palets", status_code=303)

    task_name = (task_name or "").strip()
    if not task_name:
        return RedirectResponse(url="/ui/production?error=Falta el nombre de la tarea", status_code=303)

    # parse day
    y, m, d = day.split("-")
    day_date = dt_date(int(y), int(m), int(d))

    # parse numbers safely
    minutes_f = _to_float_or_none(minutes)
    feed1_id = _to_int_or_none(feed1_item_id)
    feed2_id = _to_int_or_none(feed2_item_id)
    feed1_qty = _to_float_or_none(feed1_qty_per_tray_kg)
    feed2_qty = _to_float_or_none(feed2_qty_per_tray_kg)
    frass_f = _to_float_or_none(frass_kg)
    larvae_total_f = _to_float_or_none(larvae_total_kg)

    pallets = db.query(models.Pallet).filter(models.Pallet.id.in_(pallet_ids)).all()
    if not pallets:
        return RedirectResponse(url="/ui/production?error=Palets no encontrados", status_code=303)

    # Validar alimentos (si hay item, debe haber cantidad > 0)
    def valid_feed(item_id, qty):
        if item_id is None:
            return True
        return qty is not None and qty > 0

    if not valid_feed(feed1_id, feed1_qty):
        return RedirectResponse(url="/ui/production?error=Alimento 1: falta kg/bandeja o es 0", status_code=303)
    if not valid_feed(feed2_id, feed2_qty):
        return RedirectResponse(url="/ui/production?error=Alimento 2: falta kg/bandeja o es 0", status_code=303)

    # Validar que items sean de tipo feed
    items_by_id = {}
    ids_to_check = [x for x in [feed1_id, feed2_id] if x is not None]
    if ids_to_check:
        for it in db.query(models.Item).filter(models.Item.id.in_(ids_to_check)).all():
            items_by_id[it.id] = it
        for iid in ids_to_check:
            it = items_by_id.get(iid)
            if not it or it.category != "feed":
                return RedirectResponse(url="/ui/production?error=Uno de los alimentos no es válido", status_code=303)

    # Frass item (para stock in) si se registra frass
    frass_item = None
    if frass_f is not None and frass_f > 0:
        frass_item = (
            db.query(models.Item)
            .filter(models.Item.category == "frass")
            .order_by(models.Item.id)
            .first()
        )
        if not frass_item:
            return RedirectResponse(url="/ui/production?error=No existe el item Frass", status_code=303)

    # ---------
    # Pre-cálculo de stock necesario (batch)
    # ---------
    total_out = {}  # item_id -> total kg
    for p in pallets:
        trays = int(p.tray_count)

        if feed1_id and feed1_qty:
            total = float(feed1_qty) * trays
            total_out[feed1_id] = total_out.get(feed1_id, 0.0) + total

        if feed2_id and feed2_qty:
            total = float(feed2_qty) * trays
            total_out[feed2_id] = total_out.get(feed2_id, 0.0) + total

    # comprobar stock antes (para no dejar a medias)
    for iid, needed in total_out.items():
        current = crud.get_stock_qty(db, iid)
        if current < needed:
            item_name = items_by_id[iid].name if iid in items_by_id else f"Item {iid}"
            return RedirectResponse(
                url=f"/ui/production?error=Stock insuficiente de {item_name}: {current} kg (necesario {needed} kg)",
                status_code=303,
            )

    # ---------
    # Crear registros por palet (transacción única)
    # ---------
    created = 0
    try:
        with smart_begin(db):
            for p in pallets:
                trays = int(p.tray_count)
                larvae_per_tray = None
                if larvae_total_f is not None and larvae_total_f > 0:
                    larvae_per_tray = float(larvae_total_f) / trays if trays > 0 else None

                pt = ProductionTask(
                    day=day_date,
                    task_name=task_name,
                    pallet_id=p.id,
                    room_id=p.room_id,
                    responsible=(responsible.strip() or None),
                    minutes=(float(minutes_f) if minutes_f and minutes_f > 0 else None),
                    location=(location.strip() or None),
                    feed1_item_id=feed1_id,
                    feed1_qty_per_tray_kg=(float(feed1_qty) if feed1_id else None),
                    feed2_item_id=feed2_id,
                    feed2_qty_per_tray_kg=(float(feed2_qty) if feed2_id else None),
                    frass_kg=(float(frass_f) if frass_f and frass_f > 0 else None),
                    larvae_total_kg=(float(larvae_total_f) if larvae_total_f and larvae_total_f > 0 else None),
                    larvae_per_tray_kg=larvae_per_tray,
                    note=(note.strip() or None),
                )
                db.add(pt)
                db.flush()  # obtener pt.id

                note_full = f"[PRO:{pt.id}] {task_name}" + (f" | {note.strip()}" if note.strip() else "")

                # --- Alimentación: guardar también como FeedEvent + StockMove (out)
                if feed1_id and feed1_qty:
                    total1 = float(feed1_qty) * trays
                    ev1 = models.FeedEvent(
                        pallet_id=p.id,
                        item_id=feed1_id,
                        qty_total_kg=total1,
                        qty_per_tray_kg=float(feed1_qty),
                        tray_count_used=trays,
                        note=note_full,
                    )
                    db.add(ev1)
                    db.flush()  # obtener ev1.id
                    crud.add_stock_move(
                        db,
                        models.StockMove(
                            item_id=feed1_id,
                            move_type="out",
                            qty_kg=total1,
                            ref_type="feed",
                            ref_id=str(ev1.id),
                            note=f"[PRO:{pt.id}] {p.code} - {task_name}",
                        ),
                        commit=False,
                    )

                if feed2_id and feed2_qty:
                    total2 = float(feed2_qty) * trays
                    ev2 = models.FeedEvent(
                        pallet_id=p.id,
                        item_id=feed2_id,
                        qty_total_kg=total2,
                        qty_per_tray_kg=float(feed2_qty),
                        tray_count_used=trays,
                        note=note_full,
                    )
                    db.add(ev2)
                    db.flush()
                    crud.add_stock_move(
                        db,
                        models.StockMove(
                            item_id=feed2_id,
                            move_type="out",
                            qty_kg=total2,
                            ref_type="feed",
                            ref_id=str(ev2.id),
                            note=f"[PRO:{pt.id}] {p.code} - {task_name}",
                        ),
                        commit=False,
                    )

                # --- Frass: guardar también como SieveEvent + StockMove (in)
                if frass_item and frass_f and frass_f > 0:
                    sev = models.SieveEvent(
                        pallet_id=p.id,
                        frass_item_id=frass_item.id,
                        frass_kg=float(frass_f),
                        residue_kg=None,
                        note=note_full,
                    )
                    db.add(sev)
                    db.flush()
                    crud.add_stock_move(
                        db,
                        models.StockMove(
                            item_id=frass_item.id,
                            move_type="in",
                            qty_kg=float(frass_f),
                            ref_type="sieve",
                            ref_id=str(sev.id),
                            note=f"[PRO:{pt.id}] Frass from {p.code} - {task_name}",
                        ),
                        commit=False,
                    )

                created += 1
    except Exception as e:
        # IMPORTANTE: con db.begin() ya se hace rollback automático,
        # pero lo dejamos explícito por seguridad.
        try:
            db.rollback()
        except Exception:
            pass

        import traceback
        from urllib.parse import quote

        tb = traceback.format_exc()
        print("\n=== ERROR GUARDANDO PRO ===\n")
        print(tb)
        print("\n==========================\n")

        # Metemos un resumen corto del error en la URL (sin romper por caracteres raros)
        err_short = f"{type(e).__name__}: {str(e)}"
        err_short = err_short[:180]  # límite razonable
        return RedirectResponse(
            url="/ui/production?error=" + quote("PRO falló: " + err_short),
            status_code=303,
        )

    return RedirectResponse(url=f"/ui/production?ok=Registradas {created} tareas PRO", status_code=303)