from datetime import date
from sqlalchemy.orm import Session
from . import models, crud


def _next_pallet_code(db: Session) -> str:
    last = db.query(models.Pallet).order_by(models.Pallet.code.desc()).first()
    if not last or not last.code.startswith("PAL-"):
        return "PAL-000001"
    try:
        n = int(last.code.split("-")[1])
    except Exception:
        return "PAL-000001"
    return f"PAL-{n+1:06d}"


def seed_minimum(db: Session):
    frass = db.query(models.Item).filter(models.Item.name == "Frass").first()
    if not frass:
        db.add(models.Item(category="frass", name="Frass", unit="kg"))

    defaults = [
        ("FEED", "Alimentar", "Registrar alimentación del palet (descuenta stock)."),
        ("SIEVE", "Cribar", "Cribar el palet: separa frass y lo entra en stock."),
        ("ENV", "Registrar ambiente", "Registrar T/HR/CO2 diarios por sala."),
        ("CLEAN", "Limpieza", "Limpieza de sala/palet."),
    ]
    for code, name, desc in defaults:
        exists = db.query(models.TaskTemplate).filter(models.TaskTemplate.code == code).first()
        if not exists:
            db.add(models.TaskTemplate(code=code, name=name, description=desc))

    db.commit()


def seed_demo_if_empty(db: Session):
    if db.query(models.Room).count() == 0:
        rooms = [
            models.Room(name="Sala 1", target_temp_min=25, target_temp_max=28, target_rh_min=50, target_rh_max=70, target_co2_max=2000),
            models.Room(name="Sala 2", target_temp_min=25, target_temp_max=28, target_rh_min=50, target_rh_max=70, target_co2_max=2000),
            models.Room(name="Sala 3", target_temp_min=25, target_temp_max=28, target_rh_min=50, target_rh_max=70, target_co2_max=2000),
            models.Room(name="Sala 4", target_temp_min=25, target_temp_max=28, target_rh_min=50, target_rh_max=70, target_co2_max=2000),
        ]
        db.add_all(rooms)
        db.commit()

    if db.query(models.Item).filter(models.Item.category == "feed").count() == 0:
        db.add(models.Item(category="feed", name="Salvado", unit="kg"))
        db.add(models.Item(category="feed", name="Zanahoria", unit="kg"))
        db.commit()

    if db.query(models.StockMove).count() == 0:
        feed_items = db.query(models.Item).filter(models.Item.category == "feed").all()
        for it in feed_items:
            db.add(
                models.StockMove(
                    item_id=it.id,
                    move_type="in",
                    qty_kg=50.0,
                    ref_type="purchase",
                    ref_id="DEMO",
                    note="Stock inicial DEMO",
                )
            )
        db.commit()

    if db.query(models.Pallet).count() == 0:
        rooms = db.query(models.Room).order_by(models.Room.id).all()
        bm = crud.get_or_create_batch_month(db, date.today())

        # 3 pallets por sala, 26 bandejas por defecto
        for r in rooms:
            for _ in range(3):
                code = _next_pallet_code(db)
                p = models.Pallet(
                    room_id=r.id,
                    batch_month_id=bm.id,
                    code=code,
                    status="active",
                    tray_count=26,
                    notes="DEMO",
                )
                db.add(p)
                db.commit()