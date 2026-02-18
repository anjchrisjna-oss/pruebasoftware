from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas, crud

router = APIRouter(prefix="/pallets", tags=["Pallets"])


@router.post("", response_model=schemas.PalletOut)
def create_pallet(payload: schemas.PalletCreate, db: Session = Depends(get_db)):
    room = db.query(models.Room).filter(models.Room.id == payload.room_id).first()
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")

    today = datetime.utcnow().date()
    bm = crud.get_or_create_batch_month(db, today)

    if payload.tray_count <= 0:
        raise HTTPException(status_code=400, detail="tray_count must be > 0")

    pallet = models.Pallet(
        room_id=payload.room_id,
        code=payload.code.strip().upper(),
        tray_count=payload.tray_count,
        origin_lot=(payload.origin_lot.strip() if payload.origin_lot else None),
        parent_lot=(payload.parent_lot.strip() if payload.parent_lot else None),
        kg_per_tray=payload.kg_per_tray,
        extraction_count=payload.extraction_count or 0,
        logistic_status=(payload.logistic_status.strip() if payload.logistic_status else None),
        notes=(payload.notes.strip() if payload.notes else None),
        batch_month_id=bm.id,
    )
    db.add(pallet)
    db.commit()
    db.refresh(pallet)
    return pallet


@router.get("", response_model=list[schemas.PalletOut])
def list_pallets(db: Session = Depends(get_db)):
    return db.query(models.Pallet).order_by(models.Pallet.created_at.desc()).all()