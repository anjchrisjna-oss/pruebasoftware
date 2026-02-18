from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas, crud

router = APIRouter(prefix="/events", tags=["Events (Feed/Sieve)"])


@router.post("/feed", response_model=schemas.FeedEventOut)
def create_feed(payload: schemas.FeedEventCreate, db: Session = Depends(get_db)):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == payload.pallet_id).first()
    if not pallet:
        raise HTTPException(status_code=404, detail="Pallet not found")

    item = db.query(models.Item).filter(models.Item.id == payload.item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    if item.category != "feed":
        raise HTTPException(status_code=400, detail="Only feed items can be used in feed events")

    # Stock out (block negative)
    current = crud.get_stock_qty(db, payload.item_id)
    if current < payload.qty_kg:
        raise HTTPException(status_code=400, detail=f"Not enough feed stock. Current={current} kg")

    ev = models.FeedEvent(**payload.model_dump())
    db.add(ev)
    db.commit()
    db.refresh(ev)

    # Stock movement
    sm = models.StockMove(
        item_id=payload.item_id,
        move_type="out",
        qty_kg=payload.qty_kg,
        ref_type="feed",
        ref_id=str(ev.id),
        note=f"Feed pallet {pallet.code}",
    )
    crud.add_stock_move(db, sm)

    return ev


@router.post("/sieve", response_model=schemas.SieveEventOut)
def create_sieve(payload: schemas.SieveEventCreate, db: Session = Depends(get_db)):
    pallet = db.query(models.Pallet).filter(models.Pallet.id == payload.pallet_id).first()
    if not pallet:
        raise HTTPException(status_code=404, detail="Pallet not found")

    frass_item = db.query(models.Item).filter(models.Item.id == payload.frass_item_id).first()
    if not frass_item:
        raise HTTPException(status_code=404, detail="Item not found")

    if frass_item.category != "frass":
        raise HTTPException(status_code=400, detail="frass_item_id must be an item with category='frass'")

    ev = models.SieveEvent(**payload.model_dump())
    db.add(ev)
    db.commit()
    db.refresh(ev)

    # Stock movement (frass IN)
    sm = models.StockMove(
        item_id=payload.frass_item_id,
        move_type="in",
        qty_kg=payload.frass_kg,
        ref_type="sieve",
        ref_id=str(ev.id),
        note=f"Frass from pallet {pallet.code}",
    )
    crud.add_stock_move(db, sm)

    return ev