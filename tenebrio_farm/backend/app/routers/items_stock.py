from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas, crud

router = APIRouter(prefix="/stock", tags=["Items & Stock"])


@router.post("/items", response_model=schemas.ItemOut)
def create_item(payload: schemas.ItemCreate, db: Session = Depends(get_db)):
    item = models.Item(**payload.model_dump())
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.get("/items", response_model=list[schemas.ItemOut])
def list_items(db: Session = Depends(get_db)):
    return db.query(models.Item).order_by(models.Item.category, models.Item.name).all()


@router.post("/moves", response_model=schemas.StockMoveOut)
def create_stock_move(payload: schemas.StockMoveCreate, db: Session = Depends(get_db)):
    # Optional: prevent negative stock for outs
    if payload.move_type == "out":
        current = crud.get_stock_qty(db, payload.item_id)
        if current < payload.qty_kg:
            raise HTTPException(status_code=400, detail=f"Not enough stock. Current={current} kg")

    move = models.StockMove(**payload.model_dump())
    return crud.add_stock_move(db, move)


@router.get("/moves", response_model=list[schemas.StockMoveOut])
def list_stock_moves(db: Session = Depends(get_db)):
    return db.query(models.StockMove).order_by(models.StockMove.created_at.desc()).all()


@router.get("/qty/{item_id}")
def get_item_stock(item_id: int, db: Session = Depends(get_db)):
    return {"item_id": item_id, "qty_kg": crud.get_stock_qty(db, item_id)}