from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/rooms", tags=["Rooms"])


@router.post("", response_model=schemas.RoomOut)
def create_room(payload: schemas.RoomCreate, db: Session = Depends(get_db)):
    room = models.Room(**payload.model_dump())
    db.add(room)
    db.commit()
    db.refresh(room)
    return room


@router.get("", response_model=list[schemas.RoomOut])
def list_rooms(db: Session = Depends(get_db)):
    return db.query(models.Room).order_by(models.Room.id).all()