from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/environment", tags=["Environment"])


@router.post("", response_model=schemas.EnvReadingOut)
def create_env(payload: schemas.EnvReadingCreate, db: Session = Depends(get_db)):
    reading = models.EnvReading(**payload.model_dump())
    db.add(reading)
    db.commit()
    db.refresh(reading)
    return reading


@router.get("", response_model=list[schemas.EnvReadingOut])
def list_env(db: Session = Depends(get_db)):
    return db.query(models.EnvReading).order_by(models.EnvReading.day.desc()).all()