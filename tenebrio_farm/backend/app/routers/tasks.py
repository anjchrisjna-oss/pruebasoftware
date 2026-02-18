from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.post("/templates", response_model=schemas.TaskTemplateOut)
def create_template(payload: schemas.TaskTemplateCreate, db: Session = Depends(get_db)):
    t = models.TaskTemplate(**payload.model_dump())
    db.add(t)
    db.commit()
    db.refresh(t)
    return t


@router.get("/templates", response_model=list[schemas.TaskTemplateOut])
def list_templates(db: Session = Depends(get_db)):
    return db.query(models.TaskTemplate).order_by(models.TaskTemplate.code).all()


@router.post("", response_model=schemas.TaskInstanceOut)
def create_task(payload: schemas.TaskInstanceCreate, db: Session = Depends(get_db)):
    ti = models.TaskInstance(
        task_template_id=payload.task_template_id,
        due_day=payload.due_day,
        pallet_id=payload.pallet_id,
        room_id=payload.room_id,
        note=payload.note,
    )
    db.add(ti)
    db.commit()
    db.refresh(ti)
    return schemas.TaskInstanceOut(
        id=ti.id,
        task_template_id=ti.task_template_id,
        due_day=ti.due_day,
        status=ti.status,
        pallet_id=ti.pallet_id,
        room_id=ti.room_id,
        note=ti.note,
    )


@router.get("", response_model=list[schemas.TaskInstanceOut])
def list_tasks(due_day: date | None = None, db: Session = Depends(get_db)):
    q = db.query(models.TaskInstance)
    if due_day:
        q = q.filter(models.TaskInstance.due_day == due_day)
    tasks = q.order_by(models.TaskInstance.due_day.desc(), models.TaskInstance.id.desc()).all()

    out = []
    for t in tasks:
        out.append(
            schemas.TaskInstanceOut(
                id=t.id,
                task_template_id=t.task_template_id,
                due_day=t.due_day,
                status=t.status,
                pallet_id=t.pallet_id,
                room_id=t.room_id,
                note=t.note,
            )
        )
    return out