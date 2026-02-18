from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..database import get_db
from .. import models

router = APIRouter(tags=["UI"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/ui/config", response_class=HTMLResponse)
def ui_config(request: Request, db: Session = Depends(get_db)):
    rows = db.query(models.FarmConfig).order_by(models.FarmConfig.category.asc(), models.FarmConfig.key.asc()).all()
    # categorías sugeridas (para el selector)
    cats = sorted({r.category for r in rows} | {"general", "stock", "ambiente", "biologia", "produccion", "avisos", "ia"})
    return templates.TemplateResponse(
        "config.html",
        {"request": request, "rows": rows, "categories": cats},
    )


@router.post("/ui/config/set")
def ui_config_set(
    request: Request,
    db: Session = Depends(get_db),
    config_id: int | None = Form(default=None),
    category: str = Form(default="general"),
    key: str = Form(...),
    value: str = Form(default=""),
    value_type: str = Form(default="str"),
    description: str | None = Form(default=None),
):
    key = key.strip()
    if not key:
        return RedirectResponse(url="/ui/config", status_code=303)

    # update by id if provided; else upsert by key
    obj = None
    if config_id:
        obj = db.query(models.FarmConfig).filter(models.FarmConfig.id == config_id).first()

    if obj is None:
        obj = db.query(models.FarmConfig).filter(models.FarmConfig.key == key).first()

    if obj is None:
        obj = models.FarmConfig(key=key)
        db.add(obj)

    obj.category = (category or "general").strip() or "general"
    obj.key = key
    obj.value = value if value is not None else ""
    obj.value_type = (value_type or "str").strip() or "str"
    obj.description = description

    db.commit()
    return RedirectResponse(url="/ui/config", status_code=303)


@router.post("/ui/config/delete")
def ui_config_delete(
    request: Request,
    db: Session = Depends(get_db),
    config_id: int = Form(...),
):
    obj = db.query(models.FarmConfig).filter(models.FarmConfig.id == config_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return RedirectResponse(url="/ui/config", status_code=303)
