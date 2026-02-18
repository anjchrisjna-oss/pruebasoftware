from datetime import date, datetime
from pydantic import BaseModel, Field


# -------- Rooms
class RoomCreate(BaseModel):
    name: str
    target_temp_min: float | None = None
    target_temp_max: float | None = None
    target_rh_min: float | None = None
    target_rh_max: float | None = None
    target_co2_min: float | None = None
    target_co2_max: float | None = None


class RoomOut(RoomCreate):
    id: int


# -------- Batch month
class BatchMonthOut(BaseModel):
    id: int
    code: str
    start_date: date
    end_date: date


# -------- Pallets
class PalletCreate(BaseModel):
    room_id: int
    code: str = Field(..., examples=["PAL-000123"])
    tray_count: int = 26
    origin_lot: str | None = None
    parent_lot: str | None = None
    kg_per_tray: float | None = None
    extraction_count: int = 0
    logistic_status: str | None = None
    notes: str | None = None


class PalletOut(BaseModel):
    id: str
    code: str
    status: str
    created_at: datetime
    room_id: int
    batch_month_id: int
    tray_count: int
    origin_lot: str | None = None
    parent_lot: str | None = None
    kg_per_tray: float | None = None
    extraction_count: int
    logistic_status: str | None = None
    notes: str | None = None


# -------- Environment
class EnvReadingCreate(BaseModel):
    room_id: int
    day: date
    temp_c: float
    rh_pct: float
    co2_ppm: float
    source: str = "manual"


class EnvReadingOut(EnvReadingCreate):
    id: int
    created_at: datetime


# -------- Items and stock
class ItemCreate(BaseModel):
    category: str  # feed/frass/other
    name: str
    unit: str = "kg"

    # Umbrales de avisos (opcionales). Si se dejan a 0/None, no generan aviso.
    min_threshold: float | None = 0.0
    critical_threshold: float | None = 0.0


class ItemOut(ItemCreate):
    id: int


class ItemUpdate(BaseModel):
    category: str | None = None
    name: str | None = None
    unit: str | None = None
    min_threshold: float | None = None
    critical_threshold: float | None = None


class StockMoveCreate(BaseModel):
    item_id: int
    move_type: str  # in/out/adjust
    qty_kg: float
    ref_type: str
    ref_id: str | None = None
    note: str | None = None


class StockMoveOut(StockMoveCreate):
    id: int
    created_at: datetime


# -------- Events
class FeedEventCreate(BaseModel):
    pallet_id: str
    item_id: int
    qty_kg: float
    note: str | None = None


class FeedEventOut(FeedEventCreate):
    id: int
    created_at: datetime


class SieveEventCreate(BaseModel):
    pallet_id: str
    frass_item_id: int
    frass_kg: float
    residue_kg: float | None = None
    note: str | None = None


class SieveEventOut(SieveEventCreate):
    id: int
    created_at: datetime


# -------- Tasks
class TaskTemplateCreate(BaseModel):
    code: str
    name: str
    description: str | None = None


class TaskTemplateOut(TaskTemplateCreate):
    id: int


class TaskInstanceCreate(BaseModel):
    task_template_id: int
    due_day: date
    pallet_id: str | None = None
    room_id: int | None = None
    note: str | None = None


class TaskInstanceOut(TaskInstanceCreate):
    id: int
    status: str

# ---- Configuración dinámica (UI / Avisos / IA)
class FarmConfigBase(BaseModel):
    category: str = "general"
    key: str
    value: str = ""
    value_type: str = "str"
    description: str | None = None


class FarmConfigCreate(FarmConfigBase):
    pass


class FarmConfigOut(FarmConfigBase):
    id: int
    updated_at: datetime
