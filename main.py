from fastapi import FastAPI

from .settings import settings
from .database import engine, Base, SessionLocal
from .seed import seed_minimum, seed_demo_if_empty
from .routers.history import router as history_router

# IMPORTANTE: importar modelos para que Base los registre
from . import models  # noqa: F401
from . import models_production  # noqa: F401  <- NUEVO (ProductionTask)

from .routers.rooms import router as rooms_router
from .routers.pallets import router as pallets_router
from .routers.environment import router as env_router
from .routers.items_stock import router as stock_router
from .routers.events import router as events_router
from .routers.tasks import router as tasks_router
from .routers.ui import router as ui_router
from .routers.alerts import router as alerts_router
from .routers.config import router as config_router

# NUEVO: router UI de producción PRO
from .routers.production import router as production_router

app = FastAPI(title=settings.app_name)
app.include_router(history_router)
# Create DB tables
Base.metadata.create_all(bind=engine)

# Seed data
with SessionLocal() as db:
    seed_minimum(db)
    seed_demo_if_empty(db)

# Routers
app.include_router(rooms_router)
app.include_router(pallets_router)
app.include_router(env_router)
app.include_router(stock_router)
app.include_router(events_router)
app.include_router(tasks_router)
app.include_router(ui_router)
app.include_router(alerts_router)
app.include_router(config_router)

# NUEVO: Registro PRO estilo SELECTOR
app.include_router(production_router)


@app.get("/")
def root():
    return {
        "app": settings.app_name,
        "docs": "/docs",
        "ui": "/ui",
        "tasks": "/ui/tasks",
        "production": "/ui/production",  # NUEVO
        "alerts": "/ui/alerts",  # NUEVO
        "config": "/ui/config",  # NUEVO
        "status": "ok",
    }