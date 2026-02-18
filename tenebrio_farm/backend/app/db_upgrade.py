# Simple SQLite schema upgrade (no Alembic).
#
# Run once after updating models to add new columns without deleting data:
#     python -m app.db_upgrade
#
# This script is idempotent: it checks if columns exist before adding them.

from sqlalchemy import text
from .database import engine


def _col_exists(conn, table: str, col: str) -> bool:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1] == col for r in rows)


def _add_col(conn, table: str, ddl: str, col: str) -> None:
    if not _col_exists(conn, table, col):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))


def run_upgrade() -> None:
    with engine.begin() as conn:
        # ---- pallets: campos de trazabilidad + cierre de ciclo
        _add_col(conn, "pallets", "origin_lot VARCHAR(60)", "origin_lot")
        _add_col(conn, "pallets", "parent_lot VARCHAR(60)", "parent_lot")
        _add_col(conn, "pallets", "kg_per_tray FLOAT", "kg_per_tray")
        _add_col(conn, "pallets", "extraction_count INTEGER DEFAULT 0", "extraction_count")
        _add_col(conn, "pallets", "logistic_status VARCHAR(40)", "logistic_status")

        _add_col(conn, "pallets", "is_closed BOOLEAN DEFAULT 0", "is_closed")
        _add_col(conn, "pallets", "closed_at DATETIME", "closed_at")
        _add_col(conn, "pallets", "closed_reason VARCHAR(200)", "closed_reason")
        _add_col(conn, "pallets", "cycle_stage VARCHAR(30) DEFAULT 'ACTIVE'", "cycle_stage")

        # ---- production_tasks: esquema nuevo (REGISTRO_TAREAS)
        _add_col(conn, "production_tasks", "day DATE", "day")
        _add_col(conn, "production_tasks", "room_id INTEGER", "room_id")
        _add_col(conn, "production_tasks", "task_name VARCHAR(80)", "task_name")
        _add_col(conn, "production_tasks", "responsible VARCHAR(60)", "responsible")
        _add_col(conn, "production_tasks", "minutes FLOAT", "minutes")
        _add_col(conn, "production_tasks", "location VARCHAR(80)", "location")

        _add_col(conn, "production_tasks", "feed1_item_id INTEGER", "feed1_item_id")
        _add_col(conn, "production_tasks", "feed1_qty_per_tray_kg FLOAT", "feed1_qty_per_tray_kg")
        _add_col(conn, "production_tasks", "feed2_item_id INTEGER", "feed2_item_id")
        _add_col(conn, "production_tasks", "feed2_qty_per_tray_kg FLOAT", "feed2_qty_per_tray_kg")

        _add_col(conn, "production_tasks", "frass_kg FLOAT", "frass_kg")
        _add_col(conn, "production_tasks", "larvae_total_kg FLOAT", "larvae_total_kg")
        _add_col(conn, "production_tasks", "larvae_per_tray_kg FLOAT", "larvae_per_tray_kg")
        _add_col(conn, "production_tasks", "note VARCHAR(255)", "note")


if __name__ == "__main__":
    run_upgrade()
    print("✅ DB upgrade done")
