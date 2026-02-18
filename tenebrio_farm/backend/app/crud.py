from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import func, case
from . import models


# ---------- Batch month helper
def get_or_create_batch_month(db: Session, day: date) -> models.BatchMonth:
    code = f"{day.year:04d}-{day.month:02d}"
    existing = db.query(models.BatchMonth).filter(models.BatchMonth.code == code).first()
    if existing:
        return existing

    # Compute start/end of month
    start_date = day.replace(day=1)
    if day.month == 12:
        end_date = day.replace(day=31)
    else:
        next_month = day.replace(month=day.month + 1, day=1)
        end_date = next_month.fromordinal(next_month.toordinal() - 1)

    bm = models.BatchMonth(code=code, start_date=start_date, end_date=end_date)
    db.add(bm)
    db.commit()
    db.refresh(bm)
    return bm


# ---------- Stock helpers
def get_stock_qty(db: Session, item_id: int) -> float:
    """Return current stock quantity (kg) for an item.

    Rules:
      - move_type == 'in'     => +qty_kg
      - move_type == 'out'    => -qty_kg
      - move_type == 'adjust' => qty_kg can be +/-

    Implemented in SQL to avoid loading all moves.
    """
    signed_sum = (
        db.query(
            func.coalesce(
                func.sum(
                    case(
                        (
                            models.StockMove.move_type == "in",
                            models.StockMove.qty_kg,
                        ),
                        (
                            models.StockMove.move_type == "out",
                            -models.StockMove.qty_kg,
                        ),
                        else_=models.StockMove.qty_kg,
                    )
                ),
                0.0,
            )
        )
        .filter(models.StockMove.item_id == item_id)
        .scalar()
    )
    return float(signed_sum or 0.0)


def add_stock_move(db: Session, move: models.StockMove, *, commit: bool = True) -> models.StockMove:
    """Insert a stock move.

    When commit=False, caller is responsible for committing/rolling back.
    We still flush so the id is available.
    """
    db.add(move)
    db.flush()
    if commit:
        db.commit()
        db.refresh(move)
    return move