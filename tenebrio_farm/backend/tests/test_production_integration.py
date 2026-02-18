from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import sys
import types

# Stub mínimo para evitar dependencia externa python-multipart en tests.
multipart_pkg = types.ModuleType("multipart")
multipart_pkg.__version__ = "0.0-test"
multipart_sub = types.ModuleType("multipart.multipart")

def parse_options_header(value):
    return value, {}

multipart_sub.parse_options_header = parse_options_header
sys.modules.setdefault("multipart", multipart_pkg)
sys.modules.setdefault("multipart.multipart", multipart_sub)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import crud, models, models_production
from app.database import Base
from app.routers.production import ui_production_record


class ProductionIntegrationTests(TestCase):
    def setUp(self) -> None:
        self.tmpdir = TemporaryDirectory()
        db_path = Path(self.tmpdir.name) / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
        TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        Base.metadata.create_all(bind=engine)

        self.db_session_factory = TestingSessionLocal

        with TestingSessionLocal() as db:
            room = models.Room(name="Sala 1")
            db.add(room)
            db.flush()

            bm = models.BatchMonth(code="2026-01", start_date=date(2026, 1, 1), end_date=date(2026, 1, 31))
            db.add(bm)
            db.flush()

            p1 = models.Pallet(code="PAL-001", room_id=room.id, batch_month_id=bm.id, tray_count=10)
            p2 = models.Pallet(code="PAL-002", room_id=room.id, batch_month_id=bm.id, tray_count=8)
            db.add_all([p1, p2])

            feed = models.Item(category="feed", name="Pienso A", unit="kg")
            db.add(feed)
            db.commit()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_invalid_day_returns_redirect_error_instead_of_500(self) -> None:
        with self.db_session_factory() as db:
            pallet_ids = [p.id for p in db.query(models.Pallet).order_by(models.Pallet.code).all()]

            response = ui_production_record(
                day="2026-02-31",
                task_name="Alimentación",
                responsible="",
                minutes="0",
                location="",
                note="",
                feed1_item_id="",
                feed1_qty_per_tray_kg="",
                feed2_item_id="",
                feed2_qty_per_tray_kg="",
                frass_kg="",
                larvae_total_kg="",
                pallet_ids=pallet_ids,
                db=db,
            )

            self.assertEqual(response.status_code, 303)
            self.assertIn("error=Fecha%20inv%C3%A1lida", response.headers.get("location", ""))
            self.assertEqual(db.query(models_production.ProductionTask).count(), 0)

    def test_batch_operation_rolls_back_atomically_on_error(self) -> None:
        with self.db_session_factory() as db:
            pallets = db.query(models.Pallet).order_by(models.Pallet.code).all()
            pallet_ids = [p.id for p in pallets]
            feed_item = db.query(models.Item).filter(models.Item.category == "feed").first()
            self.assertIsNotNone(feed_item)
            feed_item_id = feed_item.id
            crud.add_stock_move(
                db,
                models.StockMove(
                    item_id=feed_item_id,
                    move_type="in",
                    qty_kg=100.0,
                    ref_type="purchase",
                    ref_id="init",
                    note="seed",
                ),
            )

        original_add_stock_move = crud.add_stock_move
        call_count = {"n": 0}

        def failing_add_stock_move(db, move, *, commit=True):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("forced stock failure")
            return original_add_stock_move(db, move, commit=commit)

        with self.db_session_factory() as db:
            with patch("app.routers.production.crud.add_stock_move", side_effect=failing_add_stock_move), patch("builtins.print"):
                response = ui_production_record(
                    day="2026-02-10",
                    task_name="Alimentación",
                    responsible="",
                    minutes="0",
                    location="",
                    note="",
                    feed1_item_id=str(feed_item_id),
                    feed1_qty_per_tray_kg="1",
                    feed2_item_id="",
                    feed2_qty_per_tray_kg="",
                    frass_kg="",
                    larvae_total_kg="",
                    pallet_ids=pallet_ids,
                    db=db,
                )

            self.assertEqual(response.status_code, 303)
            self.assertIn("PRO%20fall%C3%B3", response.headers.get("location", ""))
            self.assertEqual(db.query(models_production.ProductionTask).count(), 0)
            self.assertEqual(db.query(models.FeedEvent).count(), 0)
            # Solo queda el movimiento inicial de compra; no deben quedar salidas parciales.
            self.assertEqual(db.query(models.StockMove).count(), 1)
