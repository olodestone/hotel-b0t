"""Unit tests for logic-layer validation that isn't covered by the golden reports."""
import logic


def test_set_debt_staff_updates_trimmed(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "update_debt_staff_name",
                        lambda did, name: seen.update(args=(did, name)) or True)
    ok, msg = logic.process_set_debt_staff(7, "  bola  ")
    assert ok is True
    assert seen["args"] == (7, "bola")          # name trimmed before the write
    assert "#7" in msg and "Bola" in msg         # confirmation names the debt + new staff


def test_set_debt_staff_rejects_empty(monkeypatch):
    calls = {"n": 0}
    def fake(did, name):
        calls["n"] += 1
        return True
    monkeypatch.setattr(logic.db, "update_debt_staff_name", fake)
    ok, msg = logic.process_set_debt_staff(7, "   ")
    assert ok is False
    assert calls["n"] == 0                        # never touches the DB on an empty name
    assert "empty" in msg.lower()


def test_set_debt_staff_unknown_id(monkeypatch):
    monkeypatch.setattr(logic.db, "update_debt_staff_name", lambda did, name: False)
    ok, msg = logic.process_set_debt_staff(999, "bola")
    assert ok is False
    assert "No debt" in msg


# ── Stocktake (/count) ────────────────────────────────────────────────

def test_stock_count_reports_a_shortage_and_trues_the_books_up(monkeypatch):
    written = {}
    monkeypatch.setattr(logic.db, "get_drink",
                        lambda name: {"current_stock": 20, "cost_price": 150})
    monkeypatch.setattr(logic.db, "record_stock_count",
                        lambda *a, **kw: written.update(kw, drink=a[0]) or
                        {"variance": kw["counted"] - kw["expected"],
                         "value": (kw["counted"] - kw["expected"]) * kw["cost_price"]})

    ok, msg = logic.process_stock_count("heineken", 17, recorded_by="john")
    assert ok is True
    assert written["expected"] == 20            # expectation read from the books …
    assert written["counted"] == 17             # … and compared to the physical count
    assert "Short by 3 units" in msg
    assert "₦450" in msg                        # 3 × ₦150 at cost
    assert "theft" in msg                       # names what a shortage can mean


def test_stock_count_accepts_an_empty_shelf(monkeypatch):
    monkeypatch.setattr(logic.db, "get_drink", lambda name: {"current_stock": 4, "cost_price": 100})
    monkeypatch.setattr(logic.db, "record_stock_count",
                        lambda *a, **kw: {"variance": -4, "value": -400})
    ok, msg = logic.process_stock_count("coke", 0)
    assert ok is True and "Short by 4" in msg


def test_stock_count_rejects_negative_and_unknown_drinks(monkeypatch):
    monkeypatch.setattr(logic.db, "get_drink", lambda name: None)
    ok, msg = logic.process_stock_count("ghost", 5)
    assert ok is False and "not found" in msg

    ok, msg = logic.process_stock_count("coke", -1)
    assert ok is False and "negative" in msg


# ── Supplier credit ───────────────────────────────────────────────────

def test_restock_credit_adds_stock_without_spending_cash(monkeypatch):
    calls = {"expenses": 0}
    monkeypatch.setattr(logic.inv, "restock_drink",
                        lambda *a, **kw: logic.StockResult(ok=True, message="✅ Restocked"))
    monkeypatch.setattr(logic.db, "record_payable", lambda **kw: 42)
    monkeypatch.setattr(logic.db, "record_expense",
                        lambda *a, **kw: calls.update(expenses=calls["expenses"] + 1))

    ok, msg = logic.process_restock_credit("heineken", 24, 300, "nbl", due_date="2026-09-01")
    assert ok is True
    # The whole point: stock arrives, cash does not move until /pay_supplier.
    assert calls["expenses"] == 0
    assert "invoice #42" in msg and "₦7,200" in msg
    assert "2026-09-01" in msg


def test_restock_credit_validates_supplier_and_due_date(monkeypatch):
    monkeypatch.setattr(logic.inv, "restock_drink",
                        lambda *a, **kw: logic.StockResult(ok=True, message="ok"))
    ok, msg = logic.process_restock_credit("heineken", 24, 300, "   ")
    assert ok is False and "Supplier" in msg

    ok, msg = logic.process_restock_credit("heineken", 24, 300, "nbl", due_date="01-09-2026")
    assert ok is False and "YYYY-MM-DD" in msg


def test_pay_supplier_rejects_overpayment(monkeypatch):
    monkeypatch.setattr(logic.db, "pay_supplier",
                        lambda pid, amount=None, paid_by="":
                        {"error": "overpayment", "remaining": 2500.0, "payable_id": pid})
    ok, msg = logic.process_pay_supplier(3, amount=9000)
    assert ok is False and "₦2,500" in msg


def test_pay_supplier_reports_partial_and_full_settlement(monkeypatch):
    monkeypatch.setattr(logic.db, "pay_supplier", lambda pid, amount=None, paid_by="": {
        "payable_id": pid, "supplier": "nbl", "amount_paid_now": 2000.0,
        "total_paid": 2000.0, "remaining": 5200.0, "is_fully_paid": False,
    })
    ok, msg = logic.process_pay_supplier(1, amount=2000)
    assert ok is True and "Still owed" in msg and "₦5,200" in msg

    monkeypatch.setattr(logic.db, "pay_supplier", lambda pid, amount=None, paid_by="": {
        "payable_id": pid, "supplier": "nbl", "amount_paid_now": 7200.0,
        "total_paid": 7200.0, "remaining": 0, "is_fully_paid": True,
    })
    ok, msg = logic.process_pay_supplier(1)
    assert ok is True and "fully settled" in msg


def test_pay_supplier_unknown_invoice(monkeypatch):
    monkeypatch.setattr(logic.db, "pay_supplier", lambda pid, amount=None, paid_by="": None)
    ok, msg = logic.process_pay_supplier(999)
    assert ok is False and "No outstanding supplier invoice" in msg


# ── Room count ────────────────────────────────────────────────────────

def test_set_rooms_requires_a_positive_count(monkeypatch):
    written = {}
    monkeypatch.setattr(logic.db, "set_setting", lambda k, v: written.update({k: v}))
    ok, msg = logic.process_set_rooms(0)
    assert ok is False and written == {}

    ok, msg = logic.process_set_rooms(12)
    assert ok is True and written["total_rooms"] == "12"


def test_set_rooms_warns_when_the_total_contradicts_the_per_type_counts(monkeypatch):
    """Not an error — rooms can be out of service — but it must not pass silently."""
    monkeypatch.setattr(logic.db, "set_setting", lambda k, v: None)
    monkeypatch.setattr(logic.db, "get_all_room_type_counts", lambda: {"standard": 6, "deluxe": 2})

    ok, msg = logic.process_set_rooms(12)
    assert ok is True
    assert "add up to 8, not 12" in msg

    ok, msg = logic.process_set_rooms(8)          # agrees → no noise
    assert ok is True and "add up to" not in msg


def test_set_room_type_count_validates_before_writing(monkeypatch):
    writes = []
    monkeypatch.setattr(logic.db, "set_room_type_count", lambda t, n: writes.append((t, n)))
    monkeypatch.setattr(logic.db, "get_all_room_type_counts", lambda: {})

    assert logic.process_set_room_type_count("", 5)[0] is False
    assert logic.process_set_room_type_count("standard", 0)[0] is False
    assert logic.process_set_room_type_count("standard", -2)[0] is False
    assert writes == []                            # nothing reached the DB


def test_set_room_type_count_lowercases_and_lists_the_breakdown(monkeypatch):
    writes = []
    monkeypatch.setattr(logic.db, "set_room_type_count", lambda t, n: writes.append((t, n)))
    monkeypatch.setattr(logic.db, "get_all_room_type_counts",
                        lambda: {"standard": 6, "executive": 2})
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": "8")

    ok, msg = logic.process_set_room_type_count("  Executive  ", 2)
    assert ok is True
    assert writes == [("executive", 2)]            # trimmed + lower-cased
    assert "Standard: 6" in msg and "Executive: 2" in msg
    assert "Total listed: 8" in msg
    assert "add up to" not in msg                  # matches the hotel total


def test_set_room_type_count_points_at_the_missing_hotel_total(monkeypatch):
    monkeypatch.setattr(logic.db, "set_room_type_count", lambda t, n: None)
    monkeypatch.setattr(logic.db, "get_all_room_type_counts", lambda: {"standard": 5})
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": "")

    ok, msg = logic.process_set_room_type_count("standard", 5)
    assert ok is True
    assert "will use 5" in msg
