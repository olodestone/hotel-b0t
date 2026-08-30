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


# ── Turnaways ─────────────────────────────────────────────────────────

def test_turnaway_records_and_never_touches_money(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "record_turnaway",
                        lambda **kw: seen.update(kw) or 1)
    ok, msg = logic.process_turnaway(3, room_type="standard", reason="fully booked",
                                     recorded_by="mary", timestamp="2026-08-29")
    assert ok is True
    assert seen == {"room_type": "standard", "quantity": 3, "reason": "fully booked",
                    "recorded_by": "mary", "timestamp": "2026-08-29"}
    assert "3 turned away" in msg and "Standard" in msg
    assert "2026-08-29" in msg


def test_turnaway_rejects_a_non_positive_count(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "record_turnaway",
                        lambda **kw: calls.update(n=calls["n"] + 1) or 1)
    for bad in (0, -2):
        ok, msg = logic.process_turnaway(bad)
        assert ok is False and "at least 1" in msg
    assert calls["n"] == 0


def test_turnaway_rejects_an_implausible_count(monkeypatch):
    """A fat-fingered 3000 would swamp every ratio it feeds."""
    monkeypatch.setattr(logic.db, "record_turnaway", lambda **kw: 1)
    ok, msg = logic.process_turnaway(3000)
    assert ok is False and "typo" in msg


# ── Room stay length ──────────────────────────────────────────────────

def test_set_room_duration_marks_a_type_hourly(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "set_room_type_hours",
                        lambda t, h: seen.update(args=(t, h)))
    ok, msg = logic.process_set_room_duration("short time", 2)
    assert ok is True
    assert seen["args"] == ("short time", 2)
    assert "lets" in msg and "not room-nights" in msg


def test_set_room_duration_rejects_more_than_a_day(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "set_room_type_hours",
                        lambda t, h: calls.update(n=calls["n"] + 1))
    ok, msg = logic.process_set_room_duration("standard", 36)
    assert ok is False and "nights" in msg
    assert calls["n"] == 0


def test_set_room_duration_back_to_24_restores_nightly(monkeypatch):
    monkeypatch.setattr(logic.db, "set_room_type_hours", lambda t, h: None)
    ok, msg = logic.process_set_room_duration("short time", 24)
    assert ok is True and "full night" in msg


def test_room_sale_names_the_unit_it_was_sold_in(monkeypatch):
    monkeypatch.setattr(logic.db, "record_room", lambda *a, **k: 1)
    monkeypatch.setattr(logic.db, "get_room_type_hours", lambda t: 2.0)
    ok, msg, _ = logic.process_room_sale("short time", 1, 3000, 3)
    assert ok is True
    assert "let(s) of 2h" in msg          # never "3 night(s)" for three lets
    assert "/night" not in msg


def test_room_sale_passes_a_negotiated_duration_through(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "record_room",
                        lambda *a, **k: seen.update(k) or 1)
    monkeypatch.setattr(logic.db, "get_room_type_hours", lambda t: None)
    ok, msg, _ = logic.process_room_sale("standard", 1, 5000, 1, duration_hours=3)
    assert ok is True and seen["duration_hours"] == 3
    assert "let(s) of 3h" in msg


def test_room_sale_rejects_a_duration_longer_than_a_day(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "record_room",
                        lambda *a, **k: calls.update(n=calls["n"] + 1) or 1)
    ok, msg, rid = logic.process_room_sale("standard", 1, 5000, 1, duration_hours=25)
    assert ok is False and rid == 0
    assert "between 0 and 24 hours" in msg
    assert calls["n"] == 0


# ── Expense classification ────────────────────────────────────────────

def test_expense_accepts_overhead(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "record_expense",
                        lambda *a, **k: seen.update(args=a, **k))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("overhead", "levies", 15000)
    assert ok is True
    assert seen["args"][0] == "overhead"
    assert seen["expense_class"] == "operating"


def test_debtors_and_draws_still_refuse_overhead(monkeypatch):
    """Nobody owes money to 'overhead', and a draw comes out of a real account."""
    monkeypatch.setattr(logic.db, "record_draw", lambda *a, **k: None)
    ok, msg = logic.process_draw(5000, account="overhead")
    assert ok is False and "rooms" in msg


def test_capital_under_the_threshold_is_refused(monkeypatch):
    """Under the line you expense it even when it is technically an asset."""
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "record_expense",
                        lambda *a, **k: calls.update(n=calls["n"] + 1))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("rooms", "consumables", 30000, expense_class="capital")
    assert ok is False and "under the" in msg
    assert calls["n"] == 0


def test_capital_at_the_threshold_is_accepted(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "record_expense", lambda *a, **k: seen.update(k))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("rooms", "maintenance", 115000, expense_class="capital")
    assert ok is True and seen["expense_class"] == "capital"
    assert "not a cost" in msg


def test_the_capital_threshold_is_a_setting(monkeypatch):
    monkeypatch.setattr(logic.db, "record_expense", lambda *a, **k: None)
    monkeypatch.setattr(logic.db, "get_setting",
                        lambda k, d="": "200000" if k == "capital_threshold" else d)
    assert logic.capital_threshold() == 200000.0
    ok, msg = logic.process_expense("rooms", "maintenance", 115000, expense_class="capital")
    assert ok is False          # below the raised threshold now


def test_inventory_is_routed_to_restock_not_expensed(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "record_expense",
                        lambda *a, **k: calls.update(n=calls["n"] + 1))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("bar", "beer", 30000, expense_class="inventory")
    assert ok is False and "/restock" in msg
    assert calls["n"] == 0


def test_unknown_class_is_rejected_rather_than_silently_defaulted(monkeypatch):
    monkeypatch.setattr(logic.db, "record_expense", lambda *a, **k: None)
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("bar", "utilities", 5000, expense_class="guesswork")
    assert ok is False and "Class must be" in msg


def test_reclassify_changes_class_but_never_the_amount(monkeypatch):
    row = {"id": 7, "account": "rooms", "category": "maintenance",
           "amount": 115000, "expense_class": "operating"}
    seen = {}
    monkeypatch.setattr(logic.db, "get_expense", lambda i: dict(row))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    def _recl(i, **kw):
        seen.update(id=i, **kw)
        row.update({k: v for k, v in kw.items() if v is not None and k != "needs_review"})
        return True
    monkeypatch.setattr(logic.db, "reclassify_expense", _recl)
    ok, msg = logic.process_reclassify_expense(7, expense_class="capital")
    assert ok is True
    assert seen["expense_class"] == "capital"
    assert "amount" not in seen              # never touched
    assert "115,000" in msg


def test_reclassify_refuses_capital_below_the_threshold(monkeypatch):
    monkeypatch.setattr(logic.db, "get_expense",
                        lambda i: {"id": 3, "amount": 9000, "account": "rooms",
                                   "category": "consumables"})
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "reclassify_expense",
                        lambda i, **k: calls.update(n=calls["n"] + 1) or True)
    ok, msg = logic.process_reclassify_expense(3, expense_class="capital")
    assert ok is False and "threshold" in msg
    assert calls["n"] == 0


def test_reclassify_rejects_an_unknown_id(monkeypatch):
    monkeypatch.setattr(logic.db, "get_expense", lambda i: None)
    ok, msg = logic.process_reclassify_expense(999, expense_class="capital")
    assert ok is False and "999" in msg


# ── Periodic obligations ──────────────────────────────────────────────

def test_register_a_periodic_bill(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "add_obligation",
                        lambda *a, **k: seen.update(args=a, **k) or 1)
    ok, msg = logic.process_add_obligation("soakaway evacuation", 90000, 6)
    assert ok is True
    assert seen["args"] == ("soakaway evacuation", 90000, 6)
    assert "₦15,000.00/month" in msg


def test_a_monthly_bill_is_not_a_periodic_one(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "add_obligation",
                        lambda *a, **k: calls.update(n=calls["n"] + 1) or 1)
    ok, msg = logic.process_add_obligation("diesel", 80000, 1)
    assert ok is False and "operating expense" in msg
    assert calls["n"] == 0


def test_retiring_a_bill_is_never_a_delete(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "set_obligation_active",
                        lambda i, a: seen.update(id=i, active=a) or True)
    ok, msg = logic.process_retire_obligation(3)
    assert ok is True and seen == {"id": 3, "active": False}
    assert "stays in the reserve" in msg


def test_paying_a_periodic_bill_links_it_to_its_obligation(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "record_expense", lambda *a, **k: seen.update(k))
    monkeypatch.setattr(logic.db, "get_setting", lambda k, d="": d)
    ok, msg = logic.process_expense("rooms", "maintenance", 90000,
                                    expense_class="periodic", obligation_id=1)
    assert ok is True
    assert seen["obligation_id"] == 1
    assert "drawn from the reserve" in msg


def test_set_purchase_cap(monkeypatch):
    seen = {}
    monkeypatch.setattr(logic.db, "set_setting", lambda k, v: seen.update(k=k, v=v))
    ok, msg = logic.process_set_purchase_cap(45)
    assert ok is True and seen == {"k": "purchase_cap", "v": "45.0"}
    assert "45%" in msg


def test_purchase_cap_rejects_nonsense(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(logic.db, "set_setting",
                        lambda k, v: calls.update(n=calls["n"] + 1))
    for bad in (0, -5, 150):
        ok, _ = logic.process_set_purchase_cap(bad)
        assert ok is False
    assert calls["n"] == 0
