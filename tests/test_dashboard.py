"""
Smoke tests for the read-only web dashboard.

Covers the security-critical bits (Telegram HMAC verification, auth-gated
routes) and proves the data layer reuses the shared metrics core — the
dashboard_view numbers match the golden P&L from the bot's reports.
"""
import hashlib
import hmac
import time

from fastapi.testclient import TestClient

from dashboard import auth, data, settings
from dashboard.app import app

client = TestClient(app)


def _signed_login(token: str, **fields) -> dict:
    """Build a valid Telegram Login Widget payload signed with `token`."""
    fields.setdefault("auth_date", str(int(time.time())))
    check_string = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
    secret = hashlib.sha256(token.encode()).digest()
    fields["hash"] = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
    return fields


# ── Public routes ─────────────────────────────────────────────────────

def test_healthz_ok():
    r = client.get("/healthz")
    assert r.status_code == 200 and r.json() == {"status": "ok"}


def test_login_page_renders():
    r = client.get("/login")
    assert r.status_code == 200
    assert "Telegram" in r.text          # login page is about Telegram sign-in


def test_root_redirects_to_login_when_anonymous():
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/login"


# ── Telegram login verification ───────────────────────────────────────

def test_verify_telegram_login_valid():
    token = "123456:TESTTOKEN"
    payload = _signed_login(token, id="42", first_name="Ada", username="ada")
    assert auth.verify_telegram_login(payload, token) is True


def test_verify_telegram_login_rejects_tampering():
    token = "123456:TESTTOKEN"
    payload = _signed_login(token, id="42", first_name="Ada")
    payload["id"] = "999"  # tamper after signing
    assert auth.verify_telegram_login(payload, token) is False


def test_verify_telegram_login_rejects_stale():
    token = "123456:TESTTOKEN"
    payload = _signed_login(token, id="42", first_name="Ada", auth_date=str(int(time.time()) - 10 * 3600 * 24))
    assert auth.verify_telegram_login(payload, token) is False


def test_verify_telegram_login_rejects_wrong_token():
    payload = _signed_login("realtoken", id="42", first_name="Ada")
    assert auth.verify_telegram_login(payload, "different-token") is False


# ── Data layer reuses the shared metrics core ─────────────────────────

def test_dashboard_view_matches_golden_numbers():
    # Uses the autouse fixture dataset from conftest (frozen time + fake DB).
    view = data.dashboard_view("all")
    assert view["period_label"] == "ALL-TIME"
    assert view["pnl"].total_revenue == 163000
    assert view["pnl"].net_profit == 37201          # identical to the bot's golden full report
    assert view["owed"].total_owed == 33000
    assert view["stock_value"] == 11160             # (12+24)*300 + (3+0)*120
    assert len(view["low_stock"]) == 1              # coke bar stock 3 <= threshold 5
    assert view["trend"]                            # non-empty daily series

    pos = view["position"]                          # 'as of now', same as bot /position
    assert pos.cash == -60999                       # 0 + 130000 − 120999 − 30000 − 40000
    assert pos.receivables == 33000 and pos.outstanding_count == 2
    assert pos.profit_all == 37201                  # all-time net profit (matches full report)
    assert pos.month_profit == 31800

    staff = {s["name"]: s for s in view["staff"]}   # matches bot /staff_report
    assert staff["john"]["drink_txns"] == 2 and staff["john"]["drink_revenue"] == 5000
    assert staff["john"]["room_txns"] == 1 and staff["john"]["room_revenue"] == 50000
    assert staff["mary"]["drink_revenue"] == 3000 and staff["mary"]["room_revenue"] == 105000


def test_dashboard_template_renders(monkeypatch):
    # Render the real dashboard template against fixture data to catch template
    # errors (macros, filters, loops) that the redirect-only route tests miss.
    from dashboard.app import templates
    view = data.dashboard_view("all")
    session = {"username": "dev", "role": "admin",
               "hotels": [{"schema": "hotel85", "name": "Hotel 85", "role": "admin"}]}
    html = templates.get_template("dashboard.html").render(
        request=None, hotel_name="Hotel 85", session=session,
        current_schema="hotel85", view=view, role="admin", current_period="",
    )
    assert "Profit &amp; Loss" in html
    assert "₦163,000" in html          # total revenue card, naira-formatted
    assert "Heineken" in html          # stock table
    assert "What you have now" in html  # cash position section
    assert "₦-60,999" in html          # cash-at-hand headline
    assert "Recommended set-asides" in html   # allocation section
    assert "Sales by drink" in html           # sales drill-down
    assert "Staff activity" in html           # per-staff section
    assert "Export CSV" in html               # export links


def test_dashboard_view_staff_filter():
    # No filter → no focus panel.
    assert data.dashboard_view("all")["staff_detail"] is None

    # Focused on 'john' → only his recorded revenue/activity.
    view = data.dashboard_view("all", staff="john")
    assert view["selected_staff"] == "john"
    sd = view["staff_detail"]
    assert sd["name"] == "john"
    assert sd["bar_rev"] == 5000 and sd["drink_txns"] == 2     # heineken x6 + coke x10
    assert sd["room_rev"] == 50000 and sd["room_txns"] == 1    # one deluxe booking
    assert sd["total_rev"] == 55000
    assert [d["drink"] for d in sd["sales_breakdown"]] == ["Coke", "Heineken"]

    # Hotel-wide figures are NOT filtered by staff — identical with/without focus.
    assert view["pnl"].net_profit == data.dashboard_view("all")["pnl"].net_profit
    assert view["position"].cash == data.dashboard_view("all")["position"].cash


def test_dashboard_template_renders_staff_focus():
    from dashboard.app import templates
    view = data.dashboard_view("all", staff="john")
    session = {"username": "dev", "role": "admin",
               "hotels": [{"schema": "hotel85", "name": "Hotel 85", "role": "admin"}]}
    html = templates.get_template("dashboard.html").render(
        request=None, hotel_name="Hotel 85", session=session,
        current_schema="hotel85", view=view, role="admin", current_period="",
    )
    assert "Focus: john" in html
    assert "clear filter" in html
    assert "&staff=mary" in html          # other staff still linkable to switch focus


def test_dashboard_view_ledger_records():
    # Raw records are exposed for in-browser viewing (not CSV-only).
    led = data.dashboard_view("all")["ledger"]
    # Period-filtered + active (deleted sale id=5 dropped), newest-first.
    assert [r["id"] for r in led["sales"]] == [3, 2, 1, 4]
    assert [r["id"] for r in led["rooms"]] == [2, 1, 3]
    assert {r["id"] for r in led["expenses"]} == {1, 2, 3, 4, 5, 6, 7}   # all-time incl. restock (id=5)
    # Debtors: outstanding only, each with a computed remaining balance.
    debtors = {r["name"]: r for r in led["debtors"]}
    assert "paid guy" not in debtors                 # fully-paid debtor excluded
    assert debtors["sam"]["remaining"] == 3000       # 5000 owed − 2000 paid
    assert debtors["acme corp"]["remaining"] == 30000


def test_dashboard_view_previous_month():
    # Navigating to a prior month scopes every record + figure to that month.
    view = data.dashboard_view("2026-05")
    assert view["period_label"] == "May 2026"
    assert view["picker"] == {"month": "2026-05", "date": ""}
    assert [r["id"] for r in view["ledger"]["sales"]] == [4]   # only the May sale
    assert view["pnl"].total_revenue == 16000                  # 1000 drink + 15000 room (May)


def test_dashboard_view_specific_date_picker():
    view = data.dashboard_view("2026-06-20")
    assert view["picker"] == {"month": "", "date": "2026-06-20"}
    assert [r["id"] for r in view["ledger"]["sales"]] == [3]   # the 06-20 coke sale


def test_dashboard_template_renders_records_and_pickers():
    from dashboard.app import templates
    view = data.dashboard_view("all")
    session = {"username": "dev", "role": "admin",
               "hotels": [{"schema": "hotel85", "name": "Hotel 85", "role": "admin"}]}
    html = templates.get_template("dashboard.html").render(
        request=None, hotel_name="Hotel 85", session=session,
        current_schema="hotel85", view=view, role="admin", current_period="all",
    )
    assert "Records" in html
    assert 'type="month"' in html and 'type="date"' in html    # previous-month / date pickers
    assert "🍺 Sales" in html and "🧾 Debtors" in html
    assert "acme corp" in html        # a raw debtor name now visible in-browser, not download-only
    assert "20 Jun" in html           # formatted record timestamp (the 06-20 sale)


def _render(role: str, **ctx) -> str:
    from dashboard.app import templates
    view = data.dashboard_view("all")
    session = {"username": "dev", "role": role,
               "hotels": [{"schema": "hotel85", "name": "Hotel 85", "role": role}]}
    return templates.get_template("dashboard.html").render(
        request=None, hotel_name="Hotel 85", session=session,
        current_schema="hotel85", view=view, role=role, current_period="", **ctx,
    )


def test_expenses_table_hidden_from_staff():
    admin_html = _render("admin")
    assert "💸 Expenses" in admin_html          # itemised expenses visible to admin
    assert ">Expenses</a>" in admin_html         # and the CSV chip

    staff_html = _render("staff")
    assert "💸 Expenses" not in staff_html       # hidden from staff (mirrors /history)
    assert ">Expenses</a>" not in staff_html      # no expenses CSV chip either
    assert "🍺 Sales" in staff_html               # other records still visible to staff


def _login_cookie(role: str):
    """Forge a signed session cookie for an authenticated request in tests."""
    payload = {"tid": 1, "username": role, "role": role,
               "hotels": [{"schema": "hotel85", "name": "Hotel 85", "role": role}],
               "schema": "hotel85"}
    return {settings.SESSION_COOKIE: auth.serialize_session(payload)}


def test_export_expenses_route_gated_to_admin():
    # Staff is blocked server-side even hitting the URL directly...
    r = client.get("/export/expenses.csv?period=all", cookies=_login_cookie("staff"))
    assert r.status_code == 403
    # ...but other exports still work for staff, and admin can export expenses.
    assert client.get("/export/sales.csv?period=all", cookies=_login_cookie("staff")).status_code == 200
    assert client.get("/export/expenses.csv?period=all", cookies=_login_cookie("admin")).status_code == 200


def test_unknown_export_returns_404_not_500():
    # Guards the require_tenant cleanup fix: an HTTPException raised after the
    # dependency yields must surface as its real status, not a masked 500.
    r = client.get("/export/nonsense.csv?period=all", cookies=_login_cookie("admin"))
    assert r.status_code == 404


# ── Access control: only owner / admins / users-table members get in ──

def test_resolve_access_rejects_unknown_telegram_user(monkeypatch):
    # One hotel; the logged-in stranger is NOT its owner and NOT in admin_ids.
    monkeypatch.setattr(auth, "_hotels_meta", lambda: [
        {"schema": "hotel85", "name": "Hotel 85", "owner": 999, "admin_ids": ""}])
    monkeypatch.setattr(auth.settings, "OWNER_ID", None)
    monkeypatch.setattr(auth.settings, "ADMIN_IDS", [])

    # Stranger absent from the hotel's users table → no access at all.
    monkeypatch.setattr(auth.db, "get_user", lambda tid: None)
    assert auth.resolve_access(12345) == []

    # A user present in the users table → access with that exact role.
    monkeypatch.setattr(auth.db, "get_user", lambda tid: {"role": "staff"})
    assert auth.resolve_access(12345) == [
        {"schema": "hotel85", "name": "Hotel 85", "role": "staff"}]

    # The hotel owner is always an admin, even without a users-table row.
    monkeypatch.setattr(auth.db, "get_user", lambda tid: None)
    assert auth.resolve_access(999) == [
        {"schema": "hotel85", "name": "Hotel 85", "role": "admin"}]


def test_login_rejected_for_telegram_user_without_access(monkeypatch):
    # A genuine, correctly-signed Telegram login still gets bounced if the
    # account maps to no hotel — identity verified ≠ authorized.
    token = "123456:LOGINTOKEN"
    monkeypatch.setattr(settings, "LOGIN_BOT_TOKEN", token)
    monkeypatch.setattr(auth, "resolve_access", lambda tid: [])
    payload = _signed_login(token, id="777", first_name="Stranger")
    r = client.get("/auth/telegram", params=payload, follow_redirects=False)
    assert r.status_code == 303
    assert "no+hotel+access" in r.headers["location"]
    assert settings.SESSION_COOKIE not in r.cookies      # no session granted


def test_export_dataset():
    cols, rows = data.export_dataset("sales", "all")
    assert "total_revenue" in cols
    assert len(rows) == 4              # 4 active (non-deleted) sales all-time
    assert data.export_dataset("nonsense", "all") is None
    _, debtors = data.export_dataset("debtors", None)
    assert all(r["status"] == "outstanding" for r in debtors)


def test_parse_period_variants():
    assert data.parse_period("") == (None, None, False)
    assert data.parse_period("all") == (None, None, True)
    assert data.parse_period("2026-06") == (None, (2026, 6), False)
    fd, fm, at = data.parse_period("2026-06-05")
    assert (fd.year, fd.month, fd.day) == (2026, 6, 5) and fm is None and at is False
