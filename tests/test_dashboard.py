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

from dashboard import auth, data
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
    assert "Dashboard" in r.text


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
