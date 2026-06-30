"""
FastAPI application — read-only hotel dashboard.

Run it (separate process from the bot):
    uvicorn dashboard.app:app --host 0.0.0.0 --port ${PORT:-8000}

Security model:
  • Identity  — Telegram Login Widget, verified by HMAC (auth.verify_telegram_login).
  • Tenancy   — every authenticated request sets database._hotel_schema_var to the
                selected hotel's schema (require_tenant), so all reads are scoped
                to that hotel and one tenant can never see another's data.
  • Read-only — no route mutates the database.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import database as db
import metrics
from . import auth, data, settings

BASE_DIR = Path(__file__).resolve().parent


def _asset_version() -> str:
    """Cache-busting token — newest static-file mtime. Changes each deploy so
    browsers fetch fresh CSS/JS instead of serving a stale cached stylesheet."""
    try:
        return str(int(max(f.stat().st_mtime for f in (BASE_DIR / "static").glob("*"))))
    except (ValueError, OSError):
        return "1"


STATIC_VERSION = _asset_version()

app = FastAPI(title=f"{settings.HOTEL_NAME} Dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["naira"] = lambda v: f"₦{float(v or 0):,.0f}"


def _fmt_when(raw) -> str:
    """Render a record timestamp as 'DD Mon · HH:MM' for the in-browser tables."""
    dt = metrics.parse_ts(raw)
    return dt.strftime("%d %b · %H:%M") if dt else str(raw or "")


templates.env.filters["when"] = _fmt_when


class _NeedsLogin(Exception):
    """Raised by dependencies when the request has no valid session."""


@app.exception_handler(_NeedsLogin)
async def _redirect_to_login(request: Request, exc: _NeedsLogin):  # noqa: ARG001
    return RedirectResponse("/login", status_code=303)


def _session(request: Request) -> dict | None:
    return auth.read_session_token(request.cookies.get(settings.SESSION_COOKIE))


def require_tenant(request: Request):
    """Dependency: enforce auth AND scope the DB engine to the chosen hotel schema.

    Honors ?hotel=<schema> for users with access to more than one hotel, falling
    back to the session's default. The schema contextvar is reset after the
    request so it never leaks into another task.
    """
    sess = _session(request)
    if not sess or not sess.get("hotels"):
        raise _NeedsLogin()

    allowed = {h["schema"] for h in sess["hotels"]}
    schema = request.query_params.get("hotel") or sess.get("schema")
    if schema not in allowed:
        schema = sess["schema"]

    tok = db._hotel_schema_var.set(schema)
    request.state.schema = schema
    try:
        yield sess
    finally:
        # On the normal path set() and reset() share a context, so this restores
        # the previous value. But when the endpoint RAISES after the yield,
        # FastAPI runs this cleanup via gen.throw() in a different context (sync
        # generator deps run in a threadpool); reset(tok) then raises a spurious
        # ValueError that would mask the real HTTPException as a 500. Swallow it —
        # that other context is discarded with the failed request, so nothing leaks.
        try:
            db._hotel_schema_var.reset(tok)
        except ValueError:
            db._hotel_schema_var.set("")


def _role_for(sess: dict, schema) -> str:
    """Role for the currently-selected hotel (falls back to the session default)."""
    return next((h["role"] for h in sess["hotels"] if h["schema"] == schema), sess.get("role"))


def _ctx(request: Request, sess: dict | None, **extra) -> dict:
    """Common template context."""
    return {
        "request": request,
        "hotel_name": settings.HOTEL_NAME,
        "session": sess,
        "current_schema": getattr(request.state, "schema", None),
        "asset_v": STATIC_VERSION,
        **extra,
    }


# ── Public routes ─────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz():
    return JSONResponse({"status": "ok"})


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request, message: str | None = None):
    if _session(request):
        return RedirectResponse("/", status_code=303)
    auth_url = str(request.url_for("auth_telegram"))
    return templates.TemplateResponse(request, "login.html", _ctx(
        request, None,
        login_bot_username=settings.LOGIN_BOT_USERNAME,
        auth_url=auth_url,
        dev_login=settings.DEV_LOGIN_ID is not None,
        message=message,
    ))


@app.get("/auth/telegram", name="auth_telegram")
async def auth_telegram(request: Request):
    params = dict(request.query_params)
    if not auth.verify_telegram_login(params, settings.LOGIN_BOT_TOKEN):
        return RedirectResponse("/login?message=Login+verification+failed", status_code=303)

    tid = int(params["id"])
    hotels = auth.resolve_access(tid)
    if not hotels:
        return RedirectResponse("/login?message=This+Telegram+account+has+no+hotel+access", status_code=303)

    payload = {
        "tid": tid,
        "username": params.get("username") or params.get("first_name") or str(tid),
        "hotels": hotels,
        "schema": hotels[0]["schema"],
        "role": hotels[0]["role"],
    }
    resp = RedirectResponse("/", status_code=303)
    resp.set_cookie(
        settings.SESSION_COOKIE, auth.serialize_session(payload),
        max_age=settings.SESSION_MAX_AGE, httponly=True,
        samesite="lax", secure=settings.COOKIE_SECURE,
    )
    return resp


@app.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(settings.SESSION_COOKIE)
    return resp


# ── Authenticated routes ──────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request, period: str | None = None, staff: str | None = None,
                         sess: dict = Depends(require_tenant)):
    role = _role_for(sess, request.state.schema)
    view = data.dashboard_view(period, staff=staff)
    return templates.TemplateResponse(request, "dashboard.html", _ctx(
        request, sess, view=view, role=role,
        current_period=(period or ""), current_staff=(staff or ""),
    ))


@app.get("/partial/period", response_class=HTMLResponse)
async def period_partial(request: Request, period: str | None = None, staff: str | None = None,
                         sess: dict = Depends(require_tenant)):
    """Just the period-dependent region (toolbar → records → stock), for the
    client to swap in place without a full page reload. Same data path as `/`."""
    role = _role_for(sess, request.state.schema)
    view = data.dashboard_view(period, staff=staff)
    return templates.TemplateResponse(request, "_period.html", _ctx(
        request, sess, view=view, role=role,
        current_period=(period or ""), current_staff=(staff or ""),
    ))


@app.get("/export/{kind}.csv")
async def export_csv(kind: str, request: Request, period: str | None = None,
                     sess: dict = Depends(require_tenant)):
    # Itemised expenses are admin-only (mirrors the bot: staff see expense totals
    # in /report but not the per-row breakdown in /history).
    if kind == "expenses" and _role_for(sess, request.state.schema) != "admin":
        raise HTTPException(status_code=403, detail="admins only")
    result = data.export_dataset(kind, period)
    if result is None:
        raise HTTPException(status_code=404, detail="unknown export")
    cols, rows = result
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow({c: r.get(c, "") for c in cols})
    fname = f"{request.state.schema}_{kind}_{period or 'month'}.csv"
    return Response(
        buf.getvalue(), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ── Dev-only login (never enabled in production) ──────────────────────

if settings.DEV_LOGIN_ID is not None:
    @app.get("/dev-login")
    async def dev_login():
        hotels = auth.resolve_access(settings.DEV_LOGIN_ID)
        if not hotels:
            return RedirectResponse("/login?message=Dev+id+has+no+access", status_code=303)
        payload = {
            "tid": settings.DEV_LOGIN_ID, "username": "dev",
            "hotels": hotels, "schema": hotels[0]["schema"], "role": hotels[0]["role"],
        }
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie(settings.SESSION_COOKIE, auth.serialize_session(payload),
                        max_age=settings.SESSION_MAX_AGE, httponly=True,
                        samesite="lax", secure=settings.COOKIE_SECURE)
        return resp
