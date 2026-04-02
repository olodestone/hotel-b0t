"""
bot.py — Hotel Management Telegram Bot
=======================================
Entry point. Handles all command routing, input parsing,
role-based access control, and daily report scheduling.

Commands
--------
Public (any registered user):
  /start                                     — Initialize / register
  /sell_drink <drink> <qty> <price>          — Record drink sale
  /restock <drink> <qty> <cost>              — Add inventory
  /room <type> <qty> <price> <nights>        — Record room booking
  /expense <room|bar> <category> <amt> [note]— Record expense under Bar or Rooms account
  /add_debtor <room|bar> <name> <amt> [note] — Log a debtor for Bar or Rooms
  /pay_debtor <room|bar> <name>              — Mark debtor as paid
  /debtors [bar|rooms]                       — List outstanding debtors
  /report [today]                            — Financial report (split Bar / Rooms)
  /stock                                     — Inventory status
  
Admin only:
  /setthreshold <drink> <amount>      — Set low-stock alert threshold
  /addstaff <user_id> <username>      — Grant staff access
  /removestaff <user_id>              — Revoke access
  /dailyreport on|off                 — Toggle scheduled reports
"""
from __future__ import annotations

import logging
import os
from datetime import time as dtime

import pytz
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

import database as db
import inventory as inv
import logic
import reports
from config import (
    ADMIN_IDS,
    BOT_TOKEN,
    DAILY_REPORT_TIME,
    HOTEL_NAME,
    REPORT_CHAT_ID,
    TIMEZONE,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── Access helpers ────────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    user = db.get_user(user_id)
    return user is not None and user.get("role") == "admin"


def _is_authorized(user_id: int) -> bool:
    if _is_admin(user_id):
        return True
    user = db.get_user(user_id)
    return user is not None and user.get("role") in ("staff", "admin")


def _require_auth(fn):
    """Decorator: reject unregistered users."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not _is_authorized(uid):
            await update.message.reply_text(
                "🔒 Access denied. Ask an admin to add you with /addstaff."
            )
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


def _require_admin(fn):
    """Decorator: admin-only commands."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not _is_admin(uid):
            await update.message.reply_text("🔒 This command is admin-only.")
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


# ── Parse helpers ─────────────────────────────────────────────────────

def _parse_args(ctx: ContextTypes.DEFAULT_TYPE) -> list[str]:
    return ctx.args or []


def _to_int(value: str, label: str) -> tuple[int | None, str]:
    try:
        v = int(value)
        if v <= 0:
            raise ValueError
        return v, ""
    except ValueError:
        return None, f"❌ *{label}* must be a positive whole number. Got: `{value}`"


def _to_float(value: str, label: str) -> tuple[float | None, str]:
    try:
        v = float(value)
        if v <= 0:
            raise ValueError
        return v, ""
    except ValueError:
        return None, f"❌ *{label}* must be a positive number. Got: `{value}`"


async def _reply(update: Update, text: str) -> None:
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ── /start ────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    uid = user.id
    username = user.username or user.first_name or str(uid)

    already = db.get_user(uid)
    if already:
        role = already.get("role", "staff")
        await _reply(update, f"👋 Welcome back, *{username}*! Role: _{role}_")
        return

    # First-ever user becomes admin if no ADMIN_IDS configured
    if not ADMIN_IDS and not db.read_all("users"):
        db.upsert_user(uid, username, role="admin")
        await _reply(
            update,
            f"🏨 *{HOTEL_NAME}* Bot\n\n"
            f"Welcome, *{username}*! You've been registered as *admin* "
            f"(first user, no ADMIN_IDS set).\n\n"
            + _help_text(),
        )
    elif _is_admin(uid):
        db.upsert_user(uid, username, role="admin")
        await _reply(update, f"✅ Registered as *admin*, {username}.")
    else:
        await _reply(
            update,
            f"🔒 Hi *{username}*! Your ID is `{uid}`.\n"
            f"Ask an admin to run: `/addstaff {uid} {username}`",
        )


def _help_text() -> str:
    return (
        "*Commands:*\n"
        "`/sell_drink <drink> <qty> <price>`\n"
        "`/restock <drink> <qty> <cost_price>`\n"
        "`/room <type> <qty> <price> <nights>`\n"
        "`/expense <room|bar> <category> <amount> [note]`\n"
        "`/add_debtor <room|bar> <name> <amount> [note]`\n"
        "`/pay_debtor <room|bar> <name>`\n"
        "`/debtors` or `/debtors bar` or `/debtors rooms`\n"
        "`/report` or `/report today`\n"
        "`/stock`\n\n"
        "*Admin only:*\n"
        "`/setthreshold <drink> <amount>`\n"
        "`/addstaff <user_id> <username>`\n"
        "`/removestaff <user_id>`\n"
        "`/dailyreport on|off`"
    )


# ── /sell_drink ───────────────────────────────────────────────────────

@_require_auth
async def cmd_sell_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(update, "Usage: `/sell_drink <drink> <qty> <price>`\nExample: `/sell_drink heineken 6 500`")
        return

    drink = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return
    price, err = _to_float(args[2], "price")
    if err:
        await _reply(update, err)
        return

    ok, msg = logic.process_drink_sale(drink, qty, price)
    await _reply(update, msg)


# ── /restock ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_restock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(update, "Usage: `/restock <drink> <qty> <cost_price>`\nExample: `/restock heineken 24 300`")
        return

    drink = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return
    cost, err = _to_float(args[2], "cost_price")
    if err:
        await _reply(update, err)
        return

    ok, msg = logic.process_restock(drink, qty, cost)
    await _reply(update, msg)


# ── /room ─────────────────────────────────────────────────────────────

@_require_auth
async def cmd_room(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 4:
        await _reply(
            update,
            "Usage: `/room <type> <qty> <price> <nights>`\n"
            "Example: `/room standard 2 15000 3`",
        )
        return

    room_type = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return
    price, err = _to_float(args[2], "price")
    if err:
        await _reply(update, err)
        return
    nights, err = _to_int(args[3], "nights")
    if err:
        await _reply(update, err)
        return

    ok, msg = logic.process_room_sale(room_type, qty, price, nights)
    await _reply(update, msg)


# ── /expense ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_expense(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/expense <room|bar> <category> <amount> [note]`\n"
            "Example: `/expense bar cleaning 5000`\n"
            "Example: `/expense rooms maintenance 12000 generator repair`",
        )
        return

    account = args[0]
    category = args[1]
    amount, err = _to_float(args[2], "amount")
    if err:
        await _reply(update, err)
        return
    description = " ".join(args[3:]) if len(args) > 3 else ""

    ok, msg = logic.process_expense(account, category, amount, description)
    await _reply(update, msg)


# ── /add_debtor ───────────────────────────────────────────────────────

@_require_auth
async def cmd_add_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/add_debtor <room|bar> <name> <amount> [note]`\n"
            "Example: `/add_debtor bar john 2500`\n"
            "Example: `/add_debtor rooms emeka 45000 room 12 unpaid`",
        )
        return

    account = args[0]
    name = args[1]
    amount, err = _to_float(args[2], "amount")
    if err:
        await _reply(update, err)
        return
    description = " ".join(args[3:]) if len(args) > 3 else ""

    ok, msg = logic.process_add_debtor(account, name, amount, description)
    await _reply(update, msg)


# ── /pay_debtor ───────────────────────────────────────────────────────

@_require_auth
async def cmd_pay_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/pay_debtor <room|bar> <name>`\n"
            "Example: `/pay_debtor bar john`\n"
            "Example: `/pay_debtor rooms emeka`",
        )
        return

    account = args[0]
    name = " ".join(args[1:])

    ok, msg = logic.process_pay_debtor(account, name)
    await _reply(update, msg)


# ── /debtors ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_debtors(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    account = args[0].lower() if args else None
    if account and account not in ("bar", "rooms"):
        await _reply(update, "Usage: `/debtors` or `/debtors bar` or `/debtors rooms`")
        return
    text = reports.generate_debtors_report(account=account)
    await _reply(update, text)


# ── /report ───────────────────────────────────────────────────────────

@_require_auth
async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    from datetime import datetime
    for_date = None
    if args and args[0].lower() == "today":
        for_date = datetime.now().date()

    text = reports.generate_full_report(for_date=for_date)
    await _reply(update, text)


# ── /stock ────────────────────────────────────────────────────────────

@_require_auth
async def cmd_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = reports.generate_stock_report()
    await _reply(update, text)


# ── /setthreshold (admin) ─────────────────────────────────────────────

@_require_admin
async def cmd_setthreshold(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(update, "Usage: `/setthreshold <drink> <threshold>`\nExample: `/setthreshold heineken 10`")
        return

    drink = args[0]
    threshold, err = _to_int(args[1], "threshold")
    if err:
        await _reply(update, err)
        return

    result = inv.set_threshold(drink, threshold)
    await _reply(update, result.message)


# ── /addstaff (admin) ─────────────────────────────────────────────────

@_require_admin
async def cmd_addstaff(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(update, "Usage: `/addstaff <user_id> <username>`")
        return

    uid_str, username = args[0], args[1]
    try:
        uid = int(uid_str)
    except ValueError:
        await _reply(update, "❌ user_id must be a number.")
        return

    db.upsert_user(uid, username, role="staff")
    await _reply(update, f"✅ *{username}* (ID: `{uid}`) added as *staff*.")


# ── /removestaff (admin) ──────────────────────────────────────────────

@_require_admin
async def cmd_removestaff(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(update, "Usage: `/removestaff <user_id>`")
        return

    try:
        uid = int(args[0])
    except ValueError:
        await _reply(update, "❌ user_id must be a number.")
        return

    rows = db.read_all("users")
    new_rows = [r for r in rows if int(r["user_id"]) != uid]
    if len(new_rows) == len(rows):
        await _reply(update, f"❌ User `{uid}` not found.")
        return
    db.write_all("users", new_rows)
    await _reply(update, f"✅ User `{uid}` removed.")


# ── /dailyreport (admin) ──────────────────────────────────────────────

@_require_admin
async def cmd_dailyreport(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args or args[0].lower() not in ("on", "off"):
        await _reply(update, "Usage: `/dailyreport on|off`")
        return

    action = args[0].lower()
    job_name = "daily_report"
    current_jobs = ctx.job_queue.get_jobs_by_name(job_name)

    if action == "off":
        for job in current_jobs:
            job.schedule_removal()
        await _reply(update, "🔕 Daily report *disabled*.")
        return

    # on: schedule if not already running
    if current_jobs:
        await _reply(update, "✅ Daily report is already *enabled*.")
        return

    chat_id = REPORT_CHAT_ID or update.effective_chat.id
    _schedule_daily_report(ctx.job_queue, chat_id)
    await _reply(update, f"✅ Daily report *enabled* — sends at {DAILY_REPORT_TIME} ({TIMEZONE}).")


# ── Scheduled job ─────────────────────────────────────────────────────

async def _send_daily_report(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = ctx.job.data
    text = reports.generate_daily_report()
    await ctx.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
    logger.info("Daily report sent to chat_id=%s", chat_id)


def _schedule_daily_report(job_queue, chat_id: int) -> None:
    hour, minute = [int(x) for x in DAILY_REPORT_TIME.split(":")]
    tz = pytz.timezone(TIMEZONE)
    job_queue.run_daily(
        _send_daily_report,
        time=dtime(hour=hour, minute=minute, tzinfo=tz),
        name="daily_report",
        data=chat_id,
    )
    logger.info("Daily report scheduled at %s %s for chat_id=%s", DAILY_REPORT_TIME, TIMEZONE, chat_id)


# ── Error handler ─────────────────────────────────────────────────────

async def _error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", ctx.error, exc_info=ctx.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("⚠️ An unexpected error occurred. Please try again.")


# ── Main ──────────────────────────────────────────────────────────────

def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is not set.")

    db.init_db()
    logger.info("Database initialised. Hotel: %s", HOTEL_NAME)

    app = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("sell_drink", cmd_sell_drink))
    app.add_handler(CommandHandler("restock", cmd_restock))
    app.add_handler(CommandHandler("room", cmd_room))
    app.add_handler(CommandHandler("expense", cmd_expense))
    app.add_handler(CommandHandler("add_debtor", cmd_add_debtor))
    app.add_handler(CommandHandler("pay_debtor", cmd_pay_debtor))
    app.add_handler(CommandHandler("debtors", cmd_debtors))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("stock", cmd_stock))
    app.add_handler(CommandHandler("setthreshold", cmd_setthreshold))
    app.add_handler(CommandHandler("addstaff", cmd_addstaff))
    app.add_handler(CommandHandler("removestaff", cmd_removestaff))
    app.add_handler(CommandHandler("dailyreport", cmd_dailyreport))
    app.add_error_handler(_error_handler)

    # Auto-schedule daily report if REPORT_CHAT_ID is configured
    if REPORT_CHAT_ID:
        _schedule_daily_report(app.job_queue, REPORT_CHAT_ID)

    logger.info("Bot starting (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
