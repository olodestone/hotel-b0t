"""
bot.py — Hotel Management Telegram Bot
=======================================
Entry point. Handles all command routing, input parsing,
role-based access control, and daily report scheduling.

Commands
--------
Staff (any registered user):
  /start                                     — Initialize / register
  /sell_drink <drink> <qty>                  — Record drink sale (price from /setprice)
  /room <type> <qty> <price> <nights>        — Record room booking
  /debtors [bar|rooms]                       — List outstanding debtors
  /report [today|YYYY-MM-DD|YYYY-MM|all]     — Financial report
  /stock                                     — Inventory status
  /summary [YYYY-MM-DD]                      — Today's key numbers
  /history [YYYY-MM-DD]                      — View entries for a date

Admin only:
  /expense <room|bar> <category> <amt> [note]— Record expense
  /add_debtor <room|bar> <name> <amt> [note] — Log a debtor
  /pay_debtor <room|bar> <name>              — Mark debtor as paid
  /restock <drink> <qty> <cost>              — Add inventory
  /transfer <drink> <qty>                    — Move store → bar
  /delete <sale|room|expense> <id>           — Remove an entry
  /sales_report [today|YYYY-MM-DD|YYYY-MM|all] — Sales breakdown with cost & profit
  /expense_report [today|YYYY-MM-DD|YYYY-MM|all] — Expense breakdown by category
  /staff_report [today|YYYY-MM-DD|YYYY-MM]   — Sales per staff member
  /setthreshold <drink> <amount>             — Set low-stock alert threshold
  /addstaff <user_id> <username>             — Grant staff access
  /removestaff <user_id>                     — Revoke access
  /dailyreport on|off                        — Toggle scheduled reports
  /export                                    — Download full hotel data as Excel file
"""
from __future__ import annotations

import asyncio
import calendar as _cal
import contextvars
import io
import logging
import os
import re
import signal
from datetime import date, timedelta
from datetime import time as dtime

import pytz
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    ExtBot,
    MessageHandler,
    TypeHandler,
    filters,
)
from telegram.request import HTTPXRequest

import database as db
import inventory as inv
import logic
import reports
from config import (
    ADMIN_IDS,
    BOT_TOKEN,
    DAILY_REPORT_TIME,
    HOTEL_NAME,
    HOTEL_SCHEMA,
    OWNER_ID,
    REPORT_CHAT_ID,
    TIMEZONE,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class _MD2Bot(ExtBot):
    """Bot subclass that auto-escapes outgoing text for MarkdownV2.

    All send paths (reply_text, edit_message_text, ctx.bot.send_message, …)
    funnel through Bot.send_message / Bot.edit_message_text, so escaping here
    is the single chokepoint: every message sent with parse_mode MARKDOWN_V2 is
    run through reports.escape_markdown_v2() exactly once. Handlers can keep
    writing plain markup (``*bold*``, `` `code` ``) without escaping by hand.
    """

    @staticmethod
    def _escape(args: tuple, kwargs: dict, text_index: int | None) -> tuple:
        if kwargs.get("parse_mode") != ParseMode.MARKDOWN_V2:
            return args
        # The message body arrives as "text" (or "caption" for media) — escape
        # whichever is present. Fall back to a positional text arg if needed.
        if isinstance(kwargs.get("text"), str):
            kwargs["text"] = reports.escape_markdown_v2(kwargs["text"])
        elif isinstance(kwargs.get("caption"), str):
            kwargs["caption"] = reports.escape_markdown_v2(kwargs["caption"])
        elif text_index is not None and len(args) > text_index and isinstance(args[text_index], str):
            args = list(args)
            args[text_index] = reports.escape_markdown_v2(args[text_index])
            args = tuple(args)
        return args

    async def send_message(self, *args, **kwargs):  # chat_id is positional 0, text is 1
        args = self._escape(args, kwargs, text_index=1)
        return await super().send_message(*args, **kwargs)

    async def edit_message_text(self, *args, **kwargs):  # text is positional 0
        args = self._escape(args, kwargs, text_index=0)
        return await super().edit_message_text(*args, **kwargs)

    async def send_document(self, *args, **kwargs):  # caption escaped via kwargs
        args = self._escape(args, kwargs, text_index=None)
        return await super().send_document(*args, **kwargs)


# Per-update context variable — set by each bot's schema-setter so _is_admin and
# decorators use the right admin list without any handler code changes.
_active_admin_ids: contextvars.ContextVar[list[int]] = contextvars.ContextVar("admin_ids", default=[])

# Populated in main() — maps schema → Application so broadcast can reach all hotels' bots.
_hotel_apps: dict[str, "Application"] = {}


MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["🍺 Sell", "🛏 Book Room"], ["💰 Prices", "📦 Stock"], ["📜 History", "💳 Debtors"]],
    resize_keyboard=True,
)
ADMIN_KEYBOARD = ReplyKeyboardMarkup(
    [["📊 Summary", "📋 Report"], ["💰 Prices", "📦 Stock"], ["📜 History", "💳 Debtors"], ["⚙️ Manage"]],
    resize_keyboard=True,
)


def _get_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ADMIN_KEYBOARD if _is_admin(user_id) else MAIN_KEYBOARD

# ── Access helpers ────────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    ids = _active_admin_ids.get() or ADMIN_IDS
    if user_id in ids:
        return True
    user = db.get_user(user_id)
    return user is not None and user.get("role") == "admin"


def _is_authorized(user_id: int) -> bool:
    if _is_admin(user_id):
        return True
    user = db.get_user(user_id)
    return user is not None and user.get("role") in ("staff", "admin")


async def _access_denied(update: Update, msg: str) -> None:
    """Send an access-denied message regardless of update type."""
    if update.callback_query:
        await update.callback_query.answer(msg, show_alert=True)
    elif update.message:
        await update.message.reply_text(msg)


def _require_auth(fn):
    """Decorator: reject unregistered users."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not _is_authorized(uid):
            await _access_denied(update, "🔒 Access denied. Ask an admin to add you with /addstaff.")
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


def _require_admin(fn):
    """Decorator: admin-only commands."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not _is_admin(uid):
            await _access_denied(update, "🔒 This command is admin-only.")
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


def _require_owner(fn):
    """Decorator: app-owner only — checks OWNER_ID env var exclusively.
    Neither hotel admins nor ADMIN_IDS members can bypass this."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if OWNER_ID is None or uid != OWNER_ID:
            await update.message.reply_text("🔒 This command is restricted to the service owner.")
            return
        return await fn(update, ctx)
    wrapper.__name__ = fn.__name__
    return wrapper


# ── Parse helpers ─────────────────────────────────────────────────────

def _parse_args(ctx: ContextTypes.DEFAULT_TYPE) -> list[str]:
    return ctx.args or []


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _extract_date(args: list[str]) -> tuple[list[str], str | None]:
    """If the last arg is YYYY-MM-DD, pop it off and return (remaining_args, date_str)."""
    if args and _DATE_RE.match(args[-1]):
        return args[:-1], args[-1]
    return args, None


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
    """Send a reply, falling back to plain text if MarkdownV2 parsing fails."""
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception:
        await update.message.reply_text(text)


async def _send_chunk_cb(q, text: str) -> None:
    """Send one chunk from a callback context, falling back to plain text if Markdown fails."""
    try:
        await q.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception:
        await q.message.reply_text(text)


async def _reply_long_cb(q, text: str) -> None:
    """Send a long reply from a CallbackQuery context."""
    limit = 4000
    if len(text) <= limit:
        await _send_chunk_cb(q, text)
        return
    lines = text.split("\n")
    chunk: list[str] = []
    size = 0
    for line in lines:
        if size + len(line) + 1 > limit and chunk:
            await _send_chunk_cb(q, "\n".join(chunk))
            chunk = []
            size = 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        await _send_chunk_cb(q, "\n".join(chunk))


async def _send_chunk(update: Update, text: str) -> None:
    """Send one chunk, falling back to plain text if Markdown parsing fails."""
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception:
        await update.message.reply_text(text)


async def _reply_long(update: Update, text: str) -> None:
    """Send a message, splitting into ≤4000-char chunks at line boundaries if needed."""
    limit = 4000
    if len(text) <= limit:
        await _send_chunk(update, text)
        return
    lines = text.split("\n")
    chunk: list[str] = []
    size = 0
    for line in lines:
        # +1 for the newline we'll re-add
        if size + len(line) + 1 > limit and chunk:
            await _send_chunk(update, "\n".join(chunk))
            chunk = []
            size = 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        await _send_chunk(update, "\n".join(chunk))


# ── /start ────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    uid = user.id
    username = user.username or user.first_name or str(uid)
    logger.info("cmd_start received from uid=%s username=%s", uid, username)

    already = db.get_user(uid)
    if already:
        role = already.get("role", "staff")
        await update.message.reply_text(
            f"👋 Welcome back, *{reports._esc(username)}*! Role: _{role}_",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_get_keyboard(uid),
        )
        return

    # Only the pre-approved admin ID (set via add_hotel.py --admin-id) gets admin access.
    # Nobody can self-promote — random users are always locked out.
    if _is_admin(uid):
        db.upsert_user(uid, username, role="admin")
        hotel_name = db.get_setting("hotel_name") or HOTEL_NAME
        await _reply(
            update,
            f"🏨 *{reports._esc(hotel_name)}* Bot\n\n"
            f"Welcome, *{reports._esc(username)}*! You're registered as *admin*.\n\n"
            + _help_text(is_admin=True),
        )
    else:
        await _reply(
            update,
            f"🔒 Hi *{reports._esc(username)}*! Your ID is `{uid}`.\n"
            f"Ask an admin to run: `/addstaff {uid} {username}`",
        )


def _help_text(is_admin: bool = False) -> str:
    staff_cmds = (
        "*📌 Recording*\n"
        "`/sell` — 🍺 tap-to-sell guided flow _(easiest)_\n"
        "`/book` — 🛏 tap-to-book room guided flow _(easiest)_\n"
        "`/sell_drink <drink> <qty>` — quick text sale\n"
        "`/room <type> <qty> <nights>` — quick text room booking\n"
        "`/undo` — undo your last entry within 2 minutes\n"
        "`/cancel` — cancel a guided flow\n"
        "\n"
        "*📊 Viewing*\n"
        "`/today` — quick shift snapshot _(revenue, top sellers, debtors, stock)_\n"
        "`/stock` — bar stock levels\n"
        "`/prices` — drink & room type prices\n"
        "`/debtors` | `/debtors bar` | `/debtors rooms` | `/debtors YYYY-MM`\n"
        "`/debtor <name>` — all debts for one person\n"
        "`/debtor_history <bar|rooms> <name>` — full payment timeline\n"
        "`/debtor_staff <staff>` — all debts under a staff member\n"
        "`/history` | `/history YYYY-MM-DD` — daily entry log\n"
        "`/summary` | `/summary YYYY-MM-DD` — detailed daily overview\n"
        "\n"
        "_Tip: add a date to backdate any recording — e.g._ `/sell_drink heineken 3 2026-04-29`"
    )
    if not is_admin:
        return staff_cmds
    admin_cmds = (
        "\n\n*🔧 Admin Commands*\n"
        "`/expense <room|bar> <category> <amount> [note] [YYYY-MM-DD]`\n"
        "`/add_debtor <room|bar> <name> <amount> [note] [by:<staff>] [YYYY-MM-DD]`\n"
        "`/pay_debtor <room|bar> <name> [amount]`\n"
        "`/pay_debt <id> [amount]` — pay by debt ID\n"
        "`/set_debt_staff <id> <staff>` — assign staff to a debt\n"
        "`/restock <drink> <qty> <cost_price>`\n"
        "`/restock_plan` — weekly restock advisor _(transfers, reorder tiers, budget)_\n"
        "`/transfer <drink> <qty>` — move store → bar\n"
        "`/renamedrink <old> <new>` — rename, or merge duplicate SKUs\n"
        "`/setstock <drink> <store> <bar>` — correct stock counts\n"
        "`/deletedrink <drink>` — remove a zero-stock drink\n"
        "`/delete <sale|room|expense> <id>`\n"
        "`/setprice <drink> <price>`\n"
        "`/setroomtype <type> <price>` — set room type preset price\n"
        "`/report` | `/report today` | `/report YYYY-MM` | `/report all`\n"
        "`/sales_report` | `/expense_report` | `/staff_report` | `/allocation`\n"
        "`/setallocation <key> <percent>`\n"
        "`/setthreshold <drink> <amount>`\n"
        "`/addstaff <user_id> <username>` | `/removestaff <user_id>`\n"
        "`/dailyreport on|off`\n"
        "`/activity` | `/activity YYYY-MM-DD` | `/activity username`\n"
        "`/export` — download full hotel data as Excel file"
    )
    return staff_cmds + admin_cmds


# ── /help ────────────────────────────────────────────────────────────

@_require_auth
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(update, _help_text(is_admin=_is_admin(update.effective_user.id)))


# ── /sell_drink ───────────────────────────────────────────────────────

@_require_auth
async def cmd_sell_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 2:
        await _reply(update, "Usage: `/sell_drink <drink> <qty> [YYYY-MM-DD]`\nExample: `/sell_drink heineken 6`\nBackdate: `/sell_drink heineken 6 2025-03-15`")
        return

    drink = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg, alert = logic.process_drink_sale(drink, qty, timestamp=ts, recorded_by=recorded_by)
    if ts and ok:
        msg += f"\n_(recorded for {ts})_"
    await _reply(update, msg)

    # Proactive low-stock alert — push to all admins separately
    if ok and alert:
        for admin_id in ADMIN_IDS:
            if admin_id != user.id:
                try:
                    await ctx.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ *Low Stock Alert*\n_{recorded_by} just sold {qty}× {drink}_\n\n{alert}",
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception:
                    pass


# ── /setprice (admin) ────────────────────────────────────────────────

@_require_admin
async def cmd_setprice(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/setprice <drink> <price>`\n"
            "Example: `/setprice heineken 500`\n"
            "Multi-word: `/setprice club soda 300`",
        )
        return
    # Last arg is price; everything before is the drink name (supports multi-word)
    price, err = _to_float(args[-1], "price")
    if err:
        await _reply(update, err)
        return
    drink = " ".join(args[:-1])
    ok, msg = logic.process_set_price(drink, price)
    await _reply(update, msg)


# ── /setroomtype (admin) ──────────────────────────────────────────────

@_require_admin
async def cmd_setroomtype(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/setroomtype <type> <price_per_night>`\n"
            "Example: `/setroomtype standard 15000`\n"
            "Example: `/setroomtype executive 25000`\n"
            "Staff can then book with: `/room standard 1 3` _(qty nights)_",
        )
        return
    price, err = _to_float(args[-1], "price")
    if err:
        await _reply(update, err)
        return
    room_type = " ".join(args[:-1])
    db.set_room_type_price(room_type, price)
    await _reply(update, f"✅ *{reports._esc(room_type.title())}* preset set to ₦{price:,.0f}/night.\nStaff can now use: `/room {room_type.lower()} <qty> <nights>`")


# ── /prices ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_prices(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = reports.generate_price_list()
    await _reply_long(update, text)


# ── /undo ─────────────────────────────────────────────────────────────

@_require_auth
async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_undo(recorded_by)
    await _reply(update, msg)

    # Notify all admins of every undo — full audit trail
    if ok:
        for admin_id in ADMIN_IDS:
            if admin_id != user.id:
                try:
                    await ctx.bot.send_message(
                        chat_id=admin_id,
                        text=f"↩️ *Undo Alert*\n@{reports._esc(recorded_by)} reversed an entry:\n_{reports._esc(msg)}_",
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception:
                    pass


# ── /restock ──────────────────────────────────────────────────────────

@_require_admin
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

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_restock(drink, qty, cost, recorded_by=recorded_by)
    await _reply(update, msg)


# ── /room ─────────────────────────────────────────────────────────────

@_require_auth
async def cmd_room(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/room <type> <qty> <nights> [YYYY-MM-DD]`  _(uses preset price)_\n"
            "Or:    `/room <type> <qty> <price> <nights> [YYYY-MM-DD]`\n"
            "Example: `/room standard 1 3`\n"
            "Example: `/room standard 1 12000 3`  _(negotiated price)_\n"
            "Set room prices: `/setroomtype standard 15000`",
        )
        return

    room_type = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return

    if len(args) == 3:
        # /room <type> <qty> <nights> — use preset price
        nights, err = _to_int(args[2], "nights")
        if err:
            await _reply(update, err)
            return
        price = db.get_room_type_price(room_type)
        if price is None:
            await _reply(
                update,
                f"❌ No preset price for room type *{reports._esc(room_type)}*.\n"
                f"Set one with `/setroomtype {room_type} <price>` or provide price:\n"
                f"`/room {room_type} {qty} <price> {nights}`",
            )
            return
        price_note = f" _(preset price ₦{price:,.0f})_"
    else:
        # /room <type> <qty> <price> <nights>
        price, err = _to_float(args[2], "price")
        if err:
            await _reply(update, err)
            return
        nights, err = _to_int(args[3], "nights")
        if err:
            await _reply(update, err)
            return
        price_note = ""

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_room_sale(room_type, qty, price, nights, timestamp=ts, recorded_by=recorded_by)
    if ok and price_note:
        msg += f"\n{price_note}"
    await _reply(update, msg)


# ── /expense ──────────────────────────────────────────────────────────

@_require_admin
async def cmd_expense(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/expense <room|bar> <category> <amount> [note] [YYYY-MM-DD]`\n"
            "Example: `/expense bar cleaning 5000`\n"
            "Example: `/expense rooms maintenance 12000 generator repair`\n"
            "Backdate: `/expense bar cleaning 5000 2025-03-20`",
        )
        return

    account = args[0]
    category = args[1]
    amount, err = _to_float(args[2], "amount")
    if err:
        await _reply(update, err)
        return
    description = " ".join(args[3:]) if len(args) > 3 else ""

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_expense(account, category, amount, description, timestamp=ts, recorded_by=recorded_by)
    await _reply(update, msg)


# ── /add_debtor ───────────────────────────────────────────────────────

@_require_admin
async def cmd_add_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/add_debtor <room|bar> <name> <amount> [note] [by:<staff>] [YYYY-MM-DD]`\n"
            "Example: `/add_debtor bar john 2500`\n"
            "Example: `/add_debtor bar yussuf 2500 tab by:bola`\n"
            "Example: `/add_debtor rooms emeka 45000 room 12 unpaid by:tunde`\n"
            "Backdate: `/add_debtor bar john 2500 2025-03-15`",
        )
        return

    account = args[0]
    name = args[1]
    amount, err = _to_float(args[2], "amount")
    if err:
        await _reply(update, err)
        return

    rest = args[3:]
    staff_name = ""
    filtered = []
    for token in rest:
        if token.lower().startswith("by:") and len(token) > 3:
            staff_name = token[3:]
        else:
            filtered.append(token)
    description = " ".join(filtered)

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_add_debtor(account, name, amount, description, timestamp=ts, recorded_by=recorded_by, staff_name=staff_name)
    await _reply(update, msg)


# ── /pay_debtor ───────────────────────────────────────────────────────

@_require_admin
async def cmd_pay_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/pay_debtor <room|bar> <name> [amount]`\n"
            "Full payment:    `/pay_debtor bar john`\n"
            "Partial payment: `/pay_debtor bar john 5000`\n"
            "Example rooms:   `/pay_debtor rooms emeka 10000`",
        )
        return

    account = args[0]
    amount: float | None = None

    # If last arg is a number, treat it as the payment amount
    if len(args) >= 3:
        try:
            parsed = float(args[-1])
            if parsed > 0:
                amount = parsed
                name = " ".join(args[1:-1])
            else:
                name = " ".join(args[1:])
        except ValueError:
            name = " ".join(args[1:])
    else:
        name = args[1]

    user = update.effective_user
    paid_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_pay_debtor(account, name, paid_by=paid_by, amount=amount)
    await _reply(update, msg)


# ── /pay_debt ─────────────────────────────────────────────────────────

@_require_admin
async def cmd_pay_debt(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(
            update,
            "Usage: `/pay_debt <id> [amount]`\n"
            "Full payment:    `/pay_debt 14`\n"
            "Partial payment: `/pay_debt 14 5000`\n"
            "_Get debt IDs from /debtors_",
        )
        return

    try:
        debt_id = int(args[0])
    except ValueError:
        await _reply(update, "❌ Debt ID must be a number. Get IDs from /debtors.")
        return

    amount: float | None = None
    if len(args) >= 2:
        amount, err = _to_float(args[1], "amount")
        if err:
            await _reply(update, err)
            return

    user = update.effective_user
    paid_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_pay_debt_by_id(debt_id, paid_by=paid_by, amount=amount)
    await _reply(update, msg)


# ── /debtor_staff ─────────────────────────────────────────────────────

@_require_auth
async def cmd_debtor_staff(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(
            update,
            "Usage: `/debtor_staff <staff name>`\n"
            "Example: `/debtor_staff bola`\n"
            "Shows all outstanding debts attributed to that staff member.",
        )
        return
    staff_name = " ".join(args)
    text = reports.generate_staff_debtors(staff_name)
    await _reply_long(update, text)


# ── /set_debt_staff ───────────────────────────────────────────────────

@_require_admin
async def cmd_set_debt_staff(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/set_debt_staff <id> <staff name>`\n"
            "Example: `/set_debt_staff 12 bola`\n"
            "_Get debt IDs from /debtors_",
        )
        return

    try:
        debt_id = int(args[0])
    except ValueError:
        await _reply(update, "❌ Debt ID must be a number. Get IDs from /debtors.")
        return

    staff_name = " ".join(args[1:])
    updated = db.update_debt_staff_name(debt_id, staff_name)
    if updated:
        await _reply(update, f"✅ Debt `#{debt_id}` — staff set to *{reports._esc(staff_name.title())}*.")
    else:
        await _reply(update, f"❌ No debt found with ID `#{debt_id}`.")


# ── /debtor_history ───────────────────────────────────────────────────

@_require_auth
async def cmd_debtor_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/debtor_history <bar|rooms> <name>`\n"
            "Example: `/debtor_history bar john`",
        )
        return
    account = args[0].lower()
    if account not in ("bar", "rooms"):
        await _reply(update, "❌ Account must be `bar` or `rooms`.")
        return
    name = " ".join(args[1:])
    text = reports.generate_debtor_history(account, name)
    await _reply_long(update, text)


# ── /debtor <name> ────────────────────────────────────────────────────

@_require_auth
async def cmd_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(
            update,
            "Usage: `/debtor <name>`\n"
            "Example: `/debtor john`\n"
            "Shows all outstanding debts for that person across bar and rooms.",
        )
        return
    name = " ".join(args)
    text = reports.generate_debtor_lookup(name)
    await _reply_long(update, text)


# ── /debtors ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_debtors(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    account: str | None = None
    month: str | None = None
    for arg in args:
        if re.match(r"^\d{4}-\d{2}$", arg):
            month = arg
        elif arg.lower() in ("bar", "rooms"):
            account = arg.lower()
        else:
            await _reply(update, "Usage: `/debtors` | `/debtors bar` | `/debtors rooms` | `/debtors 2026-04` | `/debtors bar 2026-04`")
            return
    staff_view = not _is_admin(update.effective_user.id)
    text = reports.generate_debtors_report(account=account, staff_view=staff_view, month=month)
    await _reply_long(update, text)


# ── /report ───────────────────────────────────────────────────────────

@_require_admin
async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""
    staff_view = not _is_admin(update.effective_user.id)

    if not arg:
        now = datetime.now()
        text = reports.generate_full_report(for_month=(now.year, now.month), staff_view=staff_view)
    elif arg == "today":
        text = reports.generate_full_report(for_date=datetime.now().date(), staff_view=staff_view)
    elif arg == "all":
        text = reports.generate_full_report(all_time=True, staff_view=staff_view)
    else:
        # Try YYYY-MM-DD first, then YYYY-MM
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_full_report(for_date=dt.date(), staff_view=staff_view)
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_full_report(for_month=(dt.year, dt.month), staff_view=staff_view)
            except ValueError:
                await _reply(update, "Usage: `/report` | `/report today` | `/report 2025-04-01` | `/report 2025-03` | `/report all`")
                return

    await _reply_long(update, text)


# ── /history ─────────────────────────────────────────────────────────

@_require_auth
async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime, timedelta
    args = _parse_args(ctx)
    if args and _DATE_RE.match(args[0]):
        date_str = args[0]
    else:
        # Default: yesterday (entries are typically entered the next morning)
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    entries = db.get_entries_by_date(date_str)
    label = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")

    if not entries:
        await _reply(update, f"📋 No entries found for *{label}*.")
        return

    is_admin = _is_admin(update.effective_user.id)
    sales = [e for e in entries if e["entry_type"] == "sale"]
    rooms = [e for e in entries if e["entry_type"] == "room"]
    expenses = [e for e in entries if e["entry_type"] == "expense"]

    lines = [f"📋 *Entries for {label}*", "─" * 30]

    if sales:
        lines.append("🍺 *Sales*")
        for e in sales:
            lines.append(
                f"  `[{e['id']}]` {e['drink_name'].title()} ×{e['quantity']} "
                f"@ ₦{float(e['selling_price']):,.2f} = ₦{float(e['total_revenue']):,.2f}"
            )

    if rooms:
        lines.append("🛏 *Rooms*")
        for e in rooms:
            lines.append(
                f"  `[{e['id']}]` {e['room_type'].title()} ×{e['quantity']} "
                f"@ ₦{float(e['price_per_night']):,.2f} ×{e['nights']} nights "
                f"= ₦{float(e['total_revenue']):,.2f}"
            )

    if expenses and is_admin:
        lines.append("💸 *Expenses*")
        for e in expenses:
            note = f" — {e['description']}" if e.get("description") else ""
            lines.append(
                f"  `[{e['id']}]` {e['account'].title()} › {e['category'].title()} "
                f"₦{float(e['amount']):,.2f}{note}"
            )

    if is_admin:
        lines.append("")
        lines.append("_Use_ `/delete <sale|room|expense> <id>` _to remove an entry._")
    await _reply_long(update, "\n".join(lines))


# ── /delete ───────────────────────────────────────────────────────────

@_require_admin
async def cmd_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(update, "Usage: `/delete <sale|room|expense> <id>`\nExample: `/delete sale 12`")
        return

    entry_type = args[0].lower()
    try:
        entry_id = int(args[1])
    except ValueError:
        await _reply(update, "❌ ID must be a number.")
        return

    user = update.effective_user
    actor = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_delete(entry_type, entry_id, actor=actor)
    await _reply(update, msg)


# ── /stock ────────────────────────────────────────────────────────────

@_require_auth
async def cmd_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    is_admin = _is_admin(update.effective_user.id)
    text = reports.generate_stock_report(staff_view=not is_admin)
    await _reply_long(update, text)


# ── /restock_plan ─────────────────────────────────────────────────────

@_require_admin
async def cmd_restock_plan(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""

    if not arg:
        now = datetime.now()
        text = reports.generate_restock_plan(for_month=(now.year, now.month))
    elif arg == "today":
        text = reports.generate_restock_plan(for_date=datetime.now().date())
    elif arg == "all":
        text = reports.generate_restock_plan(all_time=True)
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_restock_plan(for_date=dt.date())
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_restock_plan(for_month=(dt.year, dt.month))
            except ValueError:
                await _reply(update, "Usage: `/restock_plan` | `/restock_plan today` | `/restock_plan YYYY-MM-DD` | `/restock_plan YYYY-MM` | `/restock_plan all`")
                return
    await _reply_long(update, text)


# ── Inventory corrections (/renamedrink, /setstock, /deletedrink) ─────

@_require_admin
async def cmd_renamedrink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(update,
            "Usage: `/renamedrink <old> <new>`\n"
            "Example: `/renamedrink coca-cola cocacola`\n"
            "_If <new> already exists, the two are merged into one._")
        return
    ok, msg = logic.process_rename_drink(args[0], args[1])
    await _reply(update, msg)


@_require_admin
async def cmd_setstock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(update,
            "Usage: `/setstock <drink> <store> <bar>`\n"
            "Example: `/setstock heineken 12 6`\n"
            "_Corrects counts after a recount/breakage — does not touch sales history._")
        return
    try:
        store, bar = int(args[1]), int(args[2])
        if store < 0 or bar < 0:
            raise ValueError
    except ValueError:
        await _reply(update, "❌ Store and bar must be whole numbers ≥ 0.")
        return
    ok, msg = logic.process_set_stock(args[0], store, bar)
    await _reply(update, msg)


@_require_admin
async def cmd_deletedrink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(update, "Usage: `/deletedrink <drink>`\n_Only removes a drink with zero stock._")
        return
    ok, msg = logic.process_delete_drink(args[0])
    await _reply(update, msg)


# ── /sales_report ─────────────────────────────────────────────────────

@_require_admin
async def cmd_sales_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""

    if not arg:
        now = datetime.now()
        text = reports.generate_sales_report(for_month=(now.year, now.month))
    elif arg == "today":
        text = reports.generate_sales_report(for_date=datetime.now().date())
    elif arg == "all":
        text = reports.generate_sales_report(all_time=True)
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_sales_report(for_date=dt.date())
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_sales_report(for_month=(dt.year, dt.month))
            except ValueError:
                await _reply(update, "Usage: `/sales_report` | `/sales_report today` | `/sales_report YYYY-MM-DD` | `/sales_report YYYY-MM` | `/sales_report all`")
                return
    await _reply_long(update, text)


# ── /expense_report ───────────────────────────────────────────────────

@_require_admin
async def cmd_expense_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""

    if not arg:
        now = datetime.now()
        text = reports.generate_expense_report(for_month=(now.year, now.month))
    elif arg == "today":
        text = reports.generate_expense_report(for_date=datetime.now().date())
    elif arg == "all":
        text = reports.generate_expense_report(all_time=True)
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_expense_report(for_date=dt.date())
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_expense_report(for_month=(dt.year, dt.month))
            except ValueError:
                await _reply(update, "Usage: `/expense_report` | `/expense_report today` | `/expense_report YYYY-MM-DD` | `/expense_report YYYY-MM` | `/expense_report all`")
                return
    await _reply_long(update, text)


# ── /staff_report (admin) ─────────────────────────────────────────────

@_require_admin
async def cmd_staff_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""

    if not arg:
        now = datetime.now()
        text = reports.generate_staff_report(for_month=(now.year, now.month))
    elif arg == "today":
        text = reports.generate_staff_report(for_date=datetime.now().date())
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_staff_report(for_date=dt.date())
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_staff_report(for_month=(dt.year, dt.month))
            except ValueError:
                await _reply(update, "Usage: `/staff_report` | `/staff_report today` | `/staff_report YYYY-MM-DD` | `/staff_report YYYY-MM`")
                return
    await _reply_long(update, text)


# ── /summary ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_summary(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    if args and _DATE_RE.match(args[0]):
        target = datetime.strptime(args[0], "%Y-%m-%d").date()
    else:
        target = None
    staff_view = not _is_admin(update.effective_user.id)
    text = reports.generate_daily_summary(target=target, staff_view=staff_view)
    await _reply_long(update, text)


# ── /today ────────────────────────────────────────────────────────────

@_require_auth
async def cmd_today(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    staff_view = not _is_admin(update.effective_user.id)
    text = reports.generate_daily_summary(target=None, staff_view=staff_view)
    await _reply_long(update, text)


# ── /allocation (admin) ──────────────────────────────────────────────

@_require_admin
async def cmd_allocation(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""

    if not arg:
        now = datetime.now()
        text = reports.generate_allocation_report(for_month=(now.year, now.month))
    elif arg == "today":
        text = reports.generate_allocation_report(for_date=datetime.now().date())
    elif arg == "all":
        text = reports.generate_allocation_report(all_time=True)
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_allocation_report(for_date=dt.date())
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_allocation_report(for_month=(dt.year, dt.month))
            except ValueError:
                await _reply(update, "Usage: `/allocation` | `/allocation today` | `/allocation YYYY-MM-DD` | `/allocation YYYY-MM` | `/allocation all`")
                return
    await _reply_long(update, text)


# ── /setallocation (admin) ────────────────────────────────────────────

_ALLOC_KEYS = ("buffer", "restock", "draw", "reinvest", "float")

@_require_admin
async def cmd_setallocation(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/setallocation <key> <percent>`\n\n"
            "*Set-aside keys* (% of gross revenue):\n"
            "  `buffer` — emergency buffer (default 10%)\n"
            "  `restock` — restock budget (default 0%)\n\n"
            "*Profit distribution keys* (% of leftover profit):\n"
            "  `draw` — owner's draw (default 50%)\n"
            "  `reinvest` — business reinvestment (default 30%)\n"
            "  `float` — cash reserve (default 20%)\n\n"
            "Example: `/setallocation buffer 10`\n"
            "Example: `/setallocation draw 50`"
        )
        return

    key = args[0].lower()
    if key not in _ALLOC_KEYS:
        await _reply(update, f"❌ Key must be one of: {', '.join(f'`{k}`' for k in _ALLOC_KEYS)}")
        return

    try:
        pct = int(args[1])
        if not (0 <= pct <= 100):
            raise ValueError
    except ValueError:
        await _reply(update, "❌ Percent must be a whole number between 0 and 100.")
        return

    db.set_setting(f"alloc_{key}", str(pct))

    from config import (
        ALLOC_BUFFER_DEFAULT, ALLOC_RESTOCK_DEFAULT,
        ALLOC_DRAW_DEFAULT, ALLOC_REINVEST_DEFAULT, ALLOC_FLOAT_DEFAULT,
    )
    buffer_  = int(db.get_setting("alloc_buffer",   str(ALLOC_BUFFER_DEFAULT)))
    restock  = int(db.get_setting("alloc_restock",  str(ALLOC_RESTOCK_DEFAULT)))
    draw     = int(db.get_setting("alloc_draw",     str(ALLOC_DRAW_DEFAULT)))
    reinvest = int(db.get_setting("alloc_reinvest", str(ALLOC_REINVEST_DEFAULT)))
    float_   = int(db.get_setting("alloc_float",    str(ALLOC_FLOAT_DEFAULT)))

    await _reply(
        update,
        f"✅ *{key.title()}* set to *{pct}%*\n\n"
        f"*Set-asides* (of gross revenue):\n"
        f"  Buffer: {buffer_}%  |  Restock: {restock}%\n"
        f"  Total: *{buffer_ + restock}%*\n\n"
        f"*Profit distribution* (of leftover):\n"
        f"  Draw: {draw}%  |  Reinvest: {reinvest}%  |  Float: {float_}%\n"
        f"  Total: *{draw + reinvest + float_}%*"
    )


# ── /transfer (admin) ────────────────────────────────────────────────

@_require_admin
async def cmd_transfer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(
            update,
            "Usage: `/transfer <drink> <qty>`\n"
            "Moves stock from store to bar/freezer.\n"
            "Example: `/transfer heineken 12`",
        )
        return

    drink = args[0]
    qty, err = _to_int(args[1], "qty")
    if err:
        await _reply(update, err)
        return

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_transfer(drink, qty, recorded_by=recorded_by)
    await _reply(update, msg)


# ── /activity (admin) ────────────────────────────────────────────────

_USERNAME_RE = re.compile(r"^@?[\w]+$")


@_require_admin
async def cmd_activity(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a chronological activity log for all staff (or one member) for a given day."""
    from datetime import datetime
    args = _parse_args(ctx)

    date_str: str | None = None
    username_filter: str | None = None

    for arg in args:
        if _DATE_RE.match(arg):
            date_str = arg
        elif _USERNAME_RE.match(arg):
            username_filter = arg.lstrip("@")

    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    text = reports.generate_activity_log(date_str, username_filter=username_filter)
    await _reply_long(update, text)


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
    await _reply(update, f"✅ *{reports._esc(username)}* (ID: `{uid}`) added as *staff*.")


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

    removed = db.remove_user(uid)
    if not removed:
        await _reply(update, f"❌ User `{uid}` not found.")
        return
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

    bot_cfg = ctx.application.bot_data
    chat_id = bot_cfg.get("report_chat_id") or REPORT_CHAT_ID or update.effective_chat.id
    schema = bot_cfg.get("schema") or HOTEL_SCHEMA
    tz_str = bot_cfg.get("timezone") or TIMEZONE
    time_str = bot_cfg.get("daily_report_time") or DAILY_REPORT_TIME
    _schedule_daily_report(ctx.job_queue, chat_id, schema, tz_str, time_str)
    await _reply(update, f"✅ Daily report *enabled* — sends at {time_str} ({tz_str}).")


# ── Scheduled job ─────────────────────────────────────────────────────

async def _send_daily_report(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id, schema = ctx.job.data
    db._hotel_schema_var.set(schema)
    text = reports.generate_daily_report()
    await ctx.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN_V2)
    logger.info("Daily report sent to chat_id=%s schema=%s", chat_id, schema)


def _schedule_daily_report(
    job_queue, chat_id: int, schema: str,
    tz_str: str = "Africa/Lagos", time_str: str = "23:00",
) -> None:
    hour, minute = [int(x) for x in time_str.split(":")]
    tz = pytz.timezone(tz_str)
    job_queue.run_daily(
        _send_daily_report,
        time=dtime(hour=hour, minute=minute, tzinfo=tz),
        name="daily_report",
        data=(chat_id, schema),
    )
    logger.info("Daily report scheduled at %s %s for chat_id=%s schema=%s", time_str, tz_str, chat_id, schema)


# ── /hotels (app owner only) ─────────────────────────────────────────

@_require_owner
async def cmd_hotels(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from sqlalchemy import text as _text
    base = db._base_engine()
    with base.connect() as conn:
        rows = conn.execute(_text(
            "SELECT schema_name, name, is_active, created_at, owner_telegram_id "
            "FROM public.hotels ORDER BY id"
        )).fetchall()

    if not rows:
        await _reply(update, "No hotels registered yet.")
        return

    lines = ["*🏨 All Hotels*\n"]
    for r in rows:
        status = "✅" if r[2] else "🔴 suspended"
        name = reports._esc(r[1]) if r[1] else "_(not set up)_"
        created = str(r[3])[:10] if r[3] else "—"
        owner = f"tg:{r[4]}" if r[4] else "—"
        lines.append(
            f"{status} *{reports._esc(r[0])}*\n"
            f"  Name: {name}\n"
            f"  Owner: {owner}\n"
            f"  Added: {created}"
        )

    await _reply(update, "\n\n".join(lines))


# ── Subscription expiry check ─────────────────────────────────────────

async def _check_subscriptions(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Daily job — warns hotel owner before expiry; notifies all users and auto-suspends on expiry."""
    today = date.today()
    hotels = db.get_expiring_hotels()

    for h in hotels:
        expires = date.fromisoformat(h["expires_at"])
        days_left = (expires - today).days
        notified = h["notified_days"].split(",") if h["notified_days"] else []

        if days_left < 0 and "0" not in notified:
            label = "0"
            owner_msg = (
                f"🔴 *{reports._esc(h['name'])} — Subscription Expired*\n\n"
                f"Your subscription expired on *{h['expires_at']}* and the bot has been suspended.\n\n"
                f"Contact the service provider to renew and restore access."
            )
            staff_msg = (
                f"🔴 *{reports._esc(h['name'])} Bot — Service Suspended*\n\n"
                f"This bot's service has ended. Please contact your hotel manager."
            )
        elif days_left <= 1 and "1" not in notified:
            label = "1"
            owner_msg = (
                f"🚨 *{reports._esc(h['name'])} — 1 Day Left!*\n\n"
                f"Your subscription expires *tomorrow ({h['expires_at']})*.\n"
                f"Renew now to avoid the bot being suspended."
            )
            staff_msg = None  # warnings go to owner only
        elif days_left <= 3 and "3" not in notified:
            label = "3"
            owner_msg = (
                f"⚠️ *{reports._esc(h['name'])} — 3 Days Left*\n\n"
                f"Your subscription expires on *{h['expires_at']}*. Please renew soon."
            )
            staff_msg = None
        elif days_left <= 7 and "7" not in notified:
            label = "7"
            owner_msg = (
                f"📅 *{reports._esc(h['name'])} — 7 Days Left*\n\n"
                f"Your subscription expires on *{h['expires_at']}*. Renew before it runs out."
            )
            staff_msg = None
        else:
            continue

        app = _hotel_apps.get(h["schema"])
        if not app:
            continue

        # Resolve owner Telegram ID
        owner_id = h["owner_id"]
        if not owner_id:
            ids = [x.strip() for x in h["admin_ids"].split(",") if x.strip().isdigit()]
            owner_id = int(ids[0]) if ids else None

        # Send to owner
        if owner_id:
            try:
                await app.bot.send_message(
                    chat_id=int(owner_id), text=owner_msg, parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception as e:
                logger.warning("Failed to notify %s owner: %s", h["schema"], e)

        # On expiry — notify all staff too, then auto-suspend
        if label == "0" and staff_msg:
            db._hotel_schema_var.set(h["schema"])
            try:
                users = db.read_all("users")
                for user in users:
                    uid = int(user["user_id"])
                    if uid == owner_id:
                        continue  # owner already got the owner message
                    try:
                        await app.bot.send_message(
                            chat_id=uid, text=staff_msg, parse_mode=ParseMode.MARKDOWN_V2
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.warning("Failed to notify %s staff on expiry: %s", h["schema"], e)

            try:
                from sqlalchemy import text as _text
                with db._base_engine().connect() as conn:
                    conn.execute(
                        _text("UPDATE public.hotels SET is_active = FALSE WHERE schema_name = :s"),
                        {"s": h["schema"]},
                    )
                    conn.commit()
                logger.info("Auto-suspended %s — subscription expired", h["schema"])
            except Exception as e:
                logger.error("Failed to auto-suspend %s: %s", h["schema"], e)

        db.mark_expiry_notified(h["schema"], label)


# ── Broadcast helper + /broadcast (app owner only) ───────────────────

async def _broadcast_all(message: str) -> tuple[int, int]:
    """Send message to every user across every hotel. Returns (sent, failed)."""
    sent = failed = 0
    for schema, app in _hotel_apps.items():
        db._hotel_schema_var.set(schema)
        try:
            users = db.read_all("users")
        except Exception:
            continue
        for user in users:
            try:
                await app.bot.send_message(
                    chat_id=int(user["user_id"]),
                    text=message,
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
                sent += 1
            except Exception:
                failed += 1
    return sent, failed


@_require_owner
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(update, "Usage: `/broadcast <message>`\nSends to all users across all hotels.")
        return
    message = " ".join(args)
    await _reply(update, "📢 Sending…")
    sent, failed = await _broadcast_all(message)
    await _reply(update, f"✅ Broadcast complete — {sent} delivered, {failed} failed.")


# ── /setexpiry (app owner only) ──────────────────────────────────────

@_require_owner
async def cmd_setexpiry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if len(args) < 2:
        await _reply(update,
            "*Usage:* `/setexpiry <schema> <YYYY-MM-DD>`\n"
            "*Example:* `/setexpiry kings_inn 2026-06-20`"
        )
        return
    schema, expiry = args[0].strip().lower(), args[1].strip()
    if not _DATE_RE.match(expiry):
        await _reply(update, "❌ Date must be in `YYYY-MM-DD` format.")
        return
    try:
        db.set_subscription_expiry(schema, expiry)
    except Exception as e:
        await _reply(update, f"❌ Failed: `{e}`")
        return
    await _reply(update,
        f"✅ *{reports._esc(schema)}* subscription set to expire on *{reports._esc(expiry)}*.\n"
        f"Owner will be notified at 7, 3, and 1 day before expiry."
    )


# ── /addhotel (app owner only) ───────────────────────────────────────

@_require_owner
async def cmd_addhotel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /addhotel <schema> <bot_token> <admin_id>"""
    args = _parse_args(ctx)
    if len(args) < 3:
        await _reply(update,
            "*Usage:* `/addhotel <schema> <bot_token> <admin_id>`\n\n"
            "*Example:*\n"
            "`/addhotel kings_inn 7123456789:AAGxx 98765432`"
        )
        return

    schema = args[0].strip().lower().replace(" ", "_")
    token = args[1].strip()
    admin_id = args[2].strip()

    if not admin_id.isdigit():
        await _reply(update, "❌ `admin_id` must be a numeric Telegram user ID.")
        return

    try:
        db.init_db(schema=schema, token=token)
        db.add_hotel_config(schema=schema, token=token, admin_ids=admin_id)
    except Exception as e:
        await _reply(update, f"❌ Failed: `{e}`")
        return

    await _reply(update,
        f"✅ *{reports._esc(schema)}* registered successfully.\n\n"
        f"Redeploy the Railway service — the new bot will start automatically.\n"
        f"Then the hotel owner messages their bot and runs `/setup`."
    )


# ── /suspend + /unsuspend (app owner only) ───────────────────────────

@_require_owner
async def cmd_suspend(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(update, "Usage: `/suspend <schema>`\nExample: `/suspend kings_inn`")
        return
    schema = args[0].strip().lower()
    from sqlalchemy import text as _text
    with db._base_engine().connect() as conn:
        result = conn.execute(
            _text("UPDATE public.hotels SET is_active = FALSE WHERE schema_name = :s"),
            {"s": schema},
        )
        conn.commit()
    if result.rowcount == 0:
        await _reply(update, f"❌ Hotel `{schema}` not found.")
        return
    await _reply(update, f"🔴 *{reports._esc(schema)}* suspended.\nData is fully preserved. Restart the service to cut off their bot.")


@_require_owner
async def cmd_unsuspend(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = _parse_args(ctx)
    if not args:
        await _reply(update, "Usage: `/unsuspend <schema>`\nExample: `/unsuspend kings_inn`")
        return
    schema = args[0].strip().lower()
    from sqlalchemy import text as _text
    with db._base_engine().connect() as conn:
        result = conn.execute(
            _text("UPDATE public.hotels SET is_active = TRUE WHERE schema_name = :s"),
            {"s": schema},
        )
        conn.commit()
    if result.rowcount == 0:
        await _reply(update, f"❌ Hotel `{schema}` not found.")
        return
    await _reply(update, f"✅ *{reports._esc(schema)}* unsuspended.\nRestart the service to bring their bot back.")


# ── /export (admin) ───────────────────────────────────────────────────

_EXPORT_TABLES = [
    "sales", "rooms", "expenses", "debtors", "debtor_payments",
    "inventory", "transfers", "users", "settings",
]


def _build_export_excel(schema: str) -> io.BytesIO:
    import pandas as pd
    engine = db.get_engine()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Summary sheet
        with engine.connect() as conn:
            from sqlalchemy import text as _text
            rows = []
            def _q(sql):
                return round(float(conn.execute(_text(sql)).scalar() or 0), 2)

            rows.append(("Bar Revenue (₦)",      _q("SELECT COALESCE(SUM(total_revenue),0) FROM sales WHERE deleted_at='' OR deleted_at IS NULL")))
            rows.append(("Rooms Revenue (₦)",    _q("SELECT COALESCE(SUM(total_revenue),0) FROM rooms WHERE deleted_at='' OR deleted_at IS NULL")))
            rows.append(("Total Expenses (₦)",   _q("SELECT COALESCE(SUM(amount),0) FROM expenses WHERE deleted_at='' OR deleted_at IS NULL")))
            rows.append(("Outstanding Debt (₦)", _q("SELECT COALESCE(SUM(amount - amount_paid),0) FROM debtors WHERE status='outstanding'")))
            rows.append(("Bar Stock Value (₦)",  _q("SELECT COALESCE(SUM(current_stock * cost_price),0) FROM inventory")))
            rows.append(("Store Stock Value (₦)",_q("SELECT COALESCE(SUM(store_stock * cost_price),0) FROM inventory")))
            rows.append(("Export Date", date.today().strftime("%Y-%m-%d")))

        pd.DataFrame(rows, columns=["Metric", "Value"]).to_excel(writer, sheet_name="Summary", index=False)

        # One sheet per table
        for table in _EXPORT_TABLES:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", engine)
                df.to_excel(writer, sheet_name=table.title(), index=False)
            except Exception:
                pass

    buf.seek(0)
    return buf


@_require_admin
async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    schema = db._hotel_schema_var.get() or HOTEL_SCHEMA
    await _reply(update, "⏳ Generating export, please wait…")
    try:
        buf = await asyncio.get_event_loop().run_in_executor(None, _build_export_excel, schema)
        filename = f"{schema}_export_{date.today().strftime('%Y%m%d')}.xlsx"
        await update.message.reply_document(
            document=buf,
            filename=filename,
            caption=f"📊 Full data export for *{reports._esc(schema)}*",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as e:
        logger.error("Export failed: %s", e, exc_info=e)
        await _reply(update, "❌ Export failed. Please try again.")


# ── Error handler ─────────────────────────────────────────────────────

def _plain_error(err: Exception) -> str:
    """Map an exception to a human-readable description for hotel admins."""
    err_str = str(err).lower()
    err_type = type(err).__name__
    if any(k in err_str for k in ("connect", "database", "psycopg", "sqlalchemy", "could not connect")):
        return "Database connection issue — the database may be temporarily unavailable."
    if any(k in err_str for k in ("timeout", "timed out")):
        return "The request timed out — the server may be under load."
    if any(k in err_str for k in ("permission", "forbidden", "not allowed")):
        return "Permission error — the bot may lack access to perform this action."
    if isinstance(err, (KeyError, AttributeError, TypeError)):
        return "Report generation failed — some data may be missing or in an unexpected format."
    if isinstance(err, ValueError):
        return "Invalid data encountered while processing the request."
    return f"Unexpected error ({err_type}) — check Railway logs for details."


async def _error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", ctx.error, exc_info=ctx.error)

    # Who triggered it
    user_id: int | None = None
    username = "Unknown"
    trigger = "unknown action"
    if isinstance(update, Update):
        user = update.effective_user
        if user:
            user_id = user.id
            username = f"@{user.username}" if user.username else (user.first_name or str(user.id))
        if update.message and update.message.text:
            trigger = update.message.text[:80]
        elif update.callback_query and update.callback_query.data:
            trigger = f"button: {update.callback_query.data[:60]}"

    plain = _plain_error(ctx.error)

    admin_msg = (
        f"🚨 *Bot Error*\n"
        f"👤 Triggered by: {reports._esc(username)}\n"
        f"📌 Action: `{trigger}`\n"
        f"❌ Problem: {reports._esc(plain)}"
    )

    # Button to open a chat with the staff member who hit the error
    keyboard = None
    if user_id:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Message {username}", url=f"tg://user?id={user_id}")
        ]])

    # Notify admins (skip if the admin themselves triggered the error)
    admin_ids = list(_active_admin_ids.get() or ADMIN_IDS)
    for admin_id in admin_ids:
        if admin_id == user_id:
            continue
        try:
            await ctx.bot.send_message(
                admin_id, admin_msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
            )
        except Exception:
            try:
                await ctx.bot.send_message(admin_id, admin_msg, reply_markup=keyboard)
            except Exception:
                pass

    # Reply to whoever triggered it
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("⚠️ An unexpected error occurred. Please try again.")


# ══════════════════════════════════════════════════════════════════════
# ── Guided tap flows ──────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

_SELL_DRINK, _SELL_DRINK_TEXT, _SELL_QTY, _SELL_QTY_TEXT, _SELL_DATE, _SELL_DATE_TEXT = range(6)
_BOOK_TYPE, _BOOK_TYPE_TEXT, _BOOK_QTY, _BOOK_QTY_TEXT, _BOOK_NIGHTS, _BOOK_NIGHTS_TEXT, _BOOK_DATE, _BOOK_DATE_TEXT = range(6, 14)
_SETUP_NAME, _SETUP_TZ, _SETUP_CONFIRM = range(14, 17)
_EXP_ACCT, _EXP_CAT, _EXP_CAT_TEXT, _EXP_AMT, _EXP_NOTE, _EXP_DATE = range(17, 23)
_RST_DRINK, _RST_DRINK_TEXT, _RST_QTY, _RST_QTY_TEXT, _RST_COST = range(23, 28)
_TFR_DRINK, _TFR_DRINK_TEXT, _TFR_QTY, _TFR_QTY_TEXT = range(28, 32)
_DEB_ACCT, _DEB_NAME, _DEB_AMT, _DEB_NOTE, _DEB_DATE = range(32, 37)
_PAY_ACCT, _PAY_NAME, _PAY_AMT = range(37, 40)
_DEL_TYPE, _DEL_ENTRY, _DEL_CONFIRM = range(40, 43)
_SSET_PRICE_DRINK, _SSET_PRICE_AMT = range(43, 45)
_SSET_ROOM_TYPE, _SSET_ROOM_TYPE_TEXT, _SSET_ROOM_AMT = range(45, 48)
_SSET_THRESH_DRINK, _SSET_THRESH_AMT = range(48, 50)
_SSET_ALLOC_KEY, _SSET_ALLOC_VAL = range(50, 52)
_ACT_DATE = 52
_RNM_OLD, _RNM_NEW = range(53, 55)
_SST_DRINK, _SST_STORE, _SST_BAR = range(55, 58)
_DDR_DRINK, _DDR_CONFIRM = range(58, 60)


def _drink_keyboard() -> InlineKeyboardMarkup:
    items = inv.get_inventory_summary()
    in_stock = [i["drink"].title() for i in items if i["bar_stock"] > 0]
    if not in_stock:
        in_stock = [i["drink"].title() for i in items]
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for d in in_stock:
        row.append(InlineKeyboardButton(d, callback_data=f"sd:{d.lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Other drink", callback_data="sd:__other__")])
    return InlineKeyboardMarkup(rows)


def _qty_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(n), callback_data=f"{prefix}:{n}") for n in (1, 2, 3)],
        [InlineKeyboardButton(str(n), callback_data=f"{prefix}:{n}") for n in (6, 12)],
        [InlineKeyboardButton("✏️ Other", callback_data=f"{prefix}:__other__")],
    ])


def _nights_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(n), callback_data=f"bn:{n}") for n in (1, 2, 3)],
        [InlineKeyboardButton(str(n), callback_data=f"bn:{n}") for n in (4, 5, 7)],
        [InlineKeyboardButton("✏️ Other", callback_data="bn:__other__")],
    ])


def _room_type_keyboard() -> InlineKeyboardMarkup:
    presets = db.get_all_room_type_prices()
    rows = []
    for p in presets:
        label = f"{p['room_type']} — ₦{int(p['price']):,}/night"
        rows.append([InlineKeyboardButton(label, callback_data=f"bt:{p['room_type'].lower()}")])
    rows.append([InlineKeyboardButton("✏️ Other type", callback_data="bt:__other__")])
    return InlineKeyboardMarkup(rows)


def _calendar_keyboard(prefix: str, year: int, month: int) -> InlineKeyboardMarkup:
    today = date.today()
    rows: list[list[InlineKeyboardButton]] = []

    rows.append([InlineKeyboardButton("✅ Today", callback_data=f"{prefix}:today")])

    first = date(year, month, 1)
    prev = (first - timedelta(days=1)).replace(day=1)
    nxt_year, nxt_month = (year + 1, 1) if month == 12 else (year, month + 1)
    is_current = (year == today.year and month == today.month)
    rows.append([
        InlineKeyboardButton("◀", callback_data=f"{prefix}:cal_p:{prev.year:04d}-{prev.month:02d}"),
        InlineKeyboardButton(f"{_cal.month_abbr[month]} {year}", callback_data=f"{prefix}:cal_x"),
        InlineKeyboardButton(
            "·" if is_current else "▶",
            callback_data=f"{prefix}:cal_x" if is_current else f"{prefix}:cal_n:{nxt_year:04d}-{nxt_month:02d}",
        ),
    ])

    rows.append([InlineKeyboardButton(d, callback_data=f"{prefix}:cal_x") for d in ["M", "T", "W", "T", "F", "S", "S"]])

    for week in _cal.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data=f"{prefix}:cal_x"))
            else:
                d = date(year, month, day)
                if d > today:
                    row.append(InlineKeyboardButton("·", callback_data=f"{prefix}:cal_x"))
                else:
                    label = f"[{day}]" if d == today else str(day)
                    row.append(InlineKeyboardButton(label, callback_data=f"{prefix}:{d.isoformat()}"))
        rows.append(row)

    return InlineKeyboardMarkup(rows)


def _date_keyboard(prefix: str) -> InlineKeyboardMarkup:
    today = date.today()
    return _calendar_keyboard(prefix, today.year, today.month)


def _staff_date_keyboard(prefix: str) -> InlineKeyboardMarkup:
    """Two-button date picker for staff — today and yesterday only."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Today",      callback_data=f"{prefix}:today"),
        InlineKeyboardButton("⬅️ Yesterday", callback_data=f"{prefix}:{yesterday}"),
    ]])


def _date_keyboard_for(prefix: str, user_id: int) -> InlineKeyboardMarkup:
    """Full calendar for admins, today/yesterday only for staff."""
    return _date_keyboard(prefix) if _is_admin(user_id) else _staff_date_keyboard(prefix)


async def _cancel_conv(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.clear()
    if update.message:
        await update.message.reply_text("❌ Cancelled.", reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


# ── /sell guided flow ─────────────────────────────────────────────────

@_require_auth
async def cmd_sell_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text(
        "🍺 *Which drink?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_drink_keyboard(),
    )
    return _SELL_DRINK


async def _sell_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data[3:]
    if drink == "__other__":
        await q.edit_message_text("Type the drink name:")
        return _SELL_DRINK_TEXT
    ctx.user_data["sell_drink"] = drink
    await q.edit_message_text(
        f"🍺 *{reports._esc(drink.title())}* — how many?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_qty_keyboard("sq"),
    )
    return _SELL_QTY


async def _sell_drink_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    drink = update.message.text.strip().lower()
    ctx.user_data["sell_drink"] = drink
    await update.message.reply_text(
        f"🍺 *{reports._esc(drink.title())}* — how many?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_qty_keyboard("sq"),
    )
    return _SELL_QTY


async def _sell_pick_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[3:]
    if val == "__other__":
        await q.edit_message_text("How many? (type a number)")
        return _SELL_QTY_TEXT
    ctx.user_data["sell_qty"] = int(val)
    uid = update.effective_user.id
    await q.edit_message_text(
        f"🍺 {val}× {ctx.user_data.get('sell_drink', '').title()} — when?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard_for("sdd", uid),
    )
    return _SELL_DATE


async def _sell_qty_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        qty = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return _SELL_QTY_TEXT
    ctx.user_data["sell_qty"] = qty
    drink = ctx.user_data.get("sell_drink", "")
    uid = update.effective_user.id
    await update.message.reply_text(
        f"🍺 {qty}× {drink.title()} — when?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard_for("sdd", uid),
    )
    return _SELL_DATE


async def _sell_pick_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[4:]  # strip "sdd:"

    if val == "today":
        await q.edit_message_text("📝 Recording...")
        return await _do_sell(update, ctx, timestamp=None)

    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        year, month = int(ym[:4]), int(ym[5:7])
        drink = ctx.user_data.get("sell_drink", "")
        qty = ctx.user_data.get("sell_qty", 1)
        await q.edit_message_text(
            f"🍺 {qty}× {drink.title()} — when?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("sdd", year, month),
        )
        return _SELL_DATE

    if val == "cal_x":
        return _SELL_DATE

    if _DATE_RE.match(val):
        await q.edit_message_text("📝 Recording...")
        return await _do_sell(update, ctx, timestamp=val)

    return _SELL_DATE


async def _remove_undo_button(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id, message_id = ctx.job.data
    try:
        await ctx.bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
    except Exception:
        pass


async def _do_sell(update: Update, ctx: ContextTypes.DEFAULT_TYPE, timestamp: str | None = None) -> int:
    drink = ctx.user_data.pop("sell_drink", "")
    qty   = ctx.user_data.pop("sell_qty", 1)
    user  = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg, alert = logic.process_drink_sale(drink, qty, timestamp=timestamp, recorded_by=recorded_by)
    if timestamp and ok:
        msg += f"\n_(recorded for {timestamp})_"
    if ok:
        undo_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Undo — tap to cancel this entry", callback_data="undo:sell")]])
        sent = await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=undo_kb)
        ctx.application.job_queue.run_once(
            _remove_undo_button, 120,
            data=(sent.chat_id, sent.message_id),
            name=f"undo_{sent.message_id}",
        )
    else:
        await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    if ok and alert:
        for admin_id in ADMIN_IDS:
            if admin_id != user.id:
                try:
                    await ctx.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ *Low Stock Alert*\n_{recorded_by} sold {qty}× {drink}_\n\n{alert}",
                        parse_mode=ParseMode.MARKDOWN_V2,
                    )
                except Exception:
                    pass
    return ConversationHandler.END


# ── /book guided flow ─────────────────────────────────────────────────

@_require_auth
async def cmd_book_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    presets = db.get_all_room_type_prices()
    if not presets:
        await update.effective_message.reply_text(
            "No room types configured yet.\nAsk admin to run: `/setroomtype standard 15000`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_get_keyboard(update.effective_user.id),
        )
        return ConversationHandler.END
    await update.effective_message.reply_text(
        "🛏 *Room type?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_room_type_keyboard(),
    )
    return _BOOK_TYPE


async def _book_pick_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    rtype = q.data[3:]
    if rtype == "__other__":
        await q.edit_message_text("Type the room type:")
        return _BOOK_TYPE_TEXT
    ctx.user_data["book_type"] = rtype
    await q.edit_message_text(
        f"🛏 *{reports._esc(rtype.title())}* — how many rooms?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_qty_keyboard("bq"),
    )
    return _BOOK_QTY


async def _book_type_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    rtype = update.message.text.strip().lower()
    ctx.user_data["book_type"] = rtype
    await update.message.reply_text(
        f"🛏 *{reports._esc(rtype.title())}* — how many rooms?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_qty_keyboard("bq"),
    )
    return _BOOK_QTY


async def _book_pick_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[3:]
    if val == "__other__":
        await q.edit_message_text("How many rooms? (type a number)")
        return _BOOK_QTY_TEXT
    ctx.user_data["book_qty"] = int(val)
    rtype = ctx.user_data.get("book_type", "room")
    await q.edit_message_text(
        f"🛏 {val}× *{reports._esc(rtype.title())}* — how many nights?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_nights_keyboard(),
    )
    return _BOOK_NIGHTS


async def _book_qty_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        qty = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return _BOOK_QTY_TEXT
    ctx.user_data["book_qty"] = qty
    rtype = ctx.user_data.get("book_type", "room")
    await update.message.reply_text(
        f"🛏 {qty}× *{reports._esc(rtype.title())}* — how many nights?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_nights_keyboard(),
    )
    return _BOOK_NIGHTS


async def _book_pick_nights(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[3:]
    if val == "__other__":
        await q.edit_message_text("How many nights? (type a number)")
        return _BOOK_NIGHTS_TEXT
    ctx.user_data["book_nights"] = int(val)
    uid = update.effective_user.id
    await q.edit_message_text(
        f"🛏 {ctx.user_data.get('book_qty', 1)}× {ctx.user_data.get('book_type', '').title()}, {val} nights — when?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard_for("bdd", uid),
    )
    return _BOOK_DATE


async def _book_nights_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        nights = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return _BOOK_NIGHTS_TEXT
    ctx.user_data["book_nights"] = nights
    uid = update.effective_user.id
    await update.message.reply_text(
        f"🛏 {ctx.user_data.get('book_qty', 1)}× {ctx.user_data.get('book_type', '').title()}, {nights} nights — when?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard_for("bdd", uid),
    )
    return _BOOK_DATE


async def _book_pick_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[4:]  # strip "bdd:"

    if val == "today":
        await q.edit_message_text("📝 Recording...")
        return await _do_book(update, ctx, timestamp=None)

    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        year, month = int(ym[:4]), int(ym[5:7])
        rtype = ctx.user_data.get("book_type", "")
        qty = ctx.user_data.get("book_qty", 1)
        nights = ctx.user_data.get("book_nights", 1)
        await q.edit_message_text(
            f"🛏 {qty}× {rtype.title()}, {nights} nights — when?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("bdd", year, month),
        )
        return _BOOK_DATE

    if val == "cal_x":
        return _BOOK_DATE

    if _DATE_RE.match(val):
        await q.edit_message_text("📝 Recording...")
        return await _do_book(update, ctx, timestamp=val)

    return _BOOK_DATE


async def _do_book(update: Update, ctx: ContextTypes.DEFAULT_TYPE, timestamp: str | None = None) -> int:
    rtype  = ctx.user_data.pop("book_type", "")
    qty    = ctx.user_data.pop("book_qty", 1)
    nights = ctx.user_data.pop("book_nights", 1)
    user   = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    price = db.get_room_type_price(rtype)
    if price is None:
        await update.effective_chat.send_message(
            f"❌ No preset price for *{reports._esc(rtype.title())}*.\nAsk admin: `/setroomtype {rtype} <price>`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_get_keyboard(user.id),
        )
        return ConversationHandler.END
    ok, msg = logic.process_room_sale(rtype, qty, price, nights, timestamp=timestamp, recorded_by=recorded_by)
    if ok:
        undo_kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Undo — tap to cancel this booking", callback_data="undo:book")]])
        sent = await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=undo_kb)
        ctx.application.job_queue.run_once(
            _remove_undo_button, 120,
            data=(sent.chat_id, sent.message_id),
            name=f"undo_{sent.message_id}",
        )
    else:
        await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Inline undo callback ──────────────────────────────────────────────

async def _cb_undo_inline(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    user = update.effective_user
    username = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_undo(username)
    if ok:
        # Cancel the scheduled button-removal job
        for job in ctx.application.job_queue.get_jobs_by_name(f"undo_{q.message.message_id}"):
            job.schedule_removal()
        await q.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await q.answer(msg, show_alert=True)


# ── History date picker ────────────────────────────────────────────────

@_require_auth
async def _btn_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📋 *History — which day?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard("hst"),
    )


async def _cb_history_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    val = q.data[4:]  # strip "hst:"

    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        y, m = int(ym[:4]), int(ym[5:7])
        await q.edit_message_text(
            "📋 *History — which day?*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("hst", y, m),
        )
        return

    if val == "cal_x":
        return

    from datetime import datetime as _dt
    date_str = _dt.now().strftime("%Y-%m-%d") if val == "today" else val
    label = _dt.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    entries = db.get_entries_by_date(date_str)

    if not entries:
        await q.edit_message_text(f"📋 No entries for *{label}*.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    is_admin = _is_admin(q.from_user.id)
    sales    = [e for e in entries if e["entry_type"] == "sale"]
    rooms    = [e for e in entries if e["entry_type"] == "room"]
    expenses = [e for e in entries if e["entry_type"] == "expense"]

    lines = [f"📋 *Entries for {label}*", "─" * 30]
    if sales:
        lines.append("🍺 *Sales*")
        for e in sales:
            lines.append(
                f"  `[{e['id']}]` {e['drink_name'].title()} ×{e['quantity']} "
                f"@ ₦{float(e['selling_price']):,.0f} = ₦{float(e['total_revenue']):,.0f}"
            )
    if rooms:
        lines.append("🛏 *Rooms*")
        for e in rooms:
            lines.append(
                f"  `[{e['id']}]` {e['room_type'].title()} ×{e['quantity']} "
                f"@ ₦{float(e['price_per_night']):,.0f} ×{e['nights']} nights "
                f"= ₦{float(e['total_revenue']):,.0f}"
            )
    if expenses and is_admin:
        lines.append("💸 *Expenses*")
        for e in expenses:
            note = f" — {e['description']}" if e.get("description") else ""
            lines.append(
                f"  `[{e['id']}]` {e['account'].title()} › {e['category'].title()} "
                f"₦{float(e['amount']):,.0f}{note}"
            )
    if is_admin:
        lines.append("\n_Tap_ ⚙️ *Manage → 🗑 Delete Entry* _to remove._")

    await _reply_long_cb(q, "\n".join(lines))


# ── Keyboard shortcut handlers ────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════════
# ── Admin ⚙️ Manage flows ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

def _manage_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💸 Expense",      callback_data="mgr:expense"),
         InlineKeyboardButton("📦 Restock",      callback_data="mgr:restock")],
        [InlineKeyboardButton("🔄 Transfer",     callback_data="mgr:transfer"),
         InlineKeyboardButton("➕ Add Debtor",   callback_data="mgr:add_debtor")],
        [InlineKeyboardButton("✅ Pay Debtor",   callback_data="mgr:pay_debtor"),
         InlineKeyboardButton("📊 Allocation",   callback_data="mgr:allocation")],
        [InlineKeyboardButton("🗑 Delete Entry", callback_data="mgr:delete"),
         InlineKeyboardButton("📋 Activity",     callback_data="mgr:activity")],
        [InlineKeyboardButton("📈 Restock Plan", callback_data="mgr:restock_plan"),
         InlineKeyboardButton("🛠 Fix Stock",    callback_data="mgr:fixstock")],
        [InlineKeyboardButton("⚙️ Settings",    callback_data="mgr:settings"),
         InlineKeyboardButton("👥 Staff",        callback_data="mgr:staff")],
    ])


def _fixstock_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔀 Rename / Merge", callback_data="fix:rename")],
        [InlineKeyboardButton("🔢 Set Count",      callback_data="fix:setstock")],
        [InlineKeyboardButton("🗑 Delete Drink",   callback_data="fix:delete")],
    ])


def _existing_drink_kb(prefix: str) -> InlineKeyboardMarkup:
    """Keyboard of existing drinks only (no 'new drink' option), 2 per row."""
    items = inv.get_inventory_summary()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for i in items:
        row.append(InlineKeyboardButton(i["drink"], callback_data=f"{prefix}:{i['drink'].lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def _settings_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Drink Prices",    callback_data="sset:price"),
         InlineKeyboardButton("🛏 Room Types",      callback_data="sset:roomtype")],
        [InlineKeyboardButton("🔔 Low Stock Alert", callback_data="sset:threshold"),
         InlineKeyboardButton("📊 Allocation %",    callback_data="sset:allocation")],
        [InlineKeyboardButton("📅 Daily Report",    callback_data="sset:dailyreport")],
    ])


def _account_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🍺 Bar",   callback_data=f"{prefix}:bar"),
        InlineKeyboardButton("🛏 Rooms", callback_data=f"{prefix}:rooms"),
    ]])


def _expense_cat_keyboard() -> InlineKeyboardMarkup:
    cats = ["Salary", "Utilities", "Maintenance", "Supplies", "Cleaning", "Marketing"]
    rows = [[
        InlineKeyboardButton(cats[i],   callback_data=f"exp_cat:{cats[i].lower()}"),
        InlineKeyboardButton(cats[i+1], callback_data=f"exp_cat:{cats[i+1].lower()}"),
    ] for i in range(0, len(cats), 2)]
    rows.append([InlineKeyboardButton("✏️ Other", callback_data="exp_cat:__other__")])
    return InlineKeyboardMarkup(rows)


def _all_drink_keyboard(prefix: str) -> InlineKeyboardMarkup:
    items = inv.get_inventory_summary()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for i in items:
        row.append(InlineKeyboardButton(i["drink"].title(), callback_data=f"{prefix}:{i['drink'].lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ New drink", callback_data=f"{prefix}:__other__")])
    return InlineKeyboardMarkup(rows)


def _store_drink_keyboard(prefix: str) -> InlineKeyboardMarkup:
    items = [i for i in inv.get_inventory_summary() if i["store_stock"] > 0]
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for i in items:
        label = f"{i['drink'].title()} ({i['store_stock']})"
        row.append(InlineKeyboardButton(label, callback_data=f"{prefix}:{i['drink'].lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Other drink", callback_data=f"{prefix}:__other__")])
    return InlineKeyboardMarkup(rows)


def _restock_qty_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(n), callback_data=f"rst_q:{n}") for n in (12, 24, 48)],
        [InlineKeyboardButton("✏️ Other", callback_data="rst_q:__other__")],
    ])


def _transfer_qty_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(n), callback_data=f"tfr_q:{n}") for n in (6, 12, 24)],
        [InlineKeyboardButton("✏️ Other", callback_data="tfr_q:__other__")],
    ])


def _skip_keyboard(cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data=cb)]])


@_require_admin
async def _btn_manage(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "⚙️ *Admin Actions*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_manage_menu_keyboard(),
    )


# ── Expense flow ───────────────────────────────────────────────────────

@_require_admin
async def _exp_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "💸 *Record Expense — which account?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_account_keyboard("exp_ac"),
    )
    return _EXP_ACCT


async def _exp_pick_acct(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    ctx.user_data["exp_acct"] = q.data.split(":")[1]
    await q.edit_message_text(
        "💸 *Category?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_expense_cat_keyboard(),
    )
    return _EXP_CAT


async def _exp_pick_cat(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[8:]
    if val == "__other__":
        await q.edit_message_text("Type the expense category:")
        return _EXP_CAT_TEXT
    ctx.user_data["exp_cat"] = val
    acct = ctx.user_data.get("exp_acct", "")
    await q.edit_message_text(
        f"💸 {acct.title()} / {val.title()} — *amount?* (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _EXP_AMT


async def _exp_cat_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["exp_cat"] = update.message.text.strip().lower()
    acct = ctx.user_data.get("exp_acct", "")
    cat = ctx.user_data["exp_cat"]
    await update.message.reply_text(
        f"💸 {acct.title()} / {cat.title()} — *amount?* (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _EXP_AMT


async def _exp_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    val, err = _to_float(update.message.text.strip(), "amount")
    if err:
        await update.message.reply_text("❌ Enter a valid amount (e.g. 5000):")
        return _EXP_AMT
    ctx.user_data["exp_amt"] = val
    await update.message.reply_text("📝 Any note? (optional)", reply_markup=_skip_keyboard("exp_note:skip"))
    return _EXP_NOTE


async def _exp_note_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    ctx.user_data["exp_note"] = ""
    await q.edit_message_text(
        "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_date_keyboard("exp_d"),
    )
    return _EXP_DATE


async def _exp_note_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["exp_note"] = update.message.text.strip()
    await update.message.reply_text(
        "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_date_keyboard("exp_d"),
    )
    return _EXP_DATE


async def _exp_pick_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[6:]
    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        y, m = int(ym[:4]), int(ym[5:7])
        await q.edit_message_text(
            "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("exp_d", y, m),
        )
        return _EXP_DATE
    if val == "cal_x":
        return _EXP_DATE
    ts = None if val == "today" else val
    acct = ctx.user_data.pop("exp_acct", "bar")
    cat  = ctx.user_data.pop("exp_cat", "")
    amt  = ctx.user_data.pop("exp_amt", 0.0)
    note = ctx.user_data.pop("exp_note", "")
    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_expense(acct, cat, amt, note, timestamp=ts, recorded_by=recorded_by)
    await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Restock flow ───────────────────────────────────────────────────────

@_require_admin
async def _rst_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "📦 *Restock — which drink?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_all_drink_keyboard("rst_dr"),
    )
    return _RST_DRINK


async def _rst_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data[7:]
    if drink == "__other__":
        await q.edit_message_text("Type the drink name:")
        return _RST_DRINK_TEXT
    ctx.user_data["rst_drink"] = drink
    await q.edit_message_text(
        f"📦 *{reports._esc(drink.title())}* — how many units?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_restock_qty_keyboard(),
    )
    return _RST_QTY


async def _rst_drink_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["rst_drink"] = update.message.text.strip().lower()
    drink = ctx.user_data["rst_drink"]
    await update.message.reply_text(
        f"📦 *{reports._esc(drink.title())}* — how many units?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_restock_qty_keyboard(),
    )
    return _RST_QTY


async def _rst_pick_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[6:]
    if val == "__other__":
        await q.edit_message_text("How many units? (type a number)")
        return _RST_QTY_TEXT
    ctx.user_data["rst_qty"] = int(val)
    drink = ctx.user_data.get("rst_drink", "")
    await q.edit_message_text(
        f"📦 {val}× {drink.title()} — *cost per unit?* (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _RST_COST


async def _rst_qty_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        qty = int(update.message.text.strip())
        if qty <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return _RST_QTY_TEXT
    ctx.user_data["rst_qty"] = qty
    drink = ctx.user_data.get("rst_drink", "")
    await update.message.reply_text(
        f"📦 {qty}× {drink.title()} — *cost per unit?* (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _RST_COST


async def _rst_cost(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    cost, err = _to_float(update.message.text.strip(), "cost")
    if err:
        await update.message.reply_text("❌ Enter a valid cost (e.g. 300):")
        return _RST_COST
    drink = ctx.user_data.pop("rst_drink", "")
    qty   = ctx.user_data.pop("rst_qty", 0)
    user  = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_restock(drink, qty, cost, recorded_by=recorded_by)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Transfer flow ──────────────────────────────────────────────────────

@_require_admin
async def _tfr_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    in_store = [i for i in inv.get_inventory_summary() if i["store_stock"] > 0]
    if not in_store:
        await q.edit_message_text("📦 Store is empty — nothing to transfer. Use *Restock* first.", parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    await q.edit_message_text(
        "🔄 *Transfer store → bar — which drink?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_store_drink_keyboard("tfr_dr"),
    )
    return _TFR_DRINK


async def _tfr_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data[7:]
    if drink == "__other__":
        await q.edit_message_text("Type the drink name:")
        return _TFR_DRINK_TEXT
    ctx.user_data["tfr_drink"] = drink
    await q.edit_message_text(
        f"🔄 *{reports._esc(drink.title())}* — how many to move to bar?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_transfer_qty_keyboard(),
    )
    return _TFR_QTY


async def _tfr_drink_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["tfr_drink"] = update.message.text.strip().lower()
    drink = ctx.user_data["tfr_drink"]
    await update.message.reply_text(
        f"🔄 *{reports._esc(drink.title())}* — how many to move to bar?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_transfer_qty_keyboard(),
    )
    return _TFR_QTY


async def _tfr_pick_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[6:]
    if val == "__other__":
        await q.edit_message_text("How many to transfer? (type a number)")
        return _TFR_QTY_TEXT
    drink = ctx.user_data.pop("tfr_drink", "")
    user  = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_transfer(drink, int(val), recorded_by=recorded_by)
    await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


async def _tfr_qty_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        qty = int(update.message.text.strip())
        if qty <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number:")
        return _TFR_QTY_TEXT
    drink = ctx.user_data.pop("tfr_drink", "")
    user  = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_transfer(drink, qty, recorded_by=recorded_by)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Add Debtor flow ────────────────────────────────────────────────────

@_require_admin
async def _deb_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "➕ *Add Debtor — which account?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_account_keyboard("deb_ac"),
    )
    return _DEB_ACCT


async def _deb_pick_acct(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    ctx.user_data["deb_acct"] = q.data.split(":")[1]
    await q.edit_message_text("👤 Debtor's name?")
    return _DEB_NAME


async def _deb_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["deb_name"] = update.message.text.strip()
    name = ctx.user_data["deb_name"]
    await update.message.reply_text(
        f"💰 *{reports._esc(name.title())}* — how much do they owe? (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _DEB_AMT


async def _deb_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    val, err = _to_float(update.message.text.strip(), "amount")
    if err:
        await update.message.reply_text("❌ Enter a valid amount:")
        return _DEB_AMT
    ctx.user_data["deb_amt"] = val
    await update.message.reply_text(
        "📝 Any note? (optional — e.g. 'tab from Friday')",
        reply_markup=_skip_keyboard("deb_note:skip"),
    )
    return _DEB_NOTE


async def _deb_note_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    ctx.user_data["deb_note"] = ""
    await q.edit_message_text(
        "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_date_keyboard("deb_d"),
    )
    return _DEB_DATE


async def _deb_note_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["deb_note"] = update.message.text.strip()
    await update.message.reply_text(
        "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_date_keyboard("deb_d"),
    )
    return _DEB_DATE


async def _deb_pick_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[6:]
    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        y, m = int(ym[:4]), int(ym[5:7])
        await q.edit_message_text(
            "📅 *When?*", parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("deb_d", y, m),
        )
        return _DEB_DATE
    if val == "cal_x":
        return _DEB_DATE
    ts = None if val == "today" else val
    acct = ctx.user_data.pop("deb_acct", "bar")
    name = ctx.user_data.pop("deb_name", "")
    amt  = ctx.user_data.pop("deb_amt", 0.0)
    note = ctx.user_data.pop("deb_note", "")
    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_add_debtor(acct, name, amt, note, timestamp=ts, recorded_by=recorded_by)
    await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Pay Debtor flow ────────────────────────────────────────────────────

@_require_admin
async def _pay_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "✅ *Pay Debtor — which account?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_account_keyboard("pay_ac"),
    )
    return _PAY_ACCT


async def _pay_pick_acct(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    acct = q.data.split(":")[1]
    ctx.user_data["pay_acct"] = acct
    debtors = db.get_debtors(account=acct)
    if not debtors:
        await q.edit_message_text(f"✅ No outstanding debtors in *{acct.title()}*.", parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    rows = []
    for d in debtors:
        rem = round(float(d["amount"]) - float(d.get("amount_paid") or 0), 2)
        rows.append([InlineKeyboardButton(
            f"{d['name'].title()} — ₦{rem:,.0f}",
            callback_data=f"pay_nm:{d['name'].lower()}",
        )])
    await q.edit_message_text(
        "✅ *Who paid?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return _PAY_NAME


async def _pay_pick_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    name = q.data[7:]
    ctx.user_data["pay_name"] = name
    acct = ctx.user_data.get("pay_acct", "bar")
    debtors = db.get_debtors(account=acct)
    debtor = next((d for d in debtors if d["name"].lower() == name), None)
    if debtor:
        rem = round(float(debtor["amount"]) - float(debtor.get("amount_paid") or 0), 2)
        await q.edit_message_text(
            f"💳 *{reports._esc(name.title())}* owes *₦{rem:,.0f}*\nHow much are they paying?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ Full — ₦{rem:,.0f}", callback_data="pay_am:full"),
                InlineKeyboardButton("✏️ Partial amount",      callback_data="pay_am:partial"),
            ]]),
        )
    else:
        await q.edit_message_text("How much are they paying? (₦)")
    return _PAY_AMT


async def _pay_pick_amt(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    if q.data == "pay_am:full":
        acct = ctx.user_data.pop("pay_acct", "bar")
        name = ctx.user_data.pop("pay_name", "")
        user = update.effective_user
        paid_by = user.username or user.first_name or str(user.id)
        ok, msg = logic.process_pay_debtor(acct, name, paid_by=paid_by, amount=None)
        await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
        return ConversationHandler.END
    await q.edit_message_text("Type the partial amount paid (₦):")
    return _PAY_AMT


async def _pay_amt_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    val, err = _to_float(update.message.text.strip(), "amount")
    if err:
        await update.message.reply_text("❌ Enter a valid amount:")
        return _PAY_AMT
    acct = ctx.user_data.pop("pay_acct", "bar")
    name = ctx.user_data.pop("pay_name", "")
    user = update.effective_user
    paid_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_pay_debtor(acct, name, paid_by=paid_by, amount=val)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Manage menu non-flow callbacks (allocation, staff) ─────────────────

# ── Delete Entry flow ─────────────────────────────────────────────────

def _recent_active(table: str, limit: int = 10) -> list[dict]:
    rows = [r for r in db.read_all(table) if not r.get("deleted_at")]
    rows.sort(key=lambda r: str(r.get("timestamp", "")), reverse=True)
    return rows[:limit]


def _del_entry_label(entry_type: str, r: dict) -> str:
    ts = str(r.get("timestamp", ""))[:10]
    if entry_type == "sale":
        return f"#{r['id']} • {str(r.get('drink_name','')).title()} ×{r.get('quantity','')} — ₦{float(r.get('total_revenue',0)):,.0f} ({ts})"
    if entry_type == "room":
        return f"#{r['id']} • {str(r.get('room_type','')).title()} ×{r.get('quantity','')}, {r.get('nights','')}n — ₦{float(r.get('total_revenue',0)):,.0f} ({ts})"
    return f"#{r['id']} • {str(r.get('account','')).title()}/{str(r.get('category','')).title()} — ₦{float(r.get('amount',0)):,.0f} ({ts})"


@_require_admin
async def _del_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🗑 *Delete Entry — what type?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🍺 Sale",    callback_data="del_tp:sale"),
            InlineKeyboardButton("🛏 Room",    callback_data="del_tp:room"),
            InlineKeyboardButton("💸 Expense", callback_data="del_tp:expense"),
        ]]),
    )
    return _DEL_TYPE


async def _del_pick_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    entry_type = q.data[7:]
    ctx.user_data["del_type"] = entry_type
    table = {"sale": "sales", "room": "rooms", "expense": "expenses"}[entry_type]
    rows = _recent_active(table)
    if not rows:
        await q.edit_message_text(f"No recent {entry_type} entries found.")
        return ConversationHandler.END
    btns = [[InlineKeyboardButton(
        _del_entry_label(entry_type, r), callback_data=f"del_en:{r['id']}"
    )] for r in rows]
    await q.edit_message_text(
        f"🗑 *Select {entry_type} to delete:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(btns),
    )
    return _DEL_ENTRY


async def _del_pick_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    entry_id = int(q.data[7:])
    ctx.user_data["del_id"] = entry_id
    entry_type = ctx.user_data.get("del_type", "")
    table = {"sale": "sales", "room": "rooms", "expense": "expenses"}.get(entry_type, "sales")
    row = next((r for r in _recent_active(table, 50) if r["id"] == entry_id), None)
    label = _del_entry_label(entry_type, row) if row else f"#{entry_id}"
    await q.edit_message_text(
        f"⚠️ *Confirm delete?*\n\n`{label}`",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, delete", callback_data="del_cf:yes"),
            InlineKeyboardButton("❌ Cancel",       callback_data="del_cf:no"),
        ]]),
    )
    return _DEL_CONFIRM


async def _del_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    if q.data == "del_cf:no":
        await q.edit_message_text("❌ Cancelled.")
        ctx.user_data.clear()
        return ConversationHandler.END
    entry_type = ctx.user_data.pop("del_type", "")
    entry_id   = ctx.user_data.pop("del_id", 0)
    user = update.effective_user
    actor = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_delete(entry_type, entry_id, actor=actor)
    await update.effective_chat.send_message(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(user.id))
    return ConversationHandler.END


# ── Activity Log flow ──────────────────────────────────────────────────

@_require_admin
async def _act_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "📋 *Activity Log — which day?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_date_keyboard("act_d"),
    )
    return _ACT_DATE


async def _act_pick_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[6:]
    if val.startswith("cal_p:") or val.startswith("cal_n:"):
        ym = val[6:]
        y, m = int(ym[:4]), int(ym[5:7])
        await q.edit_message_text(
            "📋 *Activity Log — which day?*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_calendar_keyboard("act_d", y, m),
        )
        return _ACT_DATE
    if val == "cal_x":
        return _ACT_DATE
    from datetime import datetime as _dt
    date_str = _dt.now().strftime("%Y-%m-%d") if val == "today" else val
    text = reports.generate_activity_log(date_str)
    await _reply_long_cb(q, text)
    return ConversationHandler.END


# ── Set Drink Price flow ───────────────────────────────────────────────

@_require_admin
async def _spr_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    price_rows = db.get_drink_selling_prices()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for r in price_rows:
        price = float(r["selling_price"])
        label = f"{r['drink_name'].title()} (₦{price:,.0f})" if price > 0 else f"{r['drink_name'].title()} (—)"
        row.append(InlineKeyboardButton(label, callback_data=f"spr_dr:{r['drink_name'].lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ New drink", callback_data="spr_dr:__other__")])
    await q.edit_message_text(
        "💰 *Set Drink Price — which drink?*\n_(current prices shown)_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return _SSET_PRICE_DRINK


async def _spr_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data[7:]
    if drink == "__other__":
        await q.edit_message_text("Type the drink name:")
        return _SSET_PRICE_DRINK
    ctx.user_data["spr_drink"] = drink
    await q.edit_message_text(
        f"💰 *{reports._esc(drink.title())}* — new selling price per unit? (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_PRICE_AMT


async def _spr_drink_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["spr_drink"] = update.message.text.strip().lower()
    drink = ctx.user_data["spr_drink"]
    await update.message.reply_text(
        f"💰 *{reports._esc(drink.title())}* — new selling price per unit? (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_PRICE_AMT


async def _spr_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    price, err = _to_float(update.message.text.strip(), "price")
    if err:
        await update.message.reply_text("❌ Enter a valid price (e.g. 500):")
        return _SSET_PRICE_AMT
    drink = ctx.user_data.pop("spr_drink", "")
    ok, msg = logic.process_set_price(drink, price)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


# ── Set Room Type flow ─────────────────────────────────────────────────

@_require_admin
async def _srt_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    presets = db.get_all_room_type_prices()
    rows = [[InlineKeyboardButton(
        f"{p['room_type'].title()} — ₦{int(p['price']):,}/night",
        callback_data=f"srt_tp:{p['room_type'].lower()}",
    )] for p in presets]
    rows.append([InlineKeyboardButton("➕ New room type", callback_data="srt_tp:__new__")])
    await q.edit_message_text(
        "🛏 *Room Type Prices — tap to update or add new:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return _SSET_ROOM_TYPE


async def _srt_pick_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    val = q.data[7:]
    if val == "__new__":
        await q.edit_message_text("Type the new room type name (e.g. standard, executive, suite):")
        return _SSET_ROOM_TYPE_TEXT
    ctx.user_data["srt_type"] = val
    await q.edit_message_text(
        f"🛏 *{reports._esc(val.title())}* — new price per night? (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_ROOM_AMT


async def _srt_type_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["srt_type"] = update.message.text.strip().lower()
    rtype = ctx.user_data["srt_type"]
    await update.message.reply_text(
        f"🛏 *{reports._esc(rtype.title())}* — price per night? (₦)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_ROOM_AMT


async def _srt_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    price, err = _to_float(update.message.text.strip(), "price")
    if err:
        await update.message.reply_text("❌ Enter a valid price (e.g. 15000):")
        return _SSET_ROOM_AMT
    rtype = ctx.user_data.pop("srt_type", "")
    db.set_room_type_price(rtype, price)
    await update.message.reply_text(
        f"✅ *{reports._esc(rtype.title())}* set to ₦{price:,.0f}/night.\nStaff can book with: `/room {rtype.lower()} <qty> <nights>`",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_get_keyboard(update.effective_user.id),
    )
    return ConversationHandler.END


# ── Set Low-Stock Threshold flow ───────────────────────────────────────

@_require_admin
async def _sthr_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    items = inv.get_inventory_summary()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for i in items:
        label = f"{i['drink']} (≤{i['threshold']})"
        row.append(InlineKeyboardButton(label, callback_data=f"sthr_dr:{i['drink'].lower()}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    await q.edit_message_text(
        "🔔 *Low Stock Alert — which drink?*\n_(current threshold in brackets)_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return _SSET_THRESH_DRINK


async def _sthr_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data[8:]
    ctx.user_data["sthr_drink"] = drink
    await q.edit_message_text(
        f"🔔 *{reports._esc(drink.title())}* — alert when bar stock falls to or below? (units)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_THRESH_AMT


async def _sthr_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    val, err = _to_int(update.message.text.strip(), "threshold")
    if err:
        await update.message.reply_text("❌ Enter a whole number (e.g. 6):")
        return _SSET_THRESH_AMT
    drink = ctx.user_data.pop("sthr_drink", "")
    result = inv.set_threshold(drink, val)
    await update.message.reply_text(result.message, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


# ── Fix Stock flows (rename/merge, set count, delete) ──────────────────

async def _rnm_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🔀 *Rename / Merge — which drink?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_existing_drink_kb("rnm_old"),
    )
    return _RNM_OLD


async def _rnm_pick_old(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    old = q.data.split(":", 1)[1]
    ctx.user_data["rnm_old"] = old
    await q.edit_message_text(
        f"🔀 Changing *{reports._esc(old.title())}*\n\n"
        "Type the *new name*, or tap an existing drink to *merge* into it:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_existing_drink_kb("rnm_new"),
    )
    return _RNM_NEW


async def _rnm_pick_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    new = q.data.split(":", 1)[1]
    old = ctx.user_data.pop("rnm_old", "")
    _ok, msg = logic.process_rename_drink(old, new)
    await q.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END


async def _rnm_new_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    new = update.message.text.strip()
    old = ctx.user_data.pop("rnm_old", "")
    _ok, msg = logic.process_rename_drink(old, new)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def _sst_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🔢 *Set Count — which drink?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_existing_drink_kb("sst_dr"),
    )
    return _SST_DRINK


async def _sst_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data.split(":", 1)[1]
    ctx.user_data["sst_drink"] = drink
    row = db.get_drink(drink) or {}
    store, bar = int(row.get("store_stock", 0)), int(row.get("current_stock", 0))
    await q.edit_message_text(
        f"🔢 *{reports._esc(drink.title())}* — now {store} store · {bar} bar\n\n"
        "New *STORE* count? (whole number)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SST_STORE


async def _sst_store(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text.strip()
    if not txt.isdigit():
        await update.message.reply_text("❌ Enter a whole number ≥ 0 for STORE:")
        return _SST_STORE
    ctx.user_data["sst_store"] = int(txt)
    await update.message.reply_text("New *BAR* count? (whole number)", parse_mode=ParseMode.MARKDOWN_V2)
    return _SST_BAR


async def _sst_bar(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text.strip()
    if not txt.isdigit():
        await update.message.reply_text("❌ Enter a whole number ≥ 0 for BAR:")
        return _SST_BAR
    drink = ctx.user_data.pop("sst_drink", "")
    store = ctx.user_data.pop("sst_store", 0)
    _ok, msg = logic.process_set_stock(drink, store, int(txt))
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def _ddr_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🗑 *Delete Drink — which one?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_existing_drink_kb("ddr_dr"),
    )
    return _DDR_DRINK


async def _ddr_pick_drink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = q.data.split(":", 1)[1]
    ctx.user_data["ddr_drink"] = drink
    row = db.get_drink(drink) or {}
    store, bar = int(row.get("store_stock", 0)), int(row.get("current_stock", 0))
    warn = "" if (store + bar) == 0 else f"\n⚠️ It still holds *{store} store · {bar} bar* — these will be discarded."
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Delete", callback_data="ddrc:yes"),
        InlineKeyboardButton("❌ Cancel", callback_data="ddrc:no"),
    ]])
    await q.edit_message_text(
        f"🗑 Delete *{reports._esc(drink.title())}*?{warn}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=kb,
    )
    return _DDR_CONFIRM


async def _ddr_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    drink = ctx.user_data.pop("ddr_drink", "")
    if q.data == "ddrc:no" or not drink:
        await q.edit_message_text("Cancelled.")
        return ConversationHandler.END
    _ok, msg = logic.process_delete_drink(drink, force=True)
    await q.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END


# ── Set Allocation % flow ──────────────────────────────────────────────

_ALLOC_LABELS = {
    "buffer":   "Buffer — % of gross revenue → Savings",
    "restock":  "Restock — % of gross revenue → Bar Account",
    "draw":     "Owner Draw — % of leftover profit",
    "reinvest": "Reinvestment — % of leftover profit",
    "float":    "Cash Float — % of leftover profit",
}


@_require_admin
async def _sall_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    from config import (
        ALLOC_BUFFER_DEFAULT, ALLOC_RESTOCK_DEFAULT,
        ALLOC_DRAW_DEFAULT, ALLOC_REINVEST_DEFAULT, ALLOC_FLOAT_DEFAULT,
    )
    defs = {"buffer": ALLOC_BUFFER_DEFAULT, "restock": ALLOC_RESTOCK_DEFAULT,
            "draw": ALLOC_DRAW_DEFAULT, "reinvest": ALLOC_REINVEST_DEFAULT, "float": ALLOC_FLOAT_DEFAULT}
    rows = []
    for k, label in _ALLOC_LABELS.items():
        current = int(db.get_setting(f"alloc_{k}", str(defs[k])))
        rows.append([InlineKeyboardButton(f"{label} [{current}%]", callback_data=f"sall_k:{k}")])
    await q.edit_message_text(
        "📊 *Allocation % — tap a key to change:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return _SSET_ALLOC_KEY


async def _sall_pick_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    key = q.data[7:]
    ctx.user_data["sall_key"] = key
    label = _ALLOC_LABELS.get(key, key)
    await q.edit_message_text(
        f"📊 *{label}*\nNew percentage? (0–100)",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SSET_ALLOC_VAL


async def _sall_value(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        pct = int(update.message.text.strip())
        if not (0 <= pct <= 100):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a whole number between 0 and 100:")
        return _SSET_ALLOC_VAL
    key = ctx.user_data.pop("sall_key", "")
    db.set_setting(f"alloc_{key}", str(pct))
    # Show updated summary using same logic as cmd_setallocation
    from config import (
        ALLOC_BUFFER_DEFAULT, ALLOC_RESTOCK_DEFAULT,
        ALLOC_DRAW_DEFAULT, ALLOC_REINVEST_DEFAULT, ALLOC_FLOAT_DEFAULT,
    )
    defs = {"buffer": ALLOC_BUFFER_DEFAULT, "restock": ALLOC_RESTOCK_DEFAULT,
            "draw": ALLOC_DRAW_DEFAULT, "reinvest": ALLOC_REINVEST_DEFAULT, "float": ALLOC_FLOAT_DEFAULT}
    def _g(k): return int(db.get_setting(f"alloc_{k}", str(defs[k])))
    buf, rst, drw, rei, flt = _g("buffer"), _g("restock"), _g("draw"), _g("reinvest"), _g("float")
    msg = (
        f"✅ *{key.title()}* set to *{pct}%*\n\n"
        f"*Set-asides* (of gross revenue):\n"
        f"  Buffer: {buf}%  |  Restock: {rst}%  |  Total: *{buf+rst}%*\n\n"
        f"*Profit distribution* (of leftover):\n"
        f"  Draw: {drw}%  |  Reinvest: {rei}%  |  Float: {flt}%  |  Total: *{drw+rei+flt}%*"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(update.effective_user.id))
    return ConversationHandler.END


async def _cb_manage_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    action = q.data[4:]
    user = update.effective_user

    if action == "allocation":
        from datetime import datetime as _dt
        now = _dt.now()
        text = reports.generate_allocation_report(for_month=(now.year, now.month))
        await _reply_long_cb(q, text)

    elif action == "restock_plan":
        from datetime import datetime as _dt
        now = _dt.now()
        text = reports.generate_restock_plan(for_month=(now.year, now.month))
        await _reply_long_cb(q, text)

    elif action == "fixstock":
        await q.edit_message_text(
            "🛠 *Fix Stock*\n_Correct duplicate SKUs, wrong counts, or remove a drink._",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_fixstock_menu_keyboard(),
        )

    elif action == "settings":
        await q.edit_message_text(
            "⚙️ *Settings*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_settings_menu_keyboard(),
        )

    elif action == "staff":
        staff_rows = [u for u in db.read_all("users") if u.get("role") == "staff"]
        lines = ["👥 *Staff*"]
        for u in staff_rows:
            lines.append(f"  • {u['username']} (`{u['user_id']}`)")
        if not staff_rows:
            lines.append("  _No staff yet._")
        lines.append("\n_To add: ask staff to send /start, then use /addstaff <id> <name>_")
        rows = []
        if staff_rows:
            rows = [[InlineKeyboardButton(
                f"🗑 Remove {u['username']}", callback_data=f"mgr_rm:{u['user_id']}"
            )] for u in staff_rows]
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(rows) if rows else None,
        )

    elif action == "addstaff":
        await q.edit_message_text(
            "👥 *Add Staff*\n\n"
            "Ask the staff member to open this bot and send `/start`.\n"
            "Their user ID will appear in the bot.\n\n"
            "Then run:\n`/addstaff <user_id> <username>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    elif action == "removestaff":
        staff_rows = [u for u in db.read_all("users") if u.get("role") == "staff"]
        if not staff_rows:
            await q.edit_message_text("No staff members to remove.")
            return
        rows = [[InlineKeyboardButton(
            f"🗑 {u['username']} ({u['user_id']})",
            callback_data=f"mgr_rm:{u['user_id']}",
        )] for u in staff_rows]
        await q.edit_message_text(
            "👥 *Remove Staff — tap to remove:*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(rows),
        )


async def _cb_manage_remove_staff(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    uid_str = q.data[7:]
    try:
        uid = int(uid_str)
    except ValueError:
        await q.edit_message_text("❌ Invalid user ID.")
        return
    removed = db.remove_user(uid)
    if not removed:
        await q.edit_message_text(f"❌ User `{uid}` not found.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    await q.edit_message_text(f"✅ User `{uid}` removed.", parse_mode=ParseMode.MARKDOWN_V2)


async def _cb_settings_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current daily report status with a toggle button."""
    q = update.callback_query
    await q.answer()
    jobs = ctx.application.job_queue.get_jobs_by_name("daily_report")
    is_on = bool(jobs)
    status = "🔔 *ON*" if is_on else "🔕 *OFF*"
    toggle_label = "Turn OFF" if is_on else "Turn ON"
    toggle_cb = "sdr:off" if is_on else "sdr:on"
    await q.edit_message_text(
        f"📅 *Daily Report*\nCurrent status: {status}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(toggle_label, callback_data=toggle_cb),
        ]]),
    )


async def _cb_daily_report_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    action = q.data[4:]  # strip "sdr:"
    job_name = "daily_report"
    current_jobs = ctx.application.job_queue.get_jobs_by_name(job_name)

    if action == "off":
        for job in current_jobs:
            job.schedule_removal()
        await q.edit_message_text("🔕 Daily report *disabled*.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    if current_jobs:
        await q.edit_message_text("✅ Daily report is already *enabled*.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    bot_cfg = ctx.application.bot_data
    from config import REPORT_CHAT_ID, HOTEL_SCHEMA, TIMEZONE, DAILY_REPORT_TIME
    chat_id = bot_cfg.get("report_chat_id") or REPORT_CHAT_ID or update.effective_chat.id
    schema   = bot_cfg.get("schema")   or HOTEL_SCHEMA
    tz_str   = bot_cfg.get("timezone") or TIMEZONE
    time_str = bot_cfg.get("daily_report_time") or DAILY_REPORT_TIME
    _schedule_daily_report(ctx.application.job_queue, chat_id, schema, tz_str, time_str)
    await q.edit_message_text(
        f"✅ Daily report *enabled* — sends at {time_str} ({tz_str}).",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


@_require_auth
async def _btn_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    text = reports.generate_stock_report(staff_view=not _is_admin(uid))
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=_get_keyboard(uid))


# ── Report / Summary date-picker keyboard ────────────────────────────

def _report_date_keyboard(rtype: str, year: int, month: int) -> InlineKeyboardMarkup:
    """Calendar picker with Today / This Month / All Time quick buttons."""
    today = date.today()
    rows: list[list[InlineKeyboardButton]] = []

    rows.append([
        InlineKeyboardButton("✅ Today",      callback_data=f"rpd:{rtype}:today"),
        InlineKeyboardButton("📅 This Month", callback_data=f"rpd:{rtype}:month"),
        InlineKeyboardButton("🔁 All Time",   callback_data=f"rpd:{rtype}:all"),
    ])

    first = date(year, month, 1)
    prev = (first - timedelta(days=1)).replace(day=1)
    nxt_year, nxt_month = (year + 1, 1) if month == 12 else (year, month + 1)
    is_current = (year == today.year and month == today.month)
    rows.append([
        InlineKeyboardButton("◀", callback_data=f"rpd:{rtype}:cal_p:{prev.year:04d}-{prev.month:02d}"),
        InlineKeyboardButton(f"📅 {_cal.month_abbr[month]} {year}", callback_data=f"rpd:{rtype}:cal_m:{year:04d}-{month:02d}"),
        InlineKeyboardButton(
            "·" if is_current else "▶",
            callback_data=f"rpd:{rtype}:cal_x" if is_current else f"rpd:{rtype}:cal_n:{nxt_year:04d}-{nxt_month:02d}",
        ),
    ])

    rows.append([InlineKeyboardButton(d, callback_data=f"rpd:{rtype}:cal_x") for d in ["M", "T", "W", "T", "F", "S", "S"]])

    for week in _cal.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data=f"rpd:{rtype}:cal_x"))
            else:
                d = date(year, month, day)
                if d > today:
                    row.append(InlineKeyboardButton("·", callback_data=f"rpd:{rtype}:cal_x"))
                else:
                    label = f"[{day}]" if d == today else str(day)
                    row.append(InlineKeyboardButton(label, callback_data=f"rpd:{rtype}:{d.isoformat()}"))
        rows.append(row)

    return InlineKeyboardMarkup(rows)


def _report_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Full Report",    callback_data="rep:full")],
        [InlineKeyboardButton("🍺 Sales Report",   callback_data="rep:sales")],
        [InlineKeyboardButton("💸 Expense Report", callback_data="rep:expense")],
        [InlineKeyboardButton("👥 Staff Report",   callback_data="rep:staff")],
    ])


def _run_report(rtype: str, for_date=None, for_month=None, all_time: bool = False) -> str:
    from datetime import datetime
    staff_view = False
    if rtype == "full":
        if for_date:
            return reports.generate_full_report(for_date=for_date, staff_view=staff_view)
        if for_month:
            return reports.generate_full_report(for_month=for_month, staff_view=staff_view)
        return reports.generate_full_report(all_time=all_time, staff_view=staff_view)
    if rtype == "sales":
        if for_date:
            return reports.generate_sales_report(for_date=for_date)
        if for_month:
            return reports.generate_sales_report(for_month=for_month)
        return reports.generate_sales_report(all_time=all_time)
    if rtype == "expense":
        if for_date:
            return reports.generate_expense_report(for_date=for_date)
        if for_month:
            return reports.generate_expense_report(for_month=for_month)
        return reports.generate_expense_report(all_time=all_time)
    if rtype == "staff":
        if for_date:
            return reports.generate_staff_report(for_date=for_date)
        if for_month:
            return reports.generate_staff_report(for_month=for_month)
        now = datetime.now()
        return reports.generate_staff_report(for_month=(now.year, now.month))
    return "❌ Unknown report type."


# ── 📋 Report button handlers ─────────────────────────────────────────

@_require_admin
async def _btn_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📋 *Which report?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_report_type_keyboard(),
    )


async def _cb_report_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    if not _is_admin(q.from_user.id):
        await q.message.reply_text("🔒 Admin only.")
        return
    rtype = q.data[4:]  # strip "rep:"
    labels = {"full": "Full", "sales": "Sales", "expense": "Expense", "staff": "Staff"}
    today = date.today()
    await q.edit_message_text(
        f"📅 *{labels.get(rtype, rtype.title())} Report — select period:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_report_date_keyboard(rtype, today.year, today.month),
    )


async def _cb_report_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    q = update.callback_query
    await q.answer()

    parts = q.data.split(":")
    # "rpd:{rtype}:{choice}" or "rpd:{rtype}:cal_p:{YYYY-MM}"
    rtype = parts[1]
    choice = parts[2]
    is_summary = rtype == "summary"

    # Summary is staff-accessible; all other report types are admin-only
    if is_summary:
        if not _is_authorized(q.from_user.id):
            await q.message.reply_text("🔒 Access denied.")
            return
    else:
        if not _is_admin(q.from_user.id):
            await q.message.reply_text("🔒 Admin only.")
            return

    staff_view = not _is_admin(q.from_user.id)

    if choice == "cal_x":
        return

    if choice in ("cal_p", "cal_n"):
        ym = parts[3]
        year, month = int(ym[:4]), int(ym[5:7])
        header = "📊 *Summary — select period:*" if is_summary else (
            f"📅 *{'Full' if rtype == 'full' else rtype.title()} Report — select period:*"
        )
        await q.edit_message_text(
            header,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_report_date_keyboard(rtype, year, month),
        )
        return

    today = date.today()
    if choice == "cal_m":
        ym = parts[3]
        year, month = int(ym[:4]), int(ym[5:7])
        if is_summary:
            text = reports.generate_full_report(for_month=(year, month), staff_view=staff_view)
        else:
            text = _run_report(rtype, for_month=(year, month))
    elif choice == "today":
        if is_summary:
            text = reports.generate_daily_summary(target=None, staff_view=staff_view)
        else:
            text = _run_report(rtype, for_date=today)
    elif choice == "month":
        if is_summary:
            text = reports.generate_full_report(for_month=(today.year, today.month), staff_view=staff_view)
        else:
            text = _run_report(rtype, for_month=(today.year, today.month))
    elif choice == "all":
        if is_summary:
            text = reports.generate_full_report(all_time=True, staff_view=staff_view)
        else:
            text = _run_report(rtype, all_time=True)
    elif _DATE_RE.match(choice):
        dt = datetime.strptime(choice, "%Y-%m-%d").date()
        if is_summary:
            text = reports.generate_daily_summary(target=dt, staff_view=staff_view)
        else:
            text = _run_report(rtype, for_date=dt)
    else:
        return
    await _reply_long_cb(q, text)


# ── 📊 Summary button handler ─────────────────────────────────────────

@_require_auth
async def _btn_summary(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    today = date.today()
    await update.message.reply_text(
        "📊 *Summary — select period:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_report_date_keyboard("summary", today.year, today.month),
    )


@_require_auth
async def _btn_debtors(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    staff_list = db.get_all_staff()
    buttons: list[list[InlineKeyboardButton]] = []
    for name in staff_list:
        buttons.append([InlineKeyboardButton(f"👤 {name.title()}", callback_data=f"dbt:staff:{name}")])
    buttons.append([InlineKeyboardButton("📋 All Debtors", callback_data="dbt:all")])
    await update.message.reply_text(
        "💳 *Debtors — View by:*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _cb_debtors_filter(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    if not _is_authorized(q.from_user.id):
        await q.message.reply_text("🔒 Access denied.")
        return
    data = q.data
    is_admin = _is_admin(q.from_user.id)
    if data == "dbt:all":
        text = reports.generate_debtors_report(staff_view=not is_admin)
    elif data.startswith("dbt:staff:"):
        staff_name = data[len("dbt:staff:"):]
        text = reports.generate_staff_debtors(staff_name)
    else:
        return
    await _reply_long_cb(q, text)


# ── /setup wizard ─────────────────────────────────────────────────────

async def _cmd_setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    # Only a hardcoded admin (ADMIN_IDS) can run setup — the users table may not
    # exist yet or may be empty on a brand-new deployment.
    if uid not in ADMIN_IDS:
        await update.message.reply_text(
            "🔒 Only the configured bot owner can run /setup.\n"
            "Add your Telegram ID to ADMIN\\_IDS in the deployment environment.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return ConversationHandler.END

    existing = db.get_setting("hotel_name")
    if existing:
        await update.message.reply_text(
            f"✅ Already set up as *{reports._esc(existing)}*\\.\n"
            "Run /help to see all available commands\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return ConversationHandler.END

    ctx.user_data["setup_username"] = update.effective_user.username or str(uid)
    await update.message.reply_text(
        "👋 *Welcome to Hotel Bot Setup!*\n\n"
        "Let's configure your hotel in a few steps.\n\n"
        "What is your hotel's name?",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return _SETUP_NAME


async def _setup_receive_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()
    if len(name) < 2:
        await update.message.reply_text("Please enter a valid hotel name (at least 2 characters).")
        return _SETUP_NAME

    ctx.user_data["setup_name"] = name
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇳🇬 Africa/Lagos",        callback_data="stz:Africa/Lagos"),
            InlineKeyboardButton("🇬🇭 Africa/Accra",        callback_data="stz:Africa/Accra"),
        ],
        [
            InlineKeyboardButton("🇰🇪 Africa/Nairobi",      callback_data="stz:Africa/Nairobi"),
            InlineKeyboardButton("🇿🇦 Africa/Johannesburg", callback_data="stz:Africa/Johannesburg"),
        ],
        [InlineKeyboardButton("✏️ Enter manually",          callback_data="stz:manual")],
    ])
    await update.message.reply_text(
        f"*{reports._esc(name)}* ✓\n\nSelect your timezone:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
    )
    return _SETUP_TZ


async def _setup_tz_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    choice = q.data[len("stz:"):]

    if choice == "manual":
        await q.message.reply_text(
            "Type your timezone — e.g. `Europe/London`, `America/New_York`:",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return _SETUP_TZ

    ctx.user_data["setup_tz"] = choice
    await _show_setup_confirm(q.message, ctx)
    return _SETUP_CONFIRM


async def _setup_tz_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    tz_input = update.message.text.strip()
    try:
        pytz.timezone(tz_input)
    except pytz.exceptions.UnknownTimeZoneError:
        await update.message.reply_text(
            f"❌ Unknown timezone `{tz_input}`\\. Try again — e.g. `Africa/Lagos`:",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return _SETUP_TZ

    ctx.user_data["setup_tz"] = tz_input
    await _show_setup_confirm(update.message, ctx)
    return _SETUP_CONFIRM


async def _show_setup_confirm(message, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    name = ctx.user_data["setup_name"]
    tz   = ctx.user_data["setup_tz"]
    uname = ctx.user_data.get("setup_username", "you")
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data="sconf:yes"),
        InlineKeyboardButton("❌ Cancel",  callback_data="sconf:no"),
    ]])
    await message.reply_text(
        f"*Ready to set up:*\n\n"
        f"🏨 Hotel: {reports._esc(name)}\n"
        f"🕐 Timezone: {reports._esc(tz)}\n"
        f"👤 Admin: @{reports._esc(uname)}\n\n"
        "Confirm?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
    )


async def _setup_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()

    if q.data == "sconf:no":
        ctx.user_data.clear()
        await q.message.reply_text("Setup cancelled. Run /setup to start again.")
        return ConversationHandler.END

    name  = ctx.user_data["setup_name"]
    tz    = ctx.user_data["setup_tz"]
    user  = q.from_user

    db.set_setting("hotel_name", name)
    db.set_setting("timezone", tz)
    db.upsert_user(user.id, user.username or str(user.id), role="admin")
    db.register_hotel_details(name, user.id, tz)

    ctx.user_data.clear()
    await q.message.reply_text(
        f"✅ *{reports._esc(name)}* is ready\\!\n\n"
        f"You're registered as admin\\.\n"
        f"Add your staff: /addstaff `<user_id>` `<username>`\n"
        f"Run /help to see all commands\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=_get_keyboard(user.id),
    )
    return ConversationHandler.END


# ── Main ──────────────────────────────────────────────────────────────

def _register_handlers(app: Application, schema: str, admin_ids: list[int]) -> None:
    """Register all command/callback handlers on app. Called once per hotel bot."""

    # ── Schema + admin-IDs setter (group -1 — runs before any other handler) ──
    async def _set_context(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        db._hotel_schema_var.set(schema)
        _active_admin_ids.set(admin_ids)

    app.add_handler(TypeHandler(Update, _set_context), group=-1)

    # Register handlers — ConversationHandlers FIRST so they capture their entry points
    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", _cmd_setup)],
        states={
            _SETUP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, _setup_receive_name)],
            _SETUP_TZ: [
                CallbackQueryHandler(_setup_tz_pick, pattern="^stz:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _setup_tz_text),
            ],
            _SETUP_CONFIRM: [CallbackQueryHandler(_setup_confirm, pattern="^sconf:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    app.add_handler(setup_conv)

    sell_conv = ConversationHandler(
        entry_points=[
            CommandHandler("sell", cmd_sell_start),
            MessageHandler(filters.Text(["🍺 Sell"]) & ~filters.COMMAND, cmd_sell_start),
        ],
        states={
            _SELL_DRINK:      [CallbackQueryHandler(_sell_pick_drink, pattern="^sd:")],
            _SELL_DRINK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _sell_drink_text)],
            _SELL_QTY:        [CallbackQueryHandler(_sell_pick_qty, pattern="^sq:")],
            _SELL_QTY_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _sell_qty_text)],
            _SELL_DATE:       [CallbackQueryHandler(_sell_pick_date, pattern="^sdd:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    book_conv = ConversationHandler(
        entry_points=[
            CommandHandler("book", cmd_book_start),
            MessageHandler(filters.Text(["🛏 Book Room"]) & ~filters.COMMAND, cmd_book_start),
        ],
        states={
            _BOOK_TYPE:        [CallbackQueryHandler(_book_pick_type, pattern="^bt:")],
            _BOOK_TYPE_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _book_type_text)],
            _BOOK_QTY:         [CallbackQueryHandler(_book_pick_qty, pattern="^bq:")],
            _BOOK_QTY_TEXT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, _book_qty_text)],
            _BOOK_NIGHTS:      [CallbackQueryHandler(_book_pick_nights, pattern="^bn:")],
            _BOOK_NIGHTS_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _book_nights_text)],
            _BOOK_DATE:        [CallbackQueryHandler(_book_pick_date, pattern="^bdd:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    app.add_handler(sell_conv)
    app.add_handler(book_conv)

    # ── Admin manage conversation flows (registered before simple callbacks) ──
    exp_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_exp_start, pattern="^mgr:expense$")],
        states={
            _EXP_ACCT:     [CallbackQueryHandler(_exp_pick_acct, pattern="^exp_ac:")],
            _EXP_CAT:      [CallbackQueryHandler(_exp_pick_cat,  pattern="^exp_cat:")],
            _EXP_CAT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _exp_cat_text)],
            _EXP_AMT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, _exp_amount)],
            _EXP_NOTE:     [
                CallbackQueryHandler(_exp_note_skip, pattern="^exp_note:skip$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _exp_note_text),
            ],
            _EXP_DATE:     [CallbackQueryHandler(_exp_pick_date, pattern="^exp_d:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    rst_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_rst_start, pattern="^mgr:restock$")],
        states={
            _RST_DRINK:      [CallbackQueryHandler(_rst_pick_drink, pattern="^rst_dr:")],
            _RST_DRINK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _rst_drink_text)],
            _RST_QTY:        [CallbackQueryHandler(_rst_pick_qty,   pattern="^rst_q:")],
            _RST_QTY_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _rst_qty_text)],
            _RST_COST:       [MessageHandler(filters.TEXT & ~filters.COMMAND, _rst_cost)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    tfr_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_tfr_start, pattern="^mgr:transfer$")],
        states={
            _TFR_DRINK:      [CallbackQueryHandler(_tfr_pick_drink, pattern="^tfr_dr:")],
            _TFR_DRINK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _tfr_drink_text)],
            _TFR_QTY:        [CallbackQueryHandler(_tfr_pick_qty,   pattern="^tfr_q:")],
            _TFR_QTY_TEXT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _tfr_qty_text)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    deb_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_deb_start, pattern="^mgr:add_debtor$")],
        states={
            _DEB_ACCT:  [CallbackQueryHandler(_deb_pick_acct, pattern="^deb_ac:")],
            _DEB_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, _deb_name)],
            _DEB_AMT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _deb_amount)],
            _DEB_NOTE:  [
                CallbackQueryHandler(_deb_note_skip, pattern="^deb_note:skip$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _deb_note_text),
            ],
            _DEB_DATE:  [CallbackQueryHandler(_deb_pick_date, pattern="^deb_d:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    pay_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_pay_start, pattern="^mgr:pay_debtor$")],
        states={
            _PAY_ACCT: [CallbackQueryHandler(_pay_pick_acct, pattern="^pay_ac:")],
            _PAY_NAME: [CallbackQueryHandler(_pay_pick_name, pattern="^pay_nm:")],
            _PAY_AMT:  [
                CallbackQueryHandler(_pay_pick_amt,  pattern="^pay_am:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _pay_amt_text),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    del_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_del_start, pattern="^mgr:delete$")],
        states={
            _DEL_TYPE:    [CallbackQueryHandler(_del_pick_type,  pattern="^del_tp:")],
            _DEL_ENTRY:   [CallbackQueryHandler(_del_pick_entry, pattern="^del_en:")],
            _DEL_CONFIRM: [CallbackQueryHandler(_del_confirm,    pattern="^del_cf:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    act_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_act_start, pattern="^mgr:activity$")],
        states={
            _ACT_DATE: [CallbackQueryHandler(_act_pick_date, pattern="^act_d:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    spr_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_spr_start, pattern="^sset:price$")],
        states={
            _SSET_PRICE_DRINK: [
                CallbackQueryHandler(_spr_pick_drink, pattern="^spr_dr:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _spr_drink_text),
            ],
            _SSET_PRICE_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _spr_amount)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    srt_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_srt_start, pattern="^sset:roomtype$")],
        states={
            _SSET_ROOM_TYPE:      [CallbackQueryHandler(_srt_pick_type, pattern="^srt_tp:")],
            _SSET_ROOM_TYPE_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, _srt_type_text)],
            _SSET_ROOM_AMT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, _srt_amount)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    sthr_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_sthr_start, pattern="^sset:threshold$")],
        states={
            _SSET_THRESH_DRINK: [CallbackQueryHandler(_sthr_pick_drink, pattern="^sthr_dr:")],
            _SSET_THRESH_AMT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _sthr_amount)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    sall_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_sall_start, pattern="^sset:allocation$")],
        states={
            _SSET_ALLOC_KEY: [CallbackQueryHandler(_sall_pick_key, pattern="^sall_k:")],
            _SSET_ALLOC_VAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, _sall_value)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    rnm_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_rnm_start, pattern="^fix:rename$")],
        states={
            _RNM_OLD: [CallbackQueryHandler(_rnm_pick_old, pattern="^rnm_old:")],
            _RNM_NEW: [
                CallbackQueryHandler(_rnm_pick_new, pattern="^rnm_new:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _rnm_new_text),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    sst_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_sst_start, pattern="^fix:setstock$")],
        states={
            _SST_DRINK: [CallbackQueryHandler(_sst_pick_drink, pattern="^sst_dr:")],
            _SST_STORE: [MessageHandler(filters.TEXT & ~filters.COMMAND, _sst_store)],
            _SST_BAR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, _sst_bar)],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    ddr_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(_ddr_start, pattern="^fix:delete$")],
        states={
            _DDR_DRINK:   [CallbackQueryHandler(_ddr_pick_drink, pattern="^ddr_dr:")],
            _DDR_CONFIRM: [CallbackQueryHandler(_ddr_confirm, pattern="^ddrc:")],
        },
        fallbacks=[CommandHandler("cancel", _cancel_conv)],
        allow_reentry=True,
    )
    for conv in (exp_conv, rst_conv, tfr_conv, deb_conv, pay_conv,
                 del_conv, act_conv, spr_conv, srt_conv, sthr_conv, sall_conv,
                 rnm_conv, sst_conv, ddr_conv):
        app.add_handler(conv)

    app.add_handler(MessageHandler(filters.Text(["⚙️ Manage"]) & ~filters.COMMAND, _btn_manage))
    app.add_handler(MessageHandler(filters.Text(["📊 Summary"]) & ~filters.COMMAND, _btn_summary))
    app.add_handler(MessageHandler(filters.Text(["📋 Report"]) & ~filters.COMMAND, _btn_report))
    app.add_handler(MessageHandler(filters.Text(["💰 Prices"]) & ~filters.COMMAND, cmd_prices))
    app.add_handler(MessageHandler(filters.Text(["📦 Stock"]) & ~filters.COMMAND, _btn_stock))
    app.add_handler(MessageHandler(filters.Text(["📜 History"]) & ~filters.COMMAND, _btn_history))
    app.add_handler(MessageHandler(filters.Text(["💳 Debtors"]) & ~filters.COMMAND, _btn_debtors))
    app.add_handler(CallbackQueryHandler(_cb_report_type, pattern="^rep:"))
    app.add_handler(CallbackQueryHandler(_cb_report_date, pattern="^rpd:"))
    app.add_handler(CallbackQueryHandler(_cb_debtors_filter, pattern="^dbt:"))
    app.add_handler(CallbackQueryHandler(_cb_undo_inline, pattern="^undo:"))
    app.add_handler(CallbackQueryHandler(_cb_history_date, pattern="^hst:"))
    app.add_handler(CallbackQueryHandler(_cb_manage_menu, pattern="^mgr:(allocation|restock_plan|fixstock|settings|staff|addstaff|removestaff)$"))
    app.add_handler(CallbackQueryHandler(_cb_manage_remove_staff, pattern="^mgr_rm:"))
    app.add_handler(CallbackQueryHandler(_cb_settings_menu, pattern="^sset:dailyreport$"))
    app.add_handler(CallbackQueryHandler(_cb_daily_report_toggle, pattern="^sdr:"))

    app.add_handler(CommandHandler("setup", _cmd_setup))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("sell_drink", cmd_sell_drink))
    app.add_handler(CommandHandler("setprice", cmd_setprice))
    app.add_handler(CommandHandler("setroomtype", cmd_setroomtype))
    app.add_handler(CommandHandler("prices", cmd_prices))
    app.add_handler(CommandHandler("undo", cmd_undo))
    app.add_handler(CommandHandler("restock", cmd_restock))
    app.add_handler(CommandHandler("room", cmd_room))
    app.add_handler(CommandHandler("expense", cmd_expense))
    app.add_handler(CommandHandler("add_debtor", cmd_add_debtor))
    app.add_handler(CommandHandler("pay_debtor", cmd_pay_debtor))
    app.add_handler(CommandHandler("pay_debt", cmd_pay_debt))
    app.add_handler(CommandHandler("debtor_staff", cmd_debtor_staff))
    app.add_handler(CommandHandler("set_debt_staff", cmd_set_debt_staff))
    app.add_handler(CommandHandler("debtor_history", cmd_debtor_history))
    app.add_handler(CommandHandler("debtor", cmd_debtor))
    app.add_handler(CommandHandler("debtors", cmd_debtors))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("sales_report", cmd_sales_report))
    app.add_handler(CommandHandler("expense_report", cmd_expense_report))
    app.add_handler(CommandHandler("staff_report", cmd_staff_report))
    app.add_handler(CommandHandler("activity", cmd_activity))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("allocation", cmd_allocation))
    app.add_handler(CommandHandler("setallocation", cmd_setallocation))
    app.add_handler(CommandHandler("stock", cmd_stock))
    app.add_handler(CommandHandler("restock_plan", cmd_restock_plan))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("transfer", cmd_transfer))
    app.add_handler(CommandHandler("renamedrink", cmd_renamedrink))
    app.add_handler(CommandHandler("setstock", cmd_setstock))
    app.add_handler(CommandHandler("deletedrink", cmd_deletedrink))
    app.add_handler(CommandHandler("setthreshold", cmd_setthreshold))
    app.add_handler(CommandHandler("addstaff", cmd_addstaff))
    app.add_handler(CommandHandler("removestaff", cmd_removestaff))
    app.add_handler(CommandHandler("dailyreport", cmd_dailyreport))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("hotels", cmd_hotels))
    app.add_handler(CommandHandler("addhotel", cmd_addhotel))
    app.add_handler(CommandHandler("setexpiry", cmd_setexpiry))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("suspend", cmd_suspend))
    app.add_handler(CommandHandler("unsuspend", cmd_unsuspend))
    app.add_error_handler(_error_handler)


def _build_app(
    token: str,
    schema: str,
    admin_ids: list[int],
    report_chat_id: int | None = None,
    timezone: str = "Africa/Lagos",
    daily_report_time: str = "23:00",
    is_owner_bot: bool = False,
) -> Application:
    """Build and configure a fully wired Application for one hotel."""
    # Use _MD2Bot so every outgoing MarkdownV2 message is auto-escaped at one
    # chokepoint. A sized request pool keeps send concurrency comparable to the
    # ApplicationBuilder default (ExtBot's own default pool is very small).
    app = (
        Application.builder()
        .bot(_MD2Bot(token, request=HTTPXRequest(connection_pool_size=256)))
        .build()
    )
    app.bot_data.update({
        "schema": schema,
        "admin_ids": admin_ids,
        "report_chat_id": report_chat_id,
        "timezone": timezone,
        "daily_report_time": daily_report_time,
    })
    _register_handlers(app, schema, admin_ids)
    if report_chat_id:
        _schedule_daily_report(app.job_queue, report_chat_id, schema, timezone, daily_report_time)
    if is_owner_bot:
        # Run subscription expiry check once daily at 09:00 Lagos time
        tz = pytz.timezone(timezone)
        app.job_queue.run_daily(
            _check_subscriptions,
            time=dtime(hour=9, minute=0, tzinfo=tz),
            name="subscription_check",
        )
        logger.info("Subscription expiry check scheduled daily at 09:00 %s", timezone)
    return app


async def _run_bot(app: Application, schema: str) -> None:
    async with app:
        await app.start()
        await app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES, drop_pending_updates=True
        )
        logger.info("Bot running for schema=%s", schema)
        await asyncio.Event().wait()  # block until cancelled


def main() -> None:
    # ── Bootstrap: run init_db once using env-var credentials ──────────
    if BOT_TOKEN and HOTEL_SCHEMA:
        db.init_db()
        db.validate_hotel_schema()
        # Register the env-var hotel's token so the multi-bot runner sees it
        admin_ids_str = ",".join(str(i) for i in ADMIN_IDS)
        db.add_hotel_config(HOTEL_SCHEMA, BOT_TOKEN, admin_ids_str)

    # ── Load all hotel configs from DB ──────────────────────────────────
    configs = db.get_all_hotel_configs()
    if not configs:
        raise RuntimeError(
            "No hotel configs found. Set BOT_TOKEN + HOTEL_SCHEMA env vars for the first hotel."
        )

    # ── Build one Application per hotel ────────────────────────────────
    apps: list[tuple[Application, str]] = []
    for cfg in configs:
        raw_ids = [x.strip() for x in (cfg["admin_ids"] or "").split(",") if x.strip().isdigit()]
        ids = [int(x) for x in raw_ids]
        # Ensure tables exist for this hotel schema
        db.init_db(schema=cfg["schema"], token=cfg["token"])
        app = _build_app(
            token=cfg["token"],
            schema=cfg["schema"],
            admin_ids=ids,
            report_chat_id=REPORT_CHAT_ID if cfg["schema"] == HOTEL_SCHEMA else None,
            timezone=TIMEZONE,
            daily_report_time=DAILY_REPORT_TIME,
            is_owner_bot=cfg["schema"] == HOTEL_SCHEMA,
        )
        apps.append((app, cfg["schema"]))
        _hotel_apps[cfg["schema"]] = app
        logger.info("Loaded hotel schema=%s", cfg["schema"])

    # ── Run all bots concurrently ───────────────────────────────────────
    async def _run_all() -> None:
        tasks = [
            asyncio.create_task(_run_bot(app, schema))
            for app, schema in apps
        ]
        loop = asyncio.get_running_loop()

        def _shutdown() -> None:
            asyncio.ensure_future(_notify_and_cancel(tasks))

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_and_cancel(tasks) -> None:
        try:
            await _broadcast_all(
                "⚠️ *Bot is restarting for maintenance.* We'll be back in a moment."
            )
        except Exception:
            pass
        for t in tasks:
            t.cancel()

    asyncio.run(_run_all())


if __name__ == "__main__":
    main()
