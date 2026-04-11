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
  /sales_report [today|YYYY-MM-DD|YYYY-MM|all] — Sales breakdown by drink
  /expense_report [today|YYYY-MM-DD|YYYY-MM|all] — Expense breakdown
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
  /staff_report [today|YYYY-MM-DD|YYYY-MM]   — Sales per staff member
  /setthreshold <drink> <amount>             — Set low-stock alert threshold
  /addstaff <user_id> <username>             — Grant staff access
  /removestaff <user_id>                     — Revoke access
  /dailyreport on|off                        — Toggle scheduled reports
"""
from __future__ import annotations

import logging
import os
import re
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
            + _help_text(is_admin=True),
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


def _help_text(is_admin: bool = False) -> str:
    staff_cmds = (
        "*Staff Commands:*\n"
        "`/sell_drink <drink> <qty> [YYYY-MM-DD]`\n"
        "`/room <type> <qty> <price> <nights> [YYYY-MM-DD]`\n"
        "`/debtors` | `/debtors bar` | `/debtors rooms`\n"
        "`/history` | `/history YYYY-MM-DD`\n"
        "`/report` — current month revenue summary\n"
        "`/report today` | `/report YYYY-MM-DD` | `/report YYYY-MM` | `/report all`\n"
        "`/sales_report` — drinks sold per item\n"
        "`/summary` | `/summary YYYY-MM-DD` — daily overview\n"
        "`/stock` — bar stock levels\n"
        "`/prices` — current drink price list\n"
        "`/undo` — undo your last entry (within 2 minutes, admin is notified)"
    )
    if not is_admin:
        return staff_cmds
    admin_cmds = (
        "\n\n*Admin Commands:*\n"
        "`/expense <room|bar> <category> <amount> [note] [YYYY-MM-DD]`\n"
        "`/add_debtor <room|bar> <name> <amount> [note] [YYYY-MM-DD]`\n"
        "`/pay_debtor <room|bar> <name>`\n"
        "`/restock <drink> <qty> <cost_price>`\n"
        "`/transfer <drink> <qty>` — move store → bar\n"
        "`/delete <sale|room|expense> <id>`\n"
        "`/setprice <drink> <price>` — set canonical selling price\n"
        "`/sales_report` — full sales breakdown with cost & profit\n"
        "`/expense_report` — expense breakdown by category\n"
        "`/staff_report` | `/staff_report today` | `/staff_report YYYY-MM`\n"
        "`/allocation` | `/allocation today` | `/allocation YYYY-MM` | `/allocation all`\n"
        "`/setallocation <buffer|restock|draw|reinvest|float> <percent>`\n"
        "`/setthreshold <drink> <amount>`\n"
        "`/addstaff <user_id> <username>`\n"
        "`/removestaff <user_id>`\n"
        "`/dailyreport on|off`"
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
                        parse_mode=ParseMode.MARKDOWN,
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


# ── /prices ──────────────────────────────────────────────────────────

@_require_auth
async def cmd_prices(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = reports.generate_price_list()
    await _reply(update, text)


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
                        text=f"↩️ *Undo Alert*\n@{recorded_by} reversed an entry:\n_{msg}_",
                        parse_mode=ParseMode.MARKDOWN,
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

    ok, msg = logic.process_restock(drink, qty, cost)
    await _reply(update, msg)


# ── /room ─────────────────────────────────────────────────────────────

@_require_auth
async def cmd_room(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 4:
        await _reply(
            update,
            "Usage: `/room <type> <qty> <price> <nights> [YYYY-MM-DD]`\n"
            "Example: `/room standard 2 15000 3`\n"
            "Backdate: `/room standard 2 15000 3 2025-03-10`",
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

    user = update.effective_user
    recorded_by = user.username or user.first_name or str(user.id)
    ok, msg = logic.process_room_sale(room_type, qty, price, nights, timestamp=ts, recorded_by=recorded_by)
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

    ok, msg = logic.process_expense(account, category, amount, description, timestamp=ts)
    await _reply(update, msg)


# ── /add_debtor ───────────────────────────────────────────────────────

@_require_admin
async def cmd_add_debtor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args, ts = _extract_date(_parse_args(ctx))
    if len(args) < 3:
        await _reply(
            update,
            "Usage: `/add_debtor <room|bar> <name> <amount> [note] [YYYY-MM-DD]`\n"
            "Example: `/add_debtor bar john 2500`\n"
            "Example: `/add_debtor rooms emeka 45000 room 12 unpaid`\n"
            "Backdate: `/add_debtor bar john 2500 2025-03-15`",
        )
        return

    account = args[0]
    name = args[1]
    amount, err = _to_float(args[2], "amount")
    if err:
        await _reply(update, err)
        return
    description = " ".join(args[3:]) if len(args) > 3 else ""

    ok, msg = logic.process_add_debtor(account, name, amount, description, timestamp=ts)
    await _reply(update, msg)


# ── /pay_debtor ───────────────────────────────────────────────────────

@_require_admin
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

    await _reply(update, text)


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
    await _reply(update, "\n".join(lines))


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

    ok, msg = logic.process_delete(entry_type, entry_id)
    await _reply(update, msg)


# ── /stock ────────────────────────────────────────────────────────────

@_require_auth
async def cmd_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    is_admin = _is_admin(update.effective_user.id)
    text = reports.generate_stock_report(staff_view=not is_admin)
    await _reply(update, text)


# ── /sales_report ─────────────────────────────────────────────────────

@_require_auth
async def cmd_sales_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from datetime import datetime
    args = _parse_args(ctx)
    arg = args[0].lower() if args else ""
    staff_view = not _is_admin(update.effective_user.id)

    if not arg:
        now = datetime.now()
        text = reports.generate_sales_report(for_month=(now.year, now.month), staff_view=staff_view)
    elif arg == "today":
        text = reports.generate_sales_report(for_date=datetime.now().date(), staff_view=staff_view)
    elif arg == "all":
        text = reports.generate_sales_report(all_time=True, staff_view=staff_view)
    else:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            text = reports.generate_sales_report(for_date=dt.date(), staff_view=staff_view)
        except ValueError:
            try:
                dt = datetime.strptime(arg, "%Y-%m")
                text = reports.generate_sales_report(for_month=(dt.year, dt.month), staff_view=staff_view)
            except ValueError:
                await _reply(update, "Usage: `/sales_report` | `/sales_report today` | `/sales_report YYYY-MM-DD` | `/sales_report YYYY-MM` | `/sales_report all`")
                return
    await _reply(update, text)


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
    await _reply(update, text)


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
    await _reply(update, text)


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
    await _reply(update, text)


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
    await _reply(update, text)


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

    ok, msg = logic.process_transfer(drink, qty)
    await _reply(update, msg)


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
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("sell_drink", cmd_sell_drink))
    app.add_handler(CommandHandler("setprice", cmd_setprice))
    app.add_handler(CommandHandler("prices", cmd_prices))
    app.add_handler(CommandHandler("undo", cmd_undo))
    app.add_handler(CommandHandler("restock", cmd_restock))
    app.add_handler(CommandHandler("room", cmd_room))
    app.add_handler(CommandHandler("expense", cmd_expense))
    app.add_handler(CommandHandler("add_debtor", cmd_add_debtor))
    app.add_handler(CommandHandler("pay_debtor", cmd_pay_debtor))
    app.add_handler(CommandHandler("debtors", cmd_debtors))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("sales_report", cmd_sales_report))
    app.add_handler(CommandHandler("expense_report", cmd_expense_report))
    app.add_handler(CommandHandler("staff_report", cmd_staff_report))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("allocation", cmd_allocation))
    app.add_handler(CommandHandler("setallocation", cmd_setallocation))
    app.add_handler(CommandHandler("stock", cmd_stock))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("transfer", cmd_transfer))
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
