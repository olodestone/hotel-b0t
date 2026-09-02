"""
Tests for the guided-flow navigation stack (⬅️ Back / ✖️ Cancel).

The stack is what makes a wrong turn cost one tap instead of the whole entry, so
these tests pin the three things that are easy to break from a distance:

1. **The snapshot.** ⬅️ Back restores what the flow knew when that prompt was
   first shown, not just the prompt itself. Without it, backing out of the
   hourly branch of a booking would keep the time-of-day answer and stamp it on
   a nightly stay.
2. **The repeat rule.** A re-prompt after a rejected answer is the *same* step,
   so it overwrites the top of the stack. Without it, three fumbled attempts at
   an amount would need three taps of Back to escape.
3. **Universal coverage.** Every state of every registered flow carries the
   escape row, checked against the real registration rather than a stand-in — a
   screen that quietly lacked a way out would be exactly the screen the front
   desk gets stuck on.

There is no pytest-asyncio in this project and no reason to add one: the
handlers are plain coroutines, so `_run()` drives them directly. Telegram is
faked at the surface the handlers actually touch — answer / edit / reply — which
is enough to read back what landed on screen.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ConversationHandler

import bot
import database


# ── Driving a coroutine handler ───────────────────────────────────────

def _run(coro):
    return asyncio.run(coro)


# ── Fake Telegram surface ─────────────────────────────────────────────

class _Message:
    """What a flow replies into when it is sending rather than editing."""

    def __init__(self, text: str = ""):
        self.text = text
        self.sent: list[tuple[str, dict]] = []

    async def reply_text(self, text, **kw):
        self.sent.append((text, kw))


class _Query:
    def __init__(self, data: str = "", user_id: int = 7):
        self.data = data
        self.from_user = SimpleNamespace(id=user_id, username="desk", first_name="Desk")
        self.edits: list[tuple[str, dict]] = []
        self.answers: list[tuple] = []
        self.markups: list = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kw):
        self.edits.append((text, kw))

    async def edit_message_reply_markup(self, reply_markup=None):
        self.markups.append(reply_markup)


class _Update:
    def __init__(self, *, query=None, message=None, user_id: int = 7):
        self.callback_query = query
        self.message = message
        self.effective_message = message if message is not None else _Message()
        self.effective_user = SimpleNamespace(id=user_id, username="desk", first_name="Desk")


def _tap(data: str = "", user_id: int = 7) -> _Update:
    """A button press."""
    return _Update(query=_Query(data, user_id), user_id=user_id)


def _typed(text: str = "", user_id: int = 7) -> _Update:
    """A typed message."""
    return _Update(message=_Message(text), user_id=user_id)


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(user_data={})


# ── Reading back what landed on screen ────────────────────────────────

def _last(update: _Update) -> tuple[str, dict]:
    q = update.callback_query
    if q is not None and q.edits:
        return q.edits[-1]
    return update.effective_message.sent[-1]


def _shown(update: _Update) -> str:
    return _last(update)[0]


def _keyboard(update: _Update) -> InlineKeyboardMarkup:
    return _last(update)[1]["reply_markup"]


def _nav_row(update: _Update) -> list[str]:
    """The escape row, by callback data — labels carry emoji, this doesn't."""
    return [b.callback_data for b in _keyboard(update).inline_keyboard[-1]]


def _depth(ctx) -> int:
    return len(ctx.user_data.get(bot._NAV) or [])


# ── The escape row ────────────────────────────────────────────────────

def test_the_opening_prompt_offers_cancel_but_nothing_to_go_back_to():
    ctx, upd = _ctx(), _typed("/sell")
    _run(bot._step(upd, ctx, "Which drink?", state=1, root=True, reply=True))
    assert _nav_row(upd) == ["nav:cancel"]        # nothing behind the first prompt


def test_the_second_prompt_offers_back():
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    upd = _tap("sd:heineken")
    _run(bot._step(upd, ctx, "How many?", state=2))
    assert _nav_row(upd) == ["nav:back", "nav:cancel"]


def test_the_flows_own_keyboard_survives_the_nav_row():
    ctx, upd = _ctx(), _typed("/sell")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("6", callback_data="sq:6")],
                               [InlineKeyboardButton("✏️ Other", callback_data="sq:__other__")]])
    _run(bot._step(upd, ctx, "How many?", state=1, root=True, reply=True, reply_markup=kb))
    rows = [[b.callback_data for b in r] for r in _keyboard(upd).inline_keyboard]
    assert rows == [["sq:6"], ["sq:__other__"], ["nav:cancel"]]   # appended, never replacing


def test_a_flow_with_no_keyboard_of_its_own_still_gets_an_escape():
    ctx, upd = _ctx(), _tap("sd:__other__")
    _run(bot._step(upd, ctx, "Type the drink name:", state=1, root=True))
    assert _nav_row(upd) == ["nav:cancel"]        # a typed step is not a dead end


# ── Going back ────────────────────────────────────────────────────────

def test_back_puts_the_previous_prompt_back_and_returns_its_state():
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=11, root=True, reply=True))
    _run(bot._step(_tap("sd:heineken"), ctx, "How many?", state=22))

    upd = _tap("nav:back")
    assert _run(bot._nav_back(upd, ctx)) == 11
    assert _shown(upd) == "Which drink?"
    assert _nav_row(upd) == ["nav:cancel"]        # back at the root, Back goes away again


def test_back_drops_what_was_answered_after_the_step_it_returns_to():
    """The snapshot, not just the prompt — a branch has to be safe to walk out of."""
    ctx = _ctx()
    _run(bot._step(_typed("/book"), ctx, "Room type?", state=11, root=True, reply=True))
    ctx.user_data["book_type"] = "short time"
    _run(bot._step(_tap("bt:short time"), ctx, "How many rooms?", state=22))
    ctx.user_data["book_qty"] = 1
    ctx.user_data["book_daypart"] = "Evening"

    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == 11
    assert "book_type" not in ctx.user_data
    assert "book_qty" not in ctx.user_data
    assert "book_daypart" not in ctx.user_data   # the hourly answer cannot survive the branch


def test_back_keeps_what_was_already_answered_before_that_step():
    ctx = _ctx()
    _run(bot._step(_typed("/book"), ctx, "Room type?", state=11, root=True, reply=True))
    ctx.user_data["book_type"] = "standard"
    _run(bot._step(_tap("bt:standard"), ctx, "How many rooms?", state=22))
    ctx.user_data["book_qty"] = 2
    _run(bot._step(_tap("bq:2"), ctx, "How many nights?", state=33))

    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == 22
    assert ctx.user_data["book_type"] == "standard"   # the answer this prompt was shown with
    assert "book_qty" not in ctx.user_data            # the one it is being asked again


def test_back_walks_a_long_flow_all_the_way_to_its_first_prompt():
    ctx = _ctx()
    _run(bot._step(_typed("/exp"), ctx, "Account?", state=1, root=True, reply=True))
    for n, prompt in ((2, "Category?"), (3, "Amount?"), (4, "Class?"), (5, "Note?")):
        _run(bot._step(_tap(f"x:{n}"), ctx, prompt, state=n))
    assert _depth(ctx) == 5

    for expected in (4, 3, 2, 1):
        assert _run(bot._nav_back(_tap("nav:back"), ctx)) == expected
    assert _depth(ctx) == 1


# ── A rejected answer is the same step, not a new one ─────────────────

def test_three_fumbled_attempts_cost_one_tap_of_back_not_three():
    ctx = _ctx()
    _run(bot._step(_typed("/exp"), ctx, "Account?", state=1, root=True, reply=True))
    _run(bot._step(_tap("ea:bar"), ctx, "Amount?", state=2))
    for _ in range(3):
        _run(bot._step(_typed("lots"), ctx, "❌ Enter a number:", state=2, reply=True))

    assert _depth(ctx) == 2                       # the re-prompts overwrote, never pushed
    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == 1


def test_a_rejected_answer_keeps_the_back_button_it_had():
    ctx = _ctx()
    _run(bot._step(_typed("/exp"), ctx, "Account?", state=1, root=True, reply=True))
    _run(bot._step(_tap("ea:bar"), ctx, "Amount?", state=2))
    upd = _typed("lots")
    _run(bot._step(upd, ctx, "❌ Enter a number:", state=2, reply=True))
    assert _nav_row(upd) == ["nav:back", "nav:cancel"]


def test_a_rejected_answer_on_the_first_prompt_grows_no_back_button():
    """Back on the root step would point at nothing — it must not appear."""
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Type the drink name:", state=1, root=True, reply=True))
    upd = _typed("???")
    _run(bot._step(upd, ctx, "❌ Drink not recognised:", state=1, reply=True))
    assert _nav_row(upd) == ["nav:cancel"]
    assert _depth(ctx) == 1


# ── The ends of the stack ─────────────────────────────────────────────

def test_back_from_the_opening_prompt_cancels_rather_than_sticking():
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    ctx.user_data["sell_drink"] = "heineken"

    upd = _tap("nav:back")
    assert _run(bot._nav_back(upd, ctx)) == ConversationHandler.END
    assert ctx.user_data == {}
    assert "Cancelled" in _shown(upd)


def test_back_with_no_stack_at_all_cancels_instead_of_raising():
    """A tap that outlived its flow's data must not take the bot down."""
    upd = _tap("nav:back")
    assert _run(bot._nav_back(upd, _ctx())) == ConversationHandler.END


def test_cancel_clears_the_flow_and_ends_it():
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    ctx.user_data["sell_drink"] = "heineken"

    upd = _tap("nav:cancel")
    assert _run(bot._nav_cancel(upd, ctx)) == ConversationHandler.END
    assert ctx.user_data == {}
    assert "Cancelled" in _shown(upd)


def test_a_step_taken_after_a_write_cannot_be_walked_back_past():
    """`root=True` seals the stack: there is nothing safe to go back to past a
    database write, so the prompt after one starts fresh."""
    ctx = _ctx()
    _run(bot._step(_typed("/count"), ctx, "Which drink?", state=1, root=True, reply=True))
    _run(bot._step(_tap("cd:heineken"), ctx, "How many units?", state=2))

    upd = _tap("ok")
    _run(bot._step(upd, ctx, "✅ Counted. Another item?", state=3, root=True))
    assert _nav_row(upd) == ["nav:cancel"]
    assert _depth(ctx) == 1
    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == ConversationHandler.END


def test_say_ends_a_flow_without_recording_a_step():
    ctx = _ctx()
    _run(bot._step(_typed("/count"), ctx, "Which drink?", state=1, root=True, reply=True))
    upd = _tap("done")
    _run(bot._say(upd, "✅ Stocktake entered — 4 items counted."))
    assert _depth(ctx) == 1                       # the closing line is not a prompt
    assert "Stocktake entered" in _shown(upd)


def test_a_back_tap_on_a_finished_flow_says_so_and_strips_the_buttons():
    upd = _tap("nav:back")
    _run(bot._nav_stale(upd, _ctx()))
    text, alert = upd.callback_query.answers[-1]
    assert alert is True                          # a toast would be missed
    assert "already finished" in text
    assert upd.callback_query.markups == [None]   # the dead buttons come off


# ── Both ways of showing a prompt record a step ───────────────────────

def test_a_typed_prompt_is_recorded_like_a_tapped_one():
    ctx, upd = _ctx(), _typed("heineken")
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    _run(bot._step(upd, ctx, "How many?", state=2, reply=True))
    assert _depth(ctx) == 2
    assert upd.effective_message.sent[-1][0] == "How many?"   # replied, not edited
    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == 1


def test_re_entering_a_flow_starts_a_fresh_stack():
    ctx = _ctx()
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    _run(bot._step(_tap("sd:heineken"), ctx, "How many?", state=2))
    _run(bot._step(_typed("/sell"), ctx, "Which drink?", state=1, root=True, reply=True))
    assert _depth(ctx) == 1                       # the abandoned flow leaves nothing behind


# ── Every flow, checked against the real registration ─────────────────

class _FakeApp:
    """Collects what `_register_handlers` registers, in order."""

    def __init__(self):
        self.handlers: list = []

    def add_handler(self, handler, group: int = 0):
        self.handlers.append((group, handler))

    def add_error_handler(self, fn):
        pass


@pytest.fixture
def registered():
    app = _FakeApp()
    bot._register_handlers(app, "public", [])
    return app.handlers


def _label(conv: ConversationHandler) -> str:
    entries = [getattr(h, "callback", None) for h in conv.entry_points]
    return next((getattr(e, "__name__", "?") for e in entries if e), "?")


def test_every_state_of_every_guided_flow_has_an_escape(registered):
    """Checked here rather than per flow: a screen that quietly lacked a way out
    would be exactly the screen someone gets stuck on."""
    convs = [h for _, h in registered if isinstance(h, ConversationHandler)]
    assert len(convs) > 20, "conversation flows are no longer being registered"

    missing = []
    for conv in convs:
        for state, handlers in conv.states.items():
            callbacks = {getattr(h, "callback", None) for h in handlers}
            if bot._nav_back not in callbacks or bot._nav_cancel not in callbacks:
                missing.append(f"{_label(conv)} state {state}")
    assert not missing, "states with no ⬅️ Back / ✖️ Cancel: " + ", ".join(missing)


def test_a_nav_tap_is_still_answered_once_the_flow_is_gone(registered):
    callbacks = [getattr(h, "callback", None) for _, h in registered
                 if isinstance(h, CallbackQueryHandler)]
    assert bot._nav_stale in callbacks


def test_the_stale_handler_sits_behind_the_flows_that_own_the_taps(registered):
    """PTB runs at most one handler per group, so a global nav handler placed
    ahead of the conversations would swallow every live Back tap."""
    groups = {g for g, _ in registered}
    assert groups <= {-1, 0}, "a new handler group would need this ordering re-checked"

    order = [h for g, h in registered if g == 0]
    last_conv = max(i for i, h in enumerate(order) if isinstance(h, ConversationHandler))
    stale = next(i for i, h in enumerate(order)
                 if isinstance(h, CallbackQueryHandler) and h.callback is bot._nav_stale)
    assert stale > last_conv


# ── The real booking flow, end to end ─────────────────────────────────

@pytest.fixture
def hourly_hotel(monkeypatch):
    """A hotel selling one type by the hour and one by the night."""
    monkeypatch.setattr(bot, "_is_admin", lambda uid: True)
    monkeypatch.setattr(bot, "_is_authorized", lambda uid: True)
    monkeypatch.setattr(database, "get_all_room_type_prices",
                        lambda: [{"room_type": "short time", "price": 3000},
                                 {"room_type": "standard", "price": 15000}])
    monkeypatch.setattr(database, "get_all_room_type_hours", lambda: {"short time": 2.0})


def test_backing_out_of_the_hourly_branch_drops_the_time_of_day(hourly_hotel):
    """The case the snapshot exists for, driven through the real handlers: an
    hourly let is asked what part of the day it was, and that answer must not
    still be sitting there if the booking is walked back and re-typed as a
    nightly stay."""
    ctx = _ctx()
    _run(bot.cmd_book_start(_typed("/book"), ctx))
    assert _run(bot._book_pick_type(_tap("bt:short time"), ctx)) == bot._BOOK_QTY
    assert _run(bot._book_pick_qty(_tap("bq:1"), ctx)) == bot._BOOK_NIGHTS
    assert _run(bot._book_pick_nights(_tap("bn:1"), ctx)) == bot._BOOK_DAYPART
    assert _run(bot._book_pick_daypart(_tap("bdp:Evening"), ctx)) == bot._BOOK_DATE
    assert ctx.user_data["book_daypart"] == "Evening"

    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == bot._BOOK_DAYPART
    assert "book_daypart" not in ctx.user_data
    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == bot._BOOK_NIGHTS
    assert "book_nights" not in ctx.user_data
    assert _run(bot._nav_back(_tap("nav:back"), ctx)) == bot._BOOK_QTY
    assert "book_qty" not in ctx.user_data

    upd = _tap("nav:back")
    assert _run(bot._nav_back(upd, ctx)) == bot._BOOK_TYPE
    assert "book_type" not in ctx.user_data       # nothing hourly left to inherit
    assert _nav_row(upd) == ["nav:cancel"]


def test_a_nightly_booking_is_never_asked_the_time_of_day(hourly_hotel):
    ctx = _ctx()
    _run(bot.cmd_book_start(_typed("/book"), ctx))
    _run(bot._book_pick_type(_tap("bt:standard"), ctx))
    _run(bot._book_pick_qty(_tap("bq:1"), ctx))
    assert _run(bot._book_pick_nights(_tap("bn:2"), ctx)) == bot._BOOK_DATE
