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
