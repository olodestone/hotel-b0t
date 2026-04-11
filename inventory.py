"""
inventory.py — Drink stock business logic.

Enforces:
  - No negative stock (sale rejected if insufficient)
  - Low-stock alerts after each operation
  - Cost price tracking
"""
from __future__ import annotations

from dataclasses import dataclass

import database as db


@dataclass
class StockResult:
    ok: bool
    message: str
    low_stock_alert: str | None = None
    current_stock: int = 0


def sell_drink(drink: str, qty: int, price: float, timestamp: str | None = None, recorded_by: str = "") -> StockResult:
    """
    Deduct `qty` from inventory and record the sale.
    Returns StockResult indicating success/failure and any alert.
    """
    existing = db.get_drink(drink)
    if existing is None:
        return StockResult(ok=False, message=f"❌ '{drink}' not found in inventory. Restock it first with /restock.")

    cur_stock = int(existing["current_stock"])
    if cur_stock < qty:
        return StockResult(
            ok=False,
            message=f"❌ Insufficient stock for *{drink}*.\n"
                    f"Available: {cur_stock} | Requested: {qty}",
            current_stock=cur_stock,
        )

    updated = db.upsert_drink(drink, qty_sold=qty)
    db.record_sale(drink, qty, price, timestamp=timestamp, recorded_by=recorded_by)

    new_stock = int(updated["current_stock"])
    threshold = int(updated["low_stock_threshold"])
    alert = None
    if new_stock <= threshold:
        alert = (
            f"⚠️ LOW STOCK ALERT: *{drink.title()}* is down to {new_stock} units "
            f"(threshold: {threshold}). Consider restocking!"
        )

    return StockResult(
        ok=True,
        message=f"✅ Sold {qty}× *{drink.title()}* @ ₦{price:,.2f} each.\nRemaining stock: {new_stock}",
        low_stock_alert=alert,
        current_stock=new_stock,
    )


def restock_drink(drink: str, qty: int, cost_price: float, threshold: int | None = None) -> StockResult:
    """Add `qty` to the store (not bar). Cost price is recorded."""
    updated = db.upsert_drink(drink, qty_to_store=qty, cost_price=cost_price, threshold=threshold)
    store = int(updated["store_stock"])
    bar = int(updated["current_stock"])
    return StockResult(
        ok=True,
        message=(
            f"✅ Restocked *{drink.title()}*: +{qty} units added to store @ ₦{cost_price:,.2f} each.\n"
            f"Store: {store} | Bar/Freezer: {bar}"
        ),
        current_stock=bar,
    )


def transfer_to_bar(drink: str, qty: int) -> StockResult:
    """Move qty units from store to bar/freezer."""
    try:
        updated = db.transfer_drink(drink, qty)
    except ValueError as e:
        return StockResult(ok=False, message=f"❌ {e}")

    store = int(updated["store_stock"])
    bar = int(updated["current_stock"])
    threshold = int(updated["low_stock_threshold"])
    alert = None
    if store == 0:
        alert = f"⚠️ Store is now *empty* for *{drink.title()}*. Consider restocking!"
    return StockResult(
        ok=True,
        message=(
            f"✅ Transferred {qty}× *{drink.title()}* from store to bar.\n"
            f"Store: {store} | Bar/Freezer: {bar}"
        ),
        low_stock_alert=alert,
        current_stock=bar,
    )


def restore_bar_stock(drink: str, qty: int) -> None:
    """Add qty back to bar stock after a sale is deleted."""
    db.upsert_drink(drink, qty_to_bar=qty)


def set_threshold(drink: str, threshold: int) -> StockResult:
    """Update low-stock alert threshold for a drink."""
    existing = db.get_drink(drink)
    if existing is None:
        return StockResult(ok=False, message=f"❌ '{drink}' not found in inventory.")
    db.upsert_drink(drink, threshold=threshold)
    return StockResult(
        ok=True,
        message=f"✅ Low-stock threshold for *{drink.title()}* set to {threshold} units.",
    )


def get_inventory_summary() -> list[dict]:
    """Return all inventory rows enriched with stock_value and is_low flag."""
    rows = db.read_all("inventory")
    result = []
    for row in rows:
        bar = int(row["current_stock"])
        store = int(row.get("store_stock", 0))
        cost = float(row["cost_price"])
        threshold = int(row["low_stock_threshold"])
        total = bar + store
        result.append({
            "drink": row["drink_name"].title(),
            "bar_stock": bar,
            "store_stock": store,
            "cost_price": cost,
            "stock_value": round(total * cost, 2),
            "is_low": bar <= threshold,
            "threshold": threshold,
            "total_sold": int(row["total_sold"]),
        })
    result.sort(key=lambda x: x["drink"])
    return result
