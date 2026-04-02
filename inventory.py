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


def sell_drink(drink: str, qty: int, price: float) -> StockResult:
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
    db.record_sale(drink, qty, price)

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
    """Add `qty` to inventory with given cost price."""
    updated = db.upsert_drink(drink, qty_purchased=qty, cost_price=cost_price, threshold=threshold)
    new_stock = int(updated["current_stock"])
    return StockResult(
        ok=True,
        message=(
            f"✅ Restocked *{drink.title()}*: +{qty} units @ ₦{cost_price:,.2f} cost each.\n"
            f"Current stock: {new_stock}"
        ),
        current_stock=new_stock,
    )


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
        stock = int(row["current_stock"])
        cost = float(row["cost_price"])
        threshold = int(row["low_stock_threshold"])
        result.append({
            "drink": row["drink_name"].title(),
            "closing_stock": stock,
            "cost_price": cost,
            "stock_value": round(stock * cost, 2),
            "is_low": stock <= threshold,
            "threshold": threshold,
            "total_sold": int(row["total_sold"]),
        })
    result.sort(key=lambda x: x["drink"])
    return result
