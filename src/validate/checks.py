from sqlalchemy import text
from src.config import SETTINGS
from src.load.postgres import get_engine


def validate_curated() -> dict:
    """
    Run data/contract assertions against curated.sales_order_lines.
    Raises ValueError if any contract is violated. Returns a summary
    dict of counts when all checks pass.
    """
    allowed_statuses = SETTINGS['quality']['allowed_order_statuses']
    min_qty = SETTINGS['quality']['min_quantity']
    max_qty = SETTINGS['quality']['max_quantity']

    engine = get_engine()
    issues = []

    with engine.connect() as conn:
        # 1. Duplicate business keys (order_id must be unique)
        dup_rows = conn.execute(text("""
            SELECT order_id, COUNT(*) AS cnt
            FROM curated.sales_order_lines
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)).fetchall()
        if dup_rows:
            issues.append(f"{len(dup_rows)} duplicate order_id value(s) found: "
                           f"{[r[0] for r in dup_rows[:5]]}{' ...' if len(dup_rows) > 5 else ''}")

        # 2. Null business keys
        null_keys = conn.execute(text("""
            SELECT COUNT(*) FROM curated.sales_order_lines
            WHERE order_id IS NULL OR customer_id IS NULL OR product_id IS NULL
        """)).scalar()
        if null_keys:
            issues.append(f"{null_keys} row(s) with a null business key "
                           f"(order_id/customer_id/product_id)")

        # 3. Invalid status values
        status_placeholders = ", ".join(f"'{s}'" for s in allowed_statuses)
        bad_status = conn.execute(text(f"""
            SELECT COUNT(*) FROM curated.sales_order_lines
            WHERE status NOT IN ({status_placeholders})
        """)).scalar()
        if bad_status:
            issues.append(f"{bad_status} row(s) with a status outside {allowed_statuses}")

        # 4. Invalid quantity range
        bad_qty = conn.execute(text("""
            SELECT COUNT(*) FROM curated.sales_order_lines
            WHERE quantity IS NULL OR quantity < :min_qty OR quantity > :max_qty
        """), {"min_qty": min_qty, "max_qty": max_qty}).scalar()
        if bad_qty:
            issues.append(f"{bad_qty} row(s) with quantity outside [{min_qty}, {max_qty}]")

        # 5. Invalid/negative amounts, and net_amount must equal gross - discount
        bad_amounts = conn.execute(text("""
            SELECT COUNT(*) FROM curated.sales_order_lines
            WHERE gross_amount < 0
               OR discount_amount < 0
               OR net_amount < 0
               OR ABS(net_amount - (gross_amount - discount_amount)) > 0.01
        """)).scalar()
        if bad_amounts:
            issues.append(f"{bad_amounts} row(s) with negative or inconsistent "
                           f"gross/discount/net amounts")

    if issues:
        raise ValueError("Curated data contract violated:\n  - " + "\n  - ".join(issues))

    with engine.connect() as conn:
        total_rows = conn.execute(text(
            "SELECT COUNT(*) FROM curated.sales_order_lines"
        )).scalar()

    return {"total_rows": total_rows, "status": "passed"}