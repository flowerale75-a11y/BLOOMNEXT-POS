
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
import sqlite3
import os
from datetime import datetime

# -------------------------
# Paths / DB
# -------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "pos.db")


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        barcode TEXT UNIQUE,
        price_cents INTEGER NOT NULL DEFAULT 0,
        taxable INTEGER NOT NULL DEFAULT 1,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        subtotal_cents INTEGER NOT NULL,
        tax_cents INTEGER NOT NULL,
        total_cents INTEGER NOT NULL,
        payment_method TEXT NOT NULL,
        notes TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER,
        name_snapshot TEXT NOT NULL,
        barcode_snapshot TEXT,
        unit_price_cents INTEGER NOT NULL,
        qty INTEGER NOT NULL,
        taxable_snapshot INTEGER NOT NULL,
        line_total_cents INTEGER NOT NULL,
        FOREIGN KEY(order_id) REFERENCES orders(id)
    );
    """)

    conn.commit()
    conn.close()


# -------------------------
# App
# -------------------------
app = FastAPI(title="BloomNext POS API", version="1.0.0")


@app.on_event("startup")
def startup():
    init_db()


# -------------------------
# Models
# -------------------------
class ProductIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    barcode: Optional[str] = Field(default=None, max_length=100)
    price: float = Field(..., ge=0)  # dollars
    taxable: bool = True
    active: bool = True


class ProductOut(BaseModel):
    id: int
    name: str
    barcode: Optional[str]
    price: float
    taxable: bool
    active: bool


class CartItemIn(BaseModel):
    product_id: int
    qty: int = Field(..., ge=1, le=999)


class CheckoutIn(BaseModel):
    items: List[CartItemIn]
    payment_method: Literal["cash", "card", "other"]
    tax_enabled: bool = False
    tax_rate: float = Field(default=0.0, ge=0.0, le=0.25)  # 0.08875 example
    notes: Optional[str] = None


class OrderItemOut(BaseModel):
    name: str
    barcode: Optional[str]
    unit_price: float
    qty: int
    taxable: bool
    line_total: float


class OrderOut(BaseModel):
    id: int
    created_at: str
    subtotal: float
    tax: float
    total: float
    payment_method: str
    notes: Optional[str]
    items: List[OrderItemOut]


# -------------------------
# Helpers
# -------------------------
def dollars_to_cents(x: float) -> int:
    return int(round(x * 100))


def cents_to_dollars(c: int) -> float:
    return round(c / 100.0, 2)


def row_product_to_out(r: sqlite3.Row) -> ProductOut:
    return ProductOut(
        id=r["id"],
        name=r["name"],
        barcode=r["barcode"],
        price=cents_to_dollars(r["price_cents"]),
        taxable=bool(r["taxable"]),
        active=bool(r["active"]),
    )


# -------------------------
# Root / Health
# -------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "BloomNext POS API",
        "message": "API is running",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
def health():
    return {"ok": True}


# -------------------------
# Products API
# -------------------------
@app.get("/api/products", response_model=List[ProductOut])
def list_products(active_only: bool = False):
    conn = db_conn()
    cur = conn.cursor()
    if active_only:
        cur.execute("SELECT * FROM products WHERE active=1 ORDER BY id DESC;")
    else:
        cur.execute("SELECT * FROM products ORDER BY id DESC;")
    rows = cur.fetchall()
    conn.close()
    return [row_product_to_out(r) for r in rows]


@app.get("/api/products/lookup", response_model=Optional[ProductOut])
def lookup_product(barcode: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE barcode=? AND active=1 LIMIT 1;", (barcode,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return row_product_to_out(r)


@app.post("/api/products", response_model=ProductOut)
def create_product(p: ProductIn):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO products (name, barcode, price_cents, taxable, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                p.name.strip(),
                (p.barcode.strip() if p.barcode else None),
                dollars_to_cents(p.price),
                1 if p.taxable else 0,
                1 if p.active else 0,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        new_id = cur.lastrowid
        cur.execute("SELECT * FROM products WHERE id=?;", (new_id,))
        r = cur.fetchone()
        return row_product_to_out(r)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Barcode already exists.")
    finally:
        conn.close()


@app.put("/api/products/{product_id}", response_model=ProductOut)
def update_product(product_id: int, p: ProductIn):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
        existing = cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Product not found")

        cur.execute(
            """
            UPDATE products
            SET name=?, barcode=?, price_cents=?, taxable=?, active=?
            WHERE id=?;
            """,
            (
                p.name.strip(),
                (p.barcode.strip() if p.barcode else None),
                dollars_to_cents(p.price),
                1 if p.taxable else 0,
                1 if p.active else 0,
                product_id,
            ),
        )
        conn.commit()

        cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
        r = cur.fetchone()
        return row_product_to_out(r)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Barcode already exists.")
    finally:
        conn.close()


@app.delete("/api/products/{product_id}")
def delete_product(product_id: int):
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Product not found")

    # Soft delete
    cur.execute("UPDATE products SET active=0 WHERE id=?;", (product_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# -------------------------
# Orders API
# -------------------------
@app.post("/api/orders", response_model=OrderOut)
def checkout(payload: CheckoutIn):
    if not payload.items:
        raise HTTPException(status_code=400, detail="Cart is empty")

    conn = db_conn()
    cur = conn.cursor()

    # Load products
    product_map = {}
    for it in payload.items:
        cur.execute("SELECT * FROM products WHERE id=? AND active=1;", (it.product_id,))
        r = cur.fetchone()
        if not r:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Product not found or inactive: {it.product_id}")
        product_map[it.product_id] = r

    subtotal_cents = 0
    taxable_base_cents = 0

    # Build item outputs (snapshots will be saved)
    items_out: List[OrderItemOut] = []

    for it in payload.items:
        pr = product_map[it.product_id]
        unit = int(pr["price_cents"])
        qty = int(it.qty)
        line = unit * qty

        subtotal_cents += line
        is_taxable = bool(pr["taxable"])

        if payload.tax_enabled and is_taxable:
            taxable_base_cents += line

        items_out.append(
            OrderItemOut(
                name=pr["name"],
                barcode=pr["barcode"],
                unit_price=cents_to_dollars(unit),
                qty=qty,
                taxable=is_taxable,
                line_total=cents_to_dollars(line),
            )
        )

    tax_cents = 0
    if payload.tax_enabled and payload.tax_rate > 0:
        tax_cents = int(round(taxable_base_cents * payload.tax_rate))

    total_cents = subtotal_cents + tax_cents
    created_at = datetime.utcnow().isoformat()

    # Create order
    cur.execute(
        """
        INSERT INTO orders (created_at, subtotal_cents, tax_cents, total_cents, payment_method, notes)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (created_at, subtotal_cents, tax_cents, total_cents, payload.payment_method, payload.notes),
    )
    order_id = cur.lastrowid

    # Create order item snapshots
    for it in payload.items:
        pr = product_map[it.product_id]
        unit = int(pr["price_cents"])
        qty = int(it.qty)
        line = unit * qty
        cur.execute(
            """
            INSERT INTO order_items
            (order_id, product_id, name_snapshot, barcode_snapshot, unit_price_cents, qty, taxable_snapshot, line_total_cents)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                order_id,
                it.product_id,
                pr["name"],
                pr["barcode"],
                unit,
                qty,
                1 if bool(pr["taxable"]) else 0,
                line,
            ),
        )

    conn.commit()

    # Read back
    cur.execute("SELECT * FROM orders WHERE id=?;", (order_id,))
    o = cur.fetchone()
    cur.execute("SELECT * FROM order_items WHERE order_id=? ORDER BY id ASC;", (order_id,))
    oi = cur.fetchall()
    conn.close()

    return OrderOut(
        id=o["id"],
        created_at=o["created_at"],
        subtotal=cents_to_dollars(o["subtotal_cents"]),
        tax=cents_to_dollars(o["tax_cents"]),
        total=cents_to_dollars(o["total_cents"]),
        payment_method=o["payment_method"],
        notes=o["notes"],
        items=[
            OrderItemOut(
                name=r["name_snapshot"],
                barcode=r["barcode_snapshot"],
                unit_price=cents_to_dollars(r["unit_price_cents"]),
                qty=r["qty"],
                taxable=bool(r["taxable_snapshot"]),
                line_total=cents_to_dollars(r["line_total_cents"]),
            )
            for r in oi
        ],
    )


@app.get("/api/orders/recent", response_model=List[OrderOut])
def recent_orders(limit: int = 20):
    limit = max(1, min(limit, 100))
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?;", (limit,))
    orders = cur.fetchall()

    out: List[OrderOut] = []
    for o in orders:
        cur.execute("SELECT * FROM order_items WHERE order_id=? ORDER BY id ASC;", (o["id"],))
        oi = cur.fetchall()
        out.append(
            OrderOut(
                id=o["id"],
                created_at=o["created_at"],
                subtotal=cents_to_dollars(o["subtotal_cents"]),
                tax=cents_to_dollars(o["tax_cents"]),
                total=cents_to_dollars(o["total_cents"]),
                payment_method=o["payment_method"],
                notes=o["notes"],
                items=[
                    OrderItemOut(
                        name=r["name_snapshot"],
                        barcode=r["barcode_snapshot"],
                        unit_price=cents_to_dollars(r["unit_price_cents"]),
                        qty=r["qty"],
                        taxable=bool(r["taxable_snapshot"]),
                        line_total=cents_to_dollars(r["line_total_cents"]),
                    )
                    for r in oi
                ],
            )
        )

    conn.close()
    return out
