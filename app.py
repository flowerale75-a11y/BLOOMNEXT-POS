from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
import sqlite3
import os
from datetime import datetime

# ----------------------------
# Paths / DB
# ----------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "pos.db")

def utc_now() -> str:
    return datetime.utcnow().isoformat()

def db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_columns(cur: sqlite3.Cursor, table: str, columns: List[tuple]):
    """
    columns: list of (col_name, sql_type_and_default)
    Example: ("stock_qty", "INTEGER NOT NULL DEFAULT 0")
    """
    cur.execute(f"PRAGMA table_info({table});")
    existing = {row["name"] for row in cur.fetchall()}
    for name, decl in columns:
        if name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl};")

def init_db():
    conn = db_conn()
    cur = conn.cursor()

    # PRODUCTS table (core)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        barcode TEXT UNIQUE,
        sku TEXT,
        category TEXT,
        unit TEXT NOT NULL DEFAULT 'each',
        price_cents INTEGER NOT NULL DEFAULT 0,
        cost_cents INTEGER NOT NULL DEFAULT 0,
        taxable INTEGER NOT NULL DEFAULT 1,
        stock_qty INTEGER NOT NULL DEFAULT 0,
        reorder_level INTEGER NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """)

    # If you already had an older products table, ensure the new columns exist
    ensure_columns(cur, "products", [
        ("sku", "TEXT"),
        ("category", "TEXT"),
        ("unit", "TEXT NOT NULL DEFAULT 'each'"),
        ("cost_cents", "INTEGER NOT NULL DEFAULT 0"),
        ("stock_qty", "INTEGER NOT NULL DEFAULT 0"),
        ("reorder_level", "INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    ])

    # INVENTORY MOVEMENTS (audit trail)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        type TEXT NOT NULL,            -- 'receive' | 'adjust' | 'set'
        delta_qty INTEGER NOT NULL,    -- change applied
        resulting_qty INTEGER NOT NULL,
        note TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(product_id) REFERENCES products(id)
    );
    """)

    conn.commit()
    conn.close()

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="BloomNext POS - Product & Inventory API", version="1.0.0")

@app.on_event("startup")
def startup():
    init_db()

# ----------------------------
# Models
# ----------------------------
class ProductIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    barcode: Optional[str] = Field(default=None, max_length=100)
    sku: Optional[str] = Field(default=None, max_length=100)
    category: Optional[str] = Field(default=None, max_length=100)

    # simple units for now
    unit: Literal["each", "bunch", "box", "stem", "kg"] = "each"

    price: float = Field(..., ge=0)  # selling price in dollars
    cost: float = Field(default=0.0, ge=0)  # cost in dollars

    taxable: bool = True
    stock_qty: int = Field(default=0, ge=0, le=1_000_000)
    reorder_level: int = Field(default=0, ge=0, le=1_000_000)

    active: bool = True

class ProductOut(BaseModel):
    id: int
    name: str
    barcode: Optional[str]
    sku: Optional[str]
    category: Optional[str]
    unit: str
    price: float
    cost: float
    taxable: bool
    stock_qty: int
    reorder_level: int
    active: bool
    created_at: str
    updated_at: str

class StockAdjustIn(BaseModel):
    delta_qty: int = Field(..., ge=-1_000_000, le=1_000_000)
    note: Optional[str] = Field(default=None, max_length=300)

class StockSetIn(BaseModel):
    new_qty: int = Field(..., ge=0, le=1_000_000)
    note: Optional[str] = Field(default=None, max_length=300)

class InventoryMoveOut(BaseModel):
    id: int
    product_id: int
    type: str
    delta_qty: int
    resulting_qty: int
    note: Optional[str]
    created_at: str

# ----------------------------
# Helpers
# ----------------------------
def dollars_to_cents(x: float) -> int:
    return int(round(x * 100))

def cents_to_dollars(c: int) -> float:
    return round(c / 100.0, 2)

def row_product_to_out(r: sqlite3.Row) -> ProductOut:
    return ProductOut(
        id=r["id"],
        name=r["name"],
        barcode=r["barcode"],
        sku=r["sku"],
        category=r["category"],
        unit=r["unit"],
        price=cents_to_dollars(int(r["price_cents"])),
        cost=cents_to_dollars(int(r["cost_cents"])),
        taxable=bool(r["taxable"]),
        stock_qty=int(r["stock_qty"]),
        reorder_level=int(r["reorder_level"]),
        active=bool(r["active"]),
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )

def get_product_row(cur: sqlite3.Cursor, product_id: int) -> sqlite3.Row:
    cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
    r = cur.fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Product not found")
    return r

# ----------------------------
# Root / Health
# ----------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "BloomNext POS API",
        "message": "Product & Inventory API is running",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
def health():
    return {"ok": True}

# ----------------------------
# Products API
# ----------------------------
@app.get("/api/products", response_model=List[ProductOut])
def list_products(
    active_only: bool = False,
    low_stock_only: bool = False,
    q: Optional[str] = Query(default=None, description="Search name/sku/barcode"),
    category: Optional[str] = None,
    limit: int = 200
):
    limit = max(1, min(limit, 1000))
    conn = db_conn()
    cur = conn.cursor()

    where = []
    params = []

    if active_only:
        where.append("active=1")
    if low_stock_only:
        where.append("stock_qty <= reorder_level")
    if category:
        where.append("category = ?")
        params.append(category.strip())

    if q and q.strip():
        qq = f"%{q.strip()}%"
        where.append("(name LIKE ? OR sku LIKE ? OR barcode LIKE ?)")
        params.extend([qq, qq, qq])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    cur.execute(f"SELECT * FROM products {where_sql} ORDER BY id DESC LIMIT ?;", (*params, limit))
    rows = cur.fetchall()
    conn.close()
    return [row_product_to_out(r) for r in rows]

@app.get("/api/products/{product_id}", response_model=ProductOut)
def get_product(product_id: int):
    conn = db_conn()
    cur = conn.cursor()
    r = get_product_row(cur, product_id)
    conn.close()
    return row_product_to_out(r)

@app.get("/api/products/lookup", response_model=Optional[ProductOut])
def lookup_product(barcode: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE barcode=? AND active=1 LIMIT 1;", (barcode.strip(),))
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return row_product_to_out(r)

@app.post("/api/products", response_model=ProductOut)
def create_product(p: ProductIn):
    conn = db_conn()
    cur = conn.cursor()

    now = utc_now()
    try:
        cur.execute(
            """INSERT INTO products
               (name, barcode, sku, category, unit, price_cents, cost_cents, taxable, stock_qty, reorder_level, active, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
            (
                p.name.strip(),
                (p.barcode.strip() if p.barcode else None),
                (p.sku.strip() if p.sku else None),
                (p.category.strip() if p.category else None),
                p.unit,
                dollars_to_cents(p.price),
                dollars_to_cents(p.cost),
                1 if p.taxable else 0,
                int(p.stock_qty),
                int(p.reorder_level),
                1 if p.active else 0,
                now,
                now
            )
        )
        conn.commit()
        new_id = cur.lastrowid
        cur.execute("SELECT * FROM products WHERE id=?;", (new_id,))
        r = cur.fetchone()
        return row_product_to_out(r)

    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Barcode already exists. Use a different barcode.")
    finally:
        conn.close()

@app.put("/api/products/{product_id}", response_model=ProductOut)
def update_product(product_id: int, p: ProductIn):
    conn = db_conn()
    cur = conn.cursor()

    # ensure exists
    _ = get_product_row(cur, product_id)
    now = utc_now()

    try:
        cur.execute(
            """UPDATE products
               SET name=?, barcode=?, sku=?, category=?, unit=?,
                   price_cents=?, cost_cents=?, taxable=?,
                   stock_qty=?, reorder_level=?, active=?,
                   updated_at=?
               WHERE id=?;""",
            (
                p.name.strip(),
                (p.barcode.strip() if p.barcode else None),
                (p.sku.strip() if p.sku else None),
                (p.category.strip() if p.category else None),
                p.unit,
                dollars_to_cents(p.price),
                dollars_to_cents(p.cost),
                1 if p.taxable else 0,
                int(p.stock_qty),
                int(p.reorder_level),
                1 if p.active else 0,
                now,
                product_id
            )
        )
        conn.commit()
        cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
        r = cur.fetchone()
        return row_product_to_out(r)

    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Barcode already exists. Use a different barcode.")
    finally:
        conn.close()

@app.delete("/api/products/{product_id}")
def deactivate_product(product_id: int):
    conn = db_conn()
    cur = conn.cursor()

    _ = get_product_row(cur, product_id)

    cur.execute("UPDATE products SET active=0, updated_at=? WHERE id=?;", (utc_now(), product_id))
    conn.commit()
    conn.close()
    return {"ok": True, "message": "Product deactivated (soft delete)"}

# ----------------------------
# Inventory API
# ----------------------------
@app.post("/api/products/{product_id}/stock/adjust", response_model=ProductOut)
def adjust_stock(product_id: int, payload: StockAdjustIn):
    """
    Adjust stock by delta (can be + or -)
    Example:
      +10 receiving
      -2 damaged/spoilage
    """
    conn = db_conn()
    cur = conn.cursor()
    r = get_product_row(cur, product_id)

    current = int(r["stock_qty"])
    new_qty = current + int(payload.delta_qty)
    if new_qty < 0:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Stock cannot go below 0 (current={current}, delta={payload.delta_qty})")

    now = utc_now()
    cur.execute("UPDATE products SET stock_qty=?, updated_at=? WHERE id=?;", (new_qty, now, product_id))
    cur.execute(
        """INSERT INTO inventory_movements (product_id, type, delta_qty, resulting_qty, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?);""",
        (product_id, "adjust", int(payload.delta_qty), new_qty, payload.note, now)
    )

    conn.commit()
    cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
    out = cur.fetchone()
    conn.close()
    return row_product_to_out(out)

@app.post("/api/products/{product_id}/stock/receive", response_model=ProductOut)
def receive_stock(product_id: int, payload: StockAdjustIn):
    """
    Receive stock (delta must be positive)
    """
    if payload.delta_qty <= 0:
        raise HTTPException(status_code=400, detail="receive requires delta_qty > 0")

    conn = db_conn()
    cur = conn.cursor()
    r = get_product_row(cur, product_id)

    current = int(r["stock_qty"])
    new_qty = current + int(payload.delta_qty)

    now = utc_now()
    cur.execute("UPDATE products SET stock_qty=?, updated_at=? WHERE id=?;", (new_qty, now, product_id))
    cur.execute(
        """INSERT INTO inventory_movements (product_id, type, delta_qty, resulting_qty, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?);""",
        (product_id, "receive", int(payload.delta_qty), new_qty, payload.note, now)
    )

    conn.commit()
    cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
    out = cur.fetchone()
    conn.close()
    return row_product_to_out(out)

@app.post("/api/products/{product_id}/stock/set", response_model=ProductOut)
def set_stock(product_id: int, payload: StockSetIn):
    """
    Hard set stock to an exact quantity (useful for inventory counts).
    """
    conn = db_conn()
    cur = conn.cursor()
    r = get_product_row(cur, product_id)

    current = int(r["stock_qty"])
    new_qty = int(payload.new_qty)
    delta = new_qty - current

    now = utc_now()
    cur.execute("UPDATE products SET stock_qty=?, updated_at=? WHERE id=?;", (new_qty, now, product_id))
    cur.execute(
        """INSERT INTO inventory_movements (product_id, type, delta_qty, resulting_qty, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?);""",
        (product_id, "set", int(delta), new_qty, payload.note, now)
    )

    conn.commit()
    cur.execute("SELECT * FROM products WHERE id=?;", (product_id,))
    out = cur.fetchone()
    conn.close()
    return row_product_to_out(out)

@app.get("/api/products/{product_id}/inventory", response_model=List[InventoryMoveOut])
def inventory_history(product_id: int, limit: int = 100):
    limit = max(1, min(limit, 500))
    conn = db_conn()
    cur = conn.cursor()
    _ = get_product_row(cur, product_id)

    cur.execute(
        "SELECT * FROM inventory_movements WHERE product_id=? ORDER BY id DESC LIMIT ?;",
        (product_id, limit)
    )
    rows = cur.fetchall()
    conn.close()

    return [
        InventoryMoveOut(
            id=r["id"],
            product_id=r["product_id"],
            type=r["type"],
            delta_qty=r["delta_qty"],
            resulting_qty=r["resulting_qty"],
            note=r["note"],
            created_at=r["created_at"]
        ) for r in rows
    ]
