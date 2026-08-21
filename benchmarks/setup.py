from __future__ import annotations

import argparse
import os
import random
from datetime import date, timedelta

import psycopg
from psycopg import sql


SEED = 1337


# ============================================================
# DATABASE
# ============================================================

def get_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set.\n"
            "Example:\n"
            'export DATABASE_URL="postgresql://user:password@host/neondb?sslmode=require"'
        )

    return psycopg.connect(database_url)


# ============================================================
# DATASET CONFIGURATION
# ============================================================

SCALES = {
    "small": {
        "customers": 10_000,
        "categories": 100,
        "suppliers": 500,
        "products": 5_000,
        "orders": 100_000,
        "order_items": 300_000,
        "payments": 100_000,
        "warehouses": 50,
        "shipments": 100_000,
    },
    "medium": {
        "customers": 100_000,
        "categories": 500,
        "suppliers": 2_000,
        "products": 50_000,
        "orders": 1_000_000,
        "order_items": 3_000_000,
        "payments": 1_000_000,
        "warehouses": 200,
        "shipments": 1_000_000,
    },
}


# ============================================================
# GENERATORS
# ============================================================

def generate_customers(count: int):
    countries = [
        "India",
        "USA",
        "Germany",
        "Canada",
        "UK",
        "France",
        "Brazil",
        "Japan",
    ]

    segments = [
        "Enterprise",
        "SMB",
        "Consumer",
        "Retail",
    ]

    for customer_id in range(1, count + 1):
        yield (
            customer_id,
            f"Customer {customer_id}",
            countries[(customer_id * 7) % len(countries)],
            date(2020, 1, 1)
            + timedelta(days=(customer_id * 17) % 1800),
            segments[(customer_id * 3) % len(segments)],
        )


def generate_categories(count: int):
    for category_id in range(1, count + 1):
        yield (
            category_id,
            f"Category {category_id}",
        )


def generate_suppliers(count: int):
    countries = [
        "India",
        "USA",
        "Germany",
        "Japan",
        "France",
    ]

    for supplier_id in range(1, count + 1):
        yield (
            supplier_id,
            f"Supplier {supplier_id}",
            countries[(supplier_id * 5) % len(countries)],
        )


def generate_products(
    count: int,
    categories_count: int,
    suppliers_count: int,
    rng: random.Random,
):
    for product_id in range(1, count + 1):
        category_id = ((product_id - 1) % categories_count) + 1
        supplier_id = ((product_id * 3) % suppliers_count) + 1

        price = round(rng.uniform(10.0, 300.0), 2)

        # Roughly 1/7 products are inactive.
        active = product_id % 7 != 0

        yield (
            product_id,
            category_id,
            supplier_id,
            f"Product {product_id}",
            price,
            active,
        )


def generate_orders(
    count: int,
    customers_count: int,
    rng: random.Random,
):
    statuses = [
        "paid",
        "pending",
        "shipped",
        "cancelled",
    ]

    for order_id in range(1, count + 1):
        customer_id = ((order_id * 13) % customers_count) + 1

        order_date = (
            date(2024, 1, 1)
            + timedelta(days=(order_id * 17) % 730)
        )

        status = statuses[(order_id * 5) % len(statuses)]

        total_amount = round(
            rng.uniform(25.0, 2500.0),
            2,
        )

        yield (
            order_id,
            customer_id,
            order_date,
            status,
            total_amount,
        )


def generate_order_items(
    count: int,
    orders_count: int,
    products_count: int,
    rng: random.Random,
):
    """
    Generate exactly `count` order items.

    Orders receive 2-5 items, but generation continues
    deterministically until the requested count is reached.
    """

    item_id = 1
    order_id = 1

    while item_id <= count:
        items_for_order = 2 + (order_id % 4)

        for _ in range(items_for_order):
            if item_id > count:
                break

            product_id = ((item_id * 7) % products_count) + 1

            quantity = 1 + (item_id % 5)

            # Deterministic price approximation.
            # The actual product price isn't required here
            # because the benchmark only needs realistic data.
            unit_price = round(
                rng.uniform(10.0, 300.0),
                2,
            )

            yield (
                item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
            )

            item_id += 1

        order_id += 1

        if order_id > orders_count:
            order_id = 1


def generate_payments(
    count: int,
    orders_count: int,
    rng: random.Random,
):
    """
    Generate exactly `count` payment records.

    Not every order must have a payment.
    Some orders can have multiple payment records.
    """

    methods = [
        "card",
        "bank_transfer",
        "wallet",
        "cash",
    ]

    statuses = [
        "success",
        "failed",
        "pending",
    ]

    payment_id = 1

    # First pass:
    # approximately 2/3 of orders get a payment.
    for order_id in range(1, orders_count + 1):
        if payment_id > count:
            break

        if order_id % 3 == 0:
            continue

        payment_date = (
            date(2024, 1, 1)
            + timedelta(days=(payment_id * 13) % 700)
        )

        amount = round(
            rng.uniform(20.0, 2400.0),
            2,
        )

        yield (
            payment_id,
            order_id,
            payment_date,
            amount,
            methods[payment_id % len(methods)],
            statuses[payment_id % len(statuses)],
        )

        payment_id += 1

    # Additional passes allow the exact target count
    # to be reached, including the medium dataset.
    order_id = 1

    while payment_id <= count:
        payment_date = (
            date(2024, 1, 1)
            + timedelta(days=(payment_id * 13) % 700)
        )

        amount = round(
            rng.uniform(20.0, 2400.0),
            2,
        )

        yield (
            payment_id,
            order_id,
            payment_date,
            amount,
            methods[payment_id % len(methods)],
            statuses[payment_id % len(statuses)],
        )

        payment_id += 1
        order_id += 1

        if order_id > orders_count:
            order_id = 1


def generate_warehouses(count: int):
    regions = [
        "North",
        "South",
        "East",
        "West",
    ]

    for warehouse_id in range(1, count + 1):
        yield (
            warehouse_id,
            f"Warehouse {warehouse_id}",
            regions[(warehouse_id - 1) % len(regions)],
        )


def generate_shipments(
    count: int,
    orders_count: int,
    warehouses_count: int,
    rng: random.Random,
):
    """
    Generate exactly `count` shipment records.

    Some orders intentionally have no shipment.
    Some orders may have multiple shipment records.
    """

    statuses = [
        "delivered",
        "in_transit",
        "pending",
    ]

    shipment_id = 1

    # First pass: leave roughly 1/4 of orders without shipments.
    for order_id in range(1, orders_count + 1):
        if shipment_id > count:
            break

        if order_id % 4 == 0:
            continue

        ship_date = (
            date(2024, 1, 1)
            + timedelta(days=(shipment_id * 11) % 500)
        )

        delivery_date = ship_date + timedelta(
            days=(shipment_id % 10) + 2
        )

        warehouse_id = (
            (shipment_id % warehouses_count) + 1
        )

        yield (
            shipment_id,
            order_id,
            warehouse_id,
            ship_date,
            delivery_date,
            statuses[shipment_id % len(statuses)],
        )

        shipment_id += 1

    # Additional passes if exact target count hasn't
    # been reached.
    order_id = 1

    while shipment_id <= count:
        ship_date = (
            date(2024, 1, 1)
            + timedelta(days=(shipment_id * 11) % 500)
        )

        delivery_date = ship_date + timedelta(
            days=(shipment_id % 10) + 2
        )

        warehouse_id = (
            (shipment_id % warehouses_count) + 1
        )

        yield (
            shipment_id,
            order_id,
            warehouse_id,
            ship_date,
            delivery_date,
            statuses[shipment_id % len(statuses)],
        )

        shipment_id += 1
        order_id += 1

        if order_id > orders_count:
            order_id = 1


# ============================================================
# BULK INSERT
# ============================================================

def copy_rows(
    conn,
    table_name: str,
    columns: list[str],
    rows,
):
    """
    Use PostgreSQL COPY for fast bulk loading.
    """

    column_sql = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in columns
    )

    query = sql.SQL(
        "COPY {} ({}) FROM STDIN"
    ).format(
        sql.Identifier(table_name),
        column_sql,
    )

    with conn.cursor() as cur:
        with cur.copy(query) as copy:
            for row in rows:
                copy.write_row(row)


# ============================================================
# DATABASE RESET
# ============================================================

def clear_existing_data(conn):
    print("\nClearing existing benchmark data...")

    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                order_items,
                payments,
                shipments,
                orders,
                products,
                suppliers,
                categories,
                customers,
                warehouses
            RESTART IDENTITY CASCADE;
            """
        )

    conn.commit()

    print("Existing data cleared.")


# ============================================================
# LOAD DATASET
# ============================================================

def seed_database(scale: str):
    config = SCALES[scale]

    rng = random.Random(SEED)

    print("=" * 70)
    print("NatQuery Neon Benchmark Database Seeder")
    print("=" * 70)

    print(f"\nScale: {scale}")
    print(f"Random seed: {SEED}")

    print("\nTarget row counts:")

    for table, count in config.items():
        print(f"  {table:<15} {count:>12,}")

    print()

    with get_connection() as conn:

        # ----------------------------------------------------
        # Clear existing data
        # ----------------------------------------------------

        clear_existing_data(conn)

        # ----------------------------------------------------
        # Categories
        # ----------------------------------------------------

        print("Loading categories...")

        copy_rows(
            conn,
            "categories",
            [
                "category_id",
                "name",
            ],
            generate_categories(
                config["categories"]
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Customers
        # ----------------------------------------------------

        print("Loading customers...")

        copy_rows(
            conn,
            "customers",
            [
                "customer_id",
                "name",
                "country",
                "signup_date",
                "segment",
            ],
            generate_customers(
                config["customers"]
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Suppliers
        # ----------------------------------------------------

        print("Loading suppliers...")

        copy_rows(
            conn,
            "suppliers",
            [
                "supplier_id",
                "name",
                "country",
            ],
            generate_suppliers(
                config["suppliers"]
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Products
        # ----------------------------------------------------

        print("Loading products...")

        copy_rows(
            conn,
            "products",
            [
                "product_id",
                "category_id",
                "supplier_id",
                "name",
                "price",
                "active",
            ],
            generate_products(
                config["products"],
                config["categories"],
                config["suppliers"],
                rng,
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        print("Loading orders...")

        copy_rows(
            conn,
            "orders",
            [
                "order_id",
                "customer_id",
                "order_date",
                "status",
                "total_amount",
            ],
            generate_orders(
                config["orders"],
                config["customers"],
                rng,
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Order Items
        # ----------------------------------------------------

        print("Loading order_items...")

        copy_rows(
            conn,
            "order_items",
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
            ],
            generate_order_items(
                config["order_items"],
                config["orders"],
                config["products"],
                rng,
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Payments
        # ----------------------------------------------------

        print("Loading payments...")

        copy_rows(
            conn,
            "payments",
            [
                "payment_id",
                "order_id",
                "payment_date",
                "amount",
                "method",
                "status",
            ],
            generate_payments(
                config["payments"],
                config["orders"],
                rng,
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Warehouses
        # ----------------------------------------------------

        print("Loading warehouses...")

        copy_rows(
            conn,
            "warehouses",
            [
                "warehouse_id",
                "name",
                "region",
            ],
            generate_warehouses(
                config["warehouses"]
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Shipments
        # ----------------------------------------------------

        print("Loading shipments...")

        copy_rows(
            conn,
            "shipments",
            [
                "shipment_id",
                "order_id",
                "warehouse_id",
                "shipped_date",
                "delivery_date",
                "status",
            ],
            generate_shipments(
                config["shipments"],
                config["orders"],
                config["warehouses"],
                rng,
            ),
        )

        conn.commit()

        # ----------------------------------------------------
        # Analyze
        # ----------------------------------------------------

        print("\nRunning ANALYZE...")

        with conn.cursor() as cur:
            cur.execute("ANALYZE;")

        conn.commit()

        # ----------------------------------------------------
        # Verify
        # ----------------------------------------------------

        verify_database(
            conn,
            config,
        )


# ============================================================
# VERIFICATION
# ============================================================

def verify_database(conn, expected):
    print("\n" + "=" * 70)
    print("VERIFYING DATABASE")
    print("=" * 70)

    tables = [
        "customers",
        "categories",
        "suppliers",
        "products",
        "orders",
        "order_items",
        "payments",
        "warehouses",
        "shipments",
    ]

    all_ok = True

    with conn.cursor() as cur:

        print("\nRow counts:")

        for table in tables:
            cur.execute(
                sql.SQL(
                    "SELECT COUNT(*) FROM {}"
                ).format(
                    sql.Identifier(table)
                )
            )

            actual = cur.fetchone()[0]
            target = expected[table]

            ok = actual == target

            if not ok:
                all_ok = False

            status = "OK" if ok else "ERROR"

            print(
                f"  {table:<15} "
                f"{actual:>12,} / "
                f"{target:>12,} "
                f"[{status}]"
            )

        # ----------------------------------------------------
        # Foreign-key integrity
        # ----------------------------------------------------

        print("\nForeign-key integrity:")

        checks = [
            (
                "orders -> customers",
                """
                SELECT COUNT(*)
                FROM orders o
                LEFT JOIN customers c
                    ON c.customer_id = o.customer_id
                WHERE c.customer_id IS NULL
                """,
            ),
            (
                "products -> categories",
                """
                SELECT COUNT(*)
                FROM products p
                LEFT JOIN categories c
                    ON c.category_id = p.category_id
                WHERE c.category_id IS NULL
                """,
            ),
            (
                "products -> suppliers",
                """
                SELECT COUNT(*)
                FROM products p
                LEFT JOIN suppliers s
                    ON s.supplier_id = p.supplier_id
                WHERE s.supplier_id IS NULL
                """,
            ),
            (
                "order_items -> orders",
                """
                SELECT COUNT(*)
                FROM order_items oi
                LEFT JOIN orders o
                    ON o.order_id = oi.order_id
                WHERE o.order_id IS NULL
                """,
            ),
            (
                "order_items -> products",
                """
                SELECT COUNT(*)
                FROM order_items oi
                LEFT JOIN products p
                    ON p.product_id = oi.product_id
                WHERE p.product_id IS NULL
                """,
            ),
            (
                "payments -> orders",
                """
                SELECT COUNT(*)
                FROM payments p
                LEFT JOIN orders o
                    ON o.order_id = p.order_id
                WHERE o.order_id IS NULL
                """,
            ),
            (
                "shipments -> orders",
                """
                SELECT COUNT(*)
                FROM shipments s
                LEFT JOIN orders o
                    ON o.order_id = s.order_id
                WHERE o.order_id IS NULL
                """,
            ),
            (
                "shipments -> warehouses",
                """
                SELECT COUNT(*)
                FROM shipments s
                LEFT JOIN warehouses w
                    ON w.warehouse_id = s.warehouse_id
                WHERE w.warehouse_id IS NULL
                """,
            ),
        ]

        for name, query in checks:
            cur.execute(query)
            invalid = cur.fetchone()[0]

            ok = invalid == 0

            if not ok:
                all_ok = False

            status = "OK" if ok else f"ERROR ({invalid})"

            print(
                f"  {name:<30} [{status}]"
            )

    print()

    if all_ok:
        print("=" * 70)
        print("SUCCESS")
        print("=" * 70)
        print("Neon benchmark database is ready.")
    else:
        print("=" * 70)
        print("ERROR")
        print("=" * 70)
        print("Database verification failed.")
        raise RuntimeError(
            "Benchmark database verification failed."
        )


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Seed the NatQuery benchmark database on Neon."
    )

    parser.add_argument(
        "--scale",
        choices=["small", "medium"],
        default="small",
        help="Dataset size. Default: small.",
    )

    args = parser.parse_args()

    seed_database(args.scale)


if __name__ == "__main__":
    main()