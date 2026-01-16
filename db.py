from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

CANON_COLS = ["supplier", "product", "details", "website", "phone", "login_info"]


def get_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def init_db(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS records (
                    id SERIAL PRIMARY KEY,
                    dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                    supplier TEXT NOT NULL,
                    product TEXT NOT NULL,
                    details TEXT,
                    website TEXT,
                    phone TEXT,
                    login_info TEXT
                );
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_records_dataset ON records(dataset_id);"))


def get_or_create_dataset_id(engine: Engine, name: str) -> int:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM datasets WHERE name=:n"), {"n": name}).fetchone()
        if row:
            return int(row[0])
        row = conn.execute(
            text("INSERT INTO datasets(name) VALUES (:n) RETURNING id"),
            {"n": name},
        ).fetchone()
        return int(row[0])


def list_datasets(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT id, name, created_at FROM datasets ORDER BY name", engine)


def load_dataset(engine: Engine, dataset_id: int) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT id, supplier, product, details, website, phone, login_info
            FROM records
            WHERE dataset_id = :did
            ORDER BY id
            """
        ),
        engine,
        params={"did": dataset_id},
    )


def replace_dataset_with_df(engine: Engine, dataset_id: int, df: pd.DataFrame) -> None:
    """
    Replace all records in dataset_id with df (canonical columns).
    """
    df = df.copy()
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[CANON_COLS].fillna("").astype(str)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM records WHERE dataset_id=:did"), {"did": dataset_id})
        if df.empty:
            return

        conn.execute(
            text(
                """
                INSERT INTO records(dataset_id, supplier, product, details, website, phone, login_info)
                VALUES (:did, :supplier, :product, :details, :website, :phone, :login_info)
                """
            ),
            [
                {
                    "did": dataset_id,
                    "supplier": r["supplier"],
                    "product": r["product"],
                    "details": r["details"],
                    "website": r["website"],
                    "phone": r["phone"],
                    "login_info": r["login_info"],
                }
                for _, r in df.iterrows()
            ],
        )


def add_record(
    engine: Engine,
    dataset_id: int,
    supplier: str,
    product: str,
    details: str = "",
    website: str = "",
    phone: str = "",
    login_info: str = "",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO records(dataset_id, supplier, product, details, website, phone, login_info)
                VALUES (:did, :s, :p, :d, :w, :ph, :li)
                """
            ),
            {"did": dataset_id, "s": supplier, "p": product, "d": details, "w": website, "ph": phone, "li": login_info},
        )


def delete_records(engine: Engine, dataset_id: int, record_ids: list[int]) -> int:
    if not record_ids:
        return 0
    with engine.begin() as conn:
        res = conn.execute(
            text("DELETE FROM records WHERE dataset_id=:did AND id = ANY(:ids)"),
            {"did": dataset_id, "ids": record_ids},
        )
        return int(res.rowcount or 0)
