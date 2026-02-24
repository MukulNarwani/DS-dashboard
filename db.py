import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple


class Database:
    def __init__(self, db_path: str = "data.db"):
        self.db_path = Path(db_path)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        # conn.row_factory = sqlite3.Row
        conn.row_factory = lambda cursor, row: dict(zip([col[0] for col in cursor.description], row))
        return conn

    def initialize(self) -> "Database":
        """Initialize database schema. Safe to call multiple times if schema uses IF NOT EXISTS."""
        with self._get_connection() as conn:
            with open("schema.sql", "r") as f:
                conn.executescript(f.read())
        return self


class CostOfLivingRepository:
    def __init__(self, database: Database):
        self.database = database

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    def upsert_location(self, city_name: str, country_name: str, url_tail: str, iso_code: Optional[str] = None) -> Tuple[int, int]:
        """Insert or update a country + city. Returns (country_id, city_id)."""
        with self.database._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO countries (name, url_tail, iso_code)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    url_tail = excluded.url_tail,
                    iso_code = COALESCE(excluded.iso_code, countries.iso_code)
                """,
                (country_name, url_tail, iso_code),
            )
            country_id = conn.execute(
                "SELECT id FROM countries WHERE name = ?", (country_name,)
            ).fetchone()["id"]

            conn.execute(
                """
                INSERT INTO cities (name, country_id)
                VALUES (?, ?)
                ON CONFLICT(name, country_id) DO NOTHING
                """,
                (city_name, country_id),
            )
            city_id = conn.execute(
                "SELECT id FROM cities WHERE name = ? AND country_id = ?",
                (city_name, country_id),
            ).fetchone()["id"]

        return country_id, city_id

    # ------------------------------------------------------------------
    # Items / Categories
    # ------------------------------------------------------------------

    def upsert_item(self, item_name: str, category_name: str) -> int:
        """Insert or update a category and item. Returns item_id."""
        with self.database._get_connection() as conn:
            conn.execute(
                "INSERT INTO categories (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
                (category_name,),
            )
            category_id = conn.execute(
                "SELECT id FROM categories WHERE name = ?", (category_name,)
            ).fetchone()["id"]

            conn.execute(
                """
                INSERT INTO items (name, category_id)
                VALUES (?, ?)
                ON CONFLICT(name, category_id) DO NOTHING
                """,
                (item_name, category_id),
            )
            item_id = conn.execute(
                "SELECT id FROM items WHERE name = ? AND category_id = ?",
                (item_name, category_id),
            ).fetchone()["id"]

        return item_id

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------
    def upsert_observation(self, city_id, item_id, price_avg, currency, price_min=None, price_max=None):
        with self.database._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO cost_observations (city_id, item_id, price_avg, price_min, price_max, currency)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(city_id, item_id) DO UPDATE SET
                    price_avg = excluded.price_avg,
                    price_min = excluded.price_min,
                    price_max = excluded.price_max,
                    currency  = excluded.currency
                """, (city_id, item_id, price_avg, price_min, price_max, currency))


    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_latest_city_data(self, city_name: str) -> List[sqlite3.Row]:
        """Return the most recent observation for every item in a given city."""
        with self.database._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    cat.name  AS category,
                    it.name   AS item,
                    co.price_avg,
                    co.price_min,
                    co.price_max,
                    co.currency
                FROM cost_observations co
                JOIN cities ci   ON ci.id = co.city_id
                JOIN items  it   ON it.id = co.item_id
                JOIN categories cat ON cat.id = it.category_id
                WHERE ci.name = ?
                ORDER BY cat.name, it.name
                """,
                (city_name,),
            ).fetchall()
        return rows

    def compare_cities(self, city_a: str, city_b: str) -> List[sqlite3.Row]:
        """
        Return a side-by-side comparison of the latest prices for items
        available in both cities.
        """
        with self.database._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    cat.name        AS category,
                    it.name         AS item,
                    a.price_avg     AS price_a,
                    b.price_avg     AS price_b,
                    a.currency      AS currency_a,
                    b.currency      AS currency_b,
                    ROUND((b.price_avg - a.price_avg) / NULLIF(a.price_avg, 0) * 100, 2) AS pct_diff
                FROM items it
                JOIN categories cat ON cat.id = it.category_id
                -- Latest observation for city A
                JOIN cost_observations a ON a.item_id = it.id
                    AND a.city_id = (SELECT id FROM cities WHERE name = ?)
                    
                -- Latest observation for city B
                JOIN cost_observations b ON b.item_id = it.id
                    AND b.city_id = (SELECT id FROM cities WHERE name = ?)
                    
                ORDER BY cat.name, it.name
                """,
                (city_a, city_b),
            ).fetchall()
        return rows
