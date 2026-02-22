import sqlite3
from pathlib import Path
from typing import List, Optional

class Database:
    def __init__(self, db_path: str = "data.db"):
        self.db_path = Path(db_path)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> self:
        """Initialize database schema."""
        # TODO: check if already init?
        with self._get_connection() as conn:
            with open("schema.sql","r") as f:
                conn.executescript(f.read())
        return self

class CostOfLivingRepository:
    def __init(self, database: Database):
        self.database = database

    def upsert_location(...):
        pass
    def upsert_item(...):
        pass
    def insert_observation(...):
        pass
    def get_latest_city_data(...):
        pass
    def compare_cities(...):
        pass
