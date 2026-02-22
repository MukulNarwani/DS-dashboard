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

    def initialize(self) -> None:
        """Initialize database schema."""
        with self._get_connection() as conn:
            with open("schema.sql","r") as f:
                conn.executescript(f.read())

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
#
# class ArticleRepository:
#     def __init__(self, database: Database):
#         self.database = database
#
#     def insert(self, title: str, url: str, content: Optional[str]) -> None:
#         """Insert article safely (ignores duplicates by URL)."""
#         with self.database._get_connection() as conn:
#             conn.execute("""
#                 INSERT OR IGNORE INTO articles (title, url, content)
#                 VALUES (?, ?, ?)
#             """, (title, url, content))
#
#     def get_all(self) -> List[sqlite3.Row]:
#         with self.database._get_connection() as conn:
#             return conn.execute(
#                 "SELECT * FROM articles ORDER BY created_at DESC"
#             ).fetchall()
#
#     def get_by_url(self, url: str) -> Optional[sqlite3.Row]:
#         with self.database._get_connection() as conn:
#             return conn.execute(
#                 "SELECT * FROM articles WHERE url = ?",
#                 (url,)
#             ).fetchone()
#
#     def delete(self, article_id: int) -> None:
#         with self.database._get_connection() as conn:
#             conn.execute(
#                 "DELETE FROM articles WHERE id = ?",
#                 (article_id,)
#             )
