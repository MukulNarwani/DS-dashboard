import sqlite3
from pathlib import Path

import pytest

from db import CostOfLivingRepository, Database


def test_initialize_creates_expected_schema_and_removes_stale_tables(
    tmp_path: Path,
) -> None:
    database = Database(str(tmp_path / "schema.db")).initialize()

    with database._get_connection() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        views = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'view'"
            ).fetchall()
        }

    assert "countries" in tables
    assert "cities" in tables
    assert "salary_benchmarks" in tables
    assert "job_postings" not in tables
    assert "role_categories" not in tables
    assert "latest_cost_observations" in views


def test_database_connections_enable_foreign_keys(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "fk.db")).initialize()

    with database._get_connection() as connection:
        pragma_value = connection.execute("PRAGMA foreign_keys").fetchone()[
            "foreign_keys"
        ]

    assert pragma_value == 1


def test_cost_observations_keep_history_and_latest_read_uses_newest_snapshot(
    tmp_path: Path,
) -> None:
    database = Database(str(tmp_path / "history.db")).initialize()
    repository = CostOfLivingRepository(database)

    country_id, city_id = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
    )
    item_id = repository.upsert_item(
        "Apartment (1 bedroom) in City Centre",
        "Housing",
    )
    repository.upsert_observation(
        city_id=city_id,
        item_id=item_id,
        price_avg=1800.0,
        currency="GBP",
        snapshot_date="2026-04-01",
    )
    repository.upsert_observation(
        city_id=city_id,
        item_id=item_id,
        price_avg=2200.0,
        currency="GBP",
        snapshot_date="2026-04-04",
    )

    with database._get_connection() as connection:
        historical_rows = connection.execute(
            """
            SELECT snapshot_date, price_avg
            FROM cost_observations
            WHERE city_id = ? AND item_id = ?
            ORDER BY snapshot_date
            """,
            (city_id, item_id),
        ).fetchall()

    latest_rows = repository.get_latest_city_data("London")

    assert country_id is not None
    assert historical_rows == [
        {"snapshot_date": "2026-04-01", "price_avg": 1800.0},
        {"snapshot_date": "2026-04-04", "price_avg": 2200.0},
    ]
    assert latest_rows == [
        {
            "category": "Housing",
            "item": "Apartment (1 bedroom) in City Centre",
            "price_avg": 2200.0,
            "price_min": None,
            "price_max": None,
            "currency": "GBP",
            "snapshot_date": "2026-04-04",
        }
    ]


def test_compare_cities_uses_latest_snapshots_for_both_cities(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "compare.db")).initialize()
    repository = CostOfLivingRepository(database)

    _, london_id = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
    )
    _, paris_id = repository.upsert_location(
        city_name="Paris",
        country_name="France",
        numbeo_slug="Paris",
    )
    item_id = repository.upsert_item("Monthly Pass", "Transport")

    repository.upsert_observation(
        city_id=london_id,
        item_id=item_id,
        price_avg=150.0,
        currency="GBP",
        snapshot_date="2026-04-01",
    )
    repository.upsert_observation(
        city_id=london_id,
        item_id=item_id,
        price_avg=180.0,
        currency="GBP",
        snapshot_date="2026-04-04",
    )
    repository.upsert_observation(
        city_id=paris_id,
        item_id=item_id,
        price_avg=90.0,
        currency="EUR",
        snapshot_date="2026-04-02",
    )
    repository.upsert_observation(
        city_id=paris_id,
        item_id=item_id,
        price_avg=100.0,
        currency="EUR",
        snapshot_date="2026-04-03",
    )

    rows = repository.compare_cities("London", "Paris")

    assert rows == [
        {
            "category": "Transport",
            "item": "Monthly Pass",
            "price_a": 180.0,
            "price_b": 100.0,
            "currency_a": "GBP",
            "currency_b": "EUR",
            "pct_diff": -44.44,
        }
    ]


def test_city_reads_fail_loudly_when_city_name_is_ambiguous(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "ambiguous.db")).initialize()
    repository = CostOfLivingRepository(database)

    repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
    )
    repository.upsert_location(
        city_name="London",
        country_name="Canada",
        numbeo_slug="London-Canada",
    )

    with pytest.raises(ValueError, match="City name 'London' is ambiguous"):
        repository.get_latest_city_data("London")

    with pytest.raises(ValueError, match="City name 'London' is ambiguous"):
        repository.compare_cities("London", "Paris")


def test_schema_declares_expected_indexes(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "indexes.db")).initialize()

    with database._get_connection() as connection:
        fx_indexes = {
            row["name"] for row in connection.execute("PRAGMA index_list(fx_rates)")
        }
        cost_indexes = {
            row["name"]
            for row in connection.execute("PRAGMA index_list(cost_observations)")
        }
        salary_indexes = {
            row["name"]
            for row in connection.execute("PRAGMA index_list(salary_benchmarks)")
        }

    assert "idx_fx_rates_currency_date" in fx_indexes
    assert "idx_cost_observations_city_item_snapshot" in cost_indexes
    assert "idx_cost_observations_item_city_snapshot" in cost_indexes
    assert "idx_salary_benchmarks_role_city_date" in salary_indexes
    assert "idx_salary_benchmarks_role_country_date" in salary_indexes


def test_query_plans_use_declared_indexes(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "plans.db")).initialize()

    with database._get_connection() as connection:
        fx_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT rate_to_usd
            FROM fx_rates
            WHERE currency_from = ?
            ORDER BY date DESC
            LIMIT 1
            """,
            ("GBP",),
        ).fetchall()
        latest_cost_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT *
            FROM latest_cost_observations
            WHERE city_id = ? AND item_id = ?
            """,
            (1, 1),
        ).fetchall()

    fx_plan_text = " ".join(row["detail"] for row in fx_plan)
    latest_cost_plan_text = " ".join(row["detail"] for row in latest_cost_plan)

    assert "idx_fx_rates_currency_date" in fx_plan_text
    assert (
        "idx_cost_observations_city_item_snapshot" in latest_cost_plan_text
        or "idx_cost_observations_item_city_snapshot" in latest_cost_plan_text
    )


def test_salary_benchmarks_reject_city_country_mismatch(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "salary_fk.db")).initialize()

    with database._get_connection() as connection:
        connection.execute(
            "INSERT INTO countries (name) VALUES (?)",
            ("United Kingdom",),
        )
        connection.execute(
            "INSERT INTO countries (name) VALUES (?)",
            ("Canada",),
        )
        uk_id = connection.execute(
            "SELECT id FROM countries WHERE name = ?",
            ("United Kingdom",),
        ).fetchone()["id"]
        canada_id = connection.execute(
            "SELECT id FROM countries WHERE name = ?",
            ("Canada",),
        ).fetchone()["id"]
        connection.execute(
            "INSERT INTO cities (name, country_id, numbeo_slug) VALUES (?, ?, ?)",
            ("London", uk_id, "London"),
        )
        london_id = connection.execute(
            "SELECT id FROM cities WHERE name = ? AND country_id = ?",
            ("London", uk_id),
        ).fetchone()["id"]

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO salary_benchmarks (
                    scraped_date, role_category, location_name, location_country,
                    country_id, city_id, location_granularity,
                    salary_median, currency
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-05",
                    "data_scientist",
                    "London",
                    "Canada",
                    canada_id,
                    london_id,
                    "city",
                    100000.0,
                    "CAD",
                ),
            )
