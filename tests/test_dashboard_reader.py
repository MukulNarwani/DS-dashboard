from pathlib import Path

import pytest

from src.dashboard_reader import DashboardReader
from src.db import CostOfLivingRepository, Database


def _insert_salary_benchmark(
    database: Database,
    *,
    scraped_date: str,
    role_category: str,
    location_name: str,
    location_country: str,
    country_id: int,
    city_id: int | None,
    location_granularity: str,
    salary_median: float,
    currency: str,
) -> None:
    with database._get_connection() as connection:
        connection.execute(
            """
            INSERT INTO salary_benchmarks (
                scraped_date,
                role_category,
                location_name,
                location_country,
                country_id,
                city_id,
                location_granularity,
                salary_median,
                currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scraped_date,
                role_category,
                location_name,
                location_country,
                country_id,
                city_id,
                location_granularity,
                salary_median,
                currency,
            ),
        )


def test_get_city_returns_canonical_city_record(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "reader.db")).initialize()
    repository = CostOfLivingRepository(database)
    country_id, city_id = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
        iso_code="GBR",
    )

    city = DashboardReader(database).get_city("London", "United Kingdom")

    assert city is not None
    assert city.id == city_id
    assert city.country_id == country_id
    assert city.country_name == "United Kingdom"
    assert city.iso_code == "GBR"
    assert city.numbeo_slug == "London"


def test_get_city_requires_country_when_name_is_ambiguous(tmp_path: Path) -> None:
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
        numbeo_slug="London-on",
    )

    with pytest.raises(ValueError, match="Multiple cities named 'London' found"):
        DashboardReader(database).get_city("London")


def test_get_cost_snapshot_supports_latest_and_explicit_dates(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "costs.db")).initialize()
    repository = CostOfLivingRepository(database)
    _, city_id = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
    )
    item_id = repository.upsert_item("Monthly Pass", "Transport")
    repository.upsert_observation(
        city_id=city_id,
        item_id=item_id,
        price_avg=150.0,
        currency="GBP",
        snapshot_date="2026-04-01",
    )
    repository.upsert_observation(
        city_id=city_id,
        item_id=item_id,
        price_avg=180.0,
        currency="GBP",
        sample_size=12,
        data_last_updated="2026-04-04T08:15:00",
        snapshot_date="2026-04-04",
    )

    reader = DashboardReader(database)
    latest_rows = reader.get_cost_snapshot(city_id)
    explicit_rows = reader.get_cost_snapshot(city_id, snapshot_date="2026-04-01")

    assert latest_rows == [
        latest_rows[0].__class__(
            city_id=city_id,
            city_name="London",
            country_id=1,
            country_name="United Kingdom",
            category="Transport",
            item="Monthly Pass",
            snapshot_date="2026-04-04",
            price_avg=180.0,
            price_min=None,
            price_max=None,
            currency="GBP",
            sample_size=12,
            data_last_updated="2026-04-04T08:15:00",
        )
    ]
    assert explicit_rows == [
        explicit_rows[0].__class__(
            city_id=city_id,
            city_name="London",
            country_id=1,
            country_name="United Kingdom",
            category="Transport",
            item="Monthly Pass",
            snapshot_date="2026-04-01",
            price_avg=150.0,
            price_min=None,
            price_max=None,
            currency="GBP",
            sample_size=None,
            data_last_updated=None,
        )
    ]


def test_get_salary_benchmark_reads_city_and_country_scopes(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "salary.db")).initialize()
    repository = CostOfLivingRepository(database)
    country_id, city_id = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
        iso_code="GBR",
    )
    _insert_salary_benchmark(
        database,
        scraped_date="2026-04-01",
        role_category="data_scientist",
        location_name="London",
        location_country="United Kingdom",
        country_id=country_id,
        city_id=city_id,
        location_granularity="city",
        salary_median=61000.0,
        currency="GBP",
    )
    _insert_salary_benchmark(
        database,
        scraped_date="2026-04-04",
        role_category="data_scientist",
        location_name="London",
        location_country="United Kingdom",
        country_id=country_id,
        city_id=city_id,
        location_granularity="city",
        salary_median=63000.0,
        currency="GBP",
    )
    _insert_salary_benchmark(
        database,
        scraped_date="2026-04-02",
        role_category="data_scientist",
        location_name="United Kingdom",
        location_country="United Kingdom",
        country_id=country_id,
        city_id=None,
        location_granularity="country",
        salary_median=58000.0,
        currency="GBP",
    )

    reader = DashboardReader(database)
    city_benchmark = reader.get_salary_benchmark(
        "data_scientist",
        city_id=city_id,
    )
    country_benchmark = reader.get_salary_benchmark(
        "data_scientist",
        country_id=country_id,
    )

    assert city_benchmark is not None
    assert city_benchmark.scraped_date == "2026-04-04"
    assert city_benchmark.location_granularity == "city"
    assert city_benchmark.salary_median == 63000.0
    assert country_benchmark is not None
    assert country_benchmark.scraped_date == "2026-04-02"
    assert country_benchmark.location_granularity == "country"
    assert country_benchmark.city_id is None
    assert country_benchmark.salary_median == 58000.0


def test_get_ppp_factor_uses_country_iso_code_and_latest_year(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "ppp.db")).initialize()
    repository = CostOfLivingRepository(database)
    country_id, _ = repository.upsert_location(
        city_name="London",
        country_name="United Kingdom",
        numbeo_slug="London",
        iso_code="GBR",
    )

    with database._get_connection() as connection:
        connection.execute(
            """
            INSERT INTO ppp_factors (country_name, iso_code, year, factor)
            VALUES (?, ?, ?, ?), (?, ?, ?, ?)
            """,
            (
                "United Kingdom",
                "GBR",
                2024,
                0.71,
                "United Kingdom",
                "GBR",
                2025,
                0.73,
            ),
        )

    reader = DashboardReader(database)
    latest_factor = reader.get_ppp_factor(country_id)
    specific_factor = reader.get_ppp_factor(country_id, year=2024)

    assert latest_factor is not None
    assert latest_factor.year == 2025
    assert latest_factor.factor == 0.73
    assert specific_factor is not None
    assert specific_factor.year == 2024
    assert specific_factor.factor == 0.71


def test_get_fx_rate_supports_as_of_cutoff(tmp_path: Path) -> None:
    database = Database(str(tmp_path / "fx.db")).initialize()

    with database._get_connection() as connection:
        connection.execute(
            """
            INSERT INTO fx_rates (date, currency_from, rate_to_usd)
            VALUES (?, ?, ?), (?, ?, ?)
            """,
            (
                "2026-04-01",
                "GBP",
                1.25,
                "2026-04-04",
                "GBP",
                1.30,
            ),
        )

    reader = DashboardReader(database)
    latest_rate = reader.get_fx_rate("gbp")
    historical_rate = reader.get_fx_rate("GBP", as_of="2026-04-03")

    assert latest_rate is not None
    assert latest_rate.date == "2026-04-04"
    assert latest_rate.rate_to_usd == 1.30
    assert historical_rate is not None
    assert historical_rate.date == "2026-04-01"
    assert historical_rate.rate_to_usd == 1.25
