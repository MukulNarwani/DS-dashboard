import sqlite3

import pytest

from db import Database
from salary_scraper import (
    GlassdoorLocation,
    GlassdoorSalaryRepository,
    SalaryPageData,
    _is_valid_salary_page,
    _parse_salary_page,
)


def _location(currency: str, country: str, display_name: str) -> GlassdoorLocation:
    return GlassdoorLocation(
        display_name=display_name,
        country=country,
        currency=currency,
        subdomain="example.com",
        city_slug="",
        il_param="",
        city_slug_len=0,
    )


def test_repository_upsert_uses_initialized_schema(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    database = Database(str(tmp_path / "salary.db")).initialize()
    repository = GlassdoorSalaryRepository(database)
    location = _location("GBP", "United Kingdom", "London")
    salary_data = SalaryPageData(
        salary_median=61098.0,
        salary_p25=46362.0,
        salary_p75=82518.0,
        salary_p90=111456.0,
        sample_size=5488,
        currency="GBP",
    )

    repository.upsert(
        "2026-04-02", "data_scientist", location, salary_data, "https://example.com"
    )

    with database._get_connection() as connection:
        row = connection.execute(
            """
            SELECT
                scraped_date,
                role_category,
                location_name,
                location_country,
                country_id,
                city_id,
                location_granularity,
                currency,
                salary_median
            FROM salary_benchmarks
            """
        ).fetchone()

    assert row == {
        "scraped_date": "2026-04-02",
        "role_category": "data_scientist",
        "location_name": "London",
        "location_country": "United Kingdom",
        "country_id": 1,
        "city_id": 1,
        "location_granularity": "city",
        "currency": "GBP",
        "salary_median": 61098.0,
    }


def test_schema_requires_scraped_date_for_raw_benchmarks(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    database = Database(str(tmp_path / "salary.db")).initialize()

    with database._get_connection() as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO salary_benchmarks (
                    role_category, location_name, location_country, salary_median, currency
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("data_scientist", "London", "United Kingdom", 61098.0, "GBP"),
            )


def test_repository_upsert_marks_country_level_salary_rows(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    database = Database(str(tmp_path / "salary.db")).initialize()
    repository = GlassdoorSalaryRepository(database)
    location = _location("USD", "United States", "United States")
    salary_data = SalaryPageData(
        salary_median=154515.0,
        salary_p25=120000.0,
        salary_p75=190000.0,
        salary_p90=240000.0,
        sample_size=300,
        currency="USD",
    )

    repository.upsert(
        "2026-04-02", "data_scientist", location, salary_data, "https://example.com"
    )

    with database._get_connection() as connection:
        row = connection.execute(
            """
            SELECT country_id, city_id, location_granularity
            FROM salary_benchmarks
            """
        ).fetchone()

    assert row == {
        "country_id": 1,
        "city_id": None,
        "location_granularity": "country",
    }


def test_repository_upsert_allows_same_name_country_and_city_rows(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    database = Database(str(tmp_path / "salary.db")).initialize()
    repository = GlassdoorSalaryRepository(database)
    country_level = _location("SGD", "Singapore", "Singapore")
    city_level = GlassdoorLocation(
        display_name="Singapore",
        country="Singapore",
        currency="SGD",
        subdomain="example.com",
        city_slug="singapore-",
        il_param="IL.0,9_IM1123",
        city_slug_len=10,
    )
    salary_data = SalaryPageData(
        salary_median=100000.0,
        salary_p25=80000.0,
        salary_p75=120000.0,
        salary_p90=150000.0,
        sample_size=100,
        currency="SGD",
    )

    repository.upsert(
        "2026-04-02",
        "data_scientist",
        country_level,
        salary_data,
        "https://example.com/country",
    )
    repository.upsert(
        "2026-04-02",
        "data_scientist",
        city_level,
        salary_data,
        "https://example.com/city",
    )

    with database._get_connection() as connection:
        rows = connection.execute(
            """
            SELECT location_name, location_country, location_granularity
            FROM salary_benchmarks
            ORDER BY location_granularity
            """
        ).fetchall()

    assert rows == [
        {
            "location_name": "Singapore",
            "location_country": "Singapore",
            "location_granularity": "city",
        },
        {
            "location_name": "Singapore",
            "location_country": "Singapore",
            "location_granularity": "country",
        },
    ]


@pytest.mark.parametrize(
    ("html", "location", "expected_currency"),
    [
        (
            """
            <html><head><title>Glassdoor Salaries</title></head><body>
            The average salary for a Data Scientist is £61,098 per year
            between £46,362 (25th percentile) and £82,518
            making up to £111,456 (90th percentile)
            based on 5,488 salaries
            </body></html>
            """,
            _location("GBP", "United Kingdom", "London"),
            "GBP",
        ),
        (
            """
            <html><body>
            The average salary for a Data Scientist is $154,515 per year
            based on 300 salaries
            </body></html>
            """,
            _location("USD", "United States", "San Francisco"),
            "USD",
        ),
        (
            """
            <html><body>
            The average salary for a Data Scientist is A$120,000 per year
            based on 140 salaries
            </body></html>
            """,
            _location("AUD", "Australia", "Melbourne"),
            "AUD",
        ),
        (
            """
            <html><body>
            The average salary for a Data Scientist is ₹2,400,000 per year
            based on 120 salaries
            </body></html>
            """,
            _location("INR", "India", "Mumbai"),
            "INR",
        ),
        (
            """
            <html><body>
            The average salary for a Data Scientist is AED 420,000 per year
            based on 80 salaries
            </body></html>
            """,
            _location("AED", "United Arab Emirates", "Dubai"),
            "AED",
        ),
    ],
)
def test_parse_salary_page_detects_expected_currency(
    html: str,
    location: GlassdoorLocation,
    expected_currency: str,
) -> None:
    salary_data = _parse_salary_page(html, location)

    assert salary_data is not None
    assert salary_data.currency == expected_currency


def test_parse_salary_page_uses_location_currency_when_symbol_is_ambiguous() -> None:
    html = """
    <html><body>
    The average salary for a Data Scientist is $120,000 per year
    based on 140 salaries
    </body></html>
    """

    salary_data = _parse_salary_page(html, _location("AUD", "Australia", "Melbourne"))

    assert salary_data is not None
    assert salary_data.currency == "AUD"


def test_is_valid_salary_page_accepts_salary_content() -> None:
    html = """
    <html><head><title>Data Scientist Salaries in London</title></head><body>
    The average salary for a Data Scientist is £61,098 per year
    between £46,362 (25th percentile) and £82,518
    based on 5,488 salaries submitted anonymously to Glassdoor
    </body></html>
    """

    assert _is_valid_salary_page(html) is True


def test_is_valid_salary_page_rejects_http_200_challenge_page() -> None:
    html = """
    <html><head><title>Access denied</title></head><body>
    Please verify you are a human to continue.
    Complete the CAPTCHA challenge.
    </body></html>
    """

    assert _is_valid_salary_page(html) is False


def test_parse_salary_page_keeps_median_when_range_details_are_missing() -> None:
    html = """
    <html><body>
    The average salary for a Data Scientist is £61,098 per year
    </body></html>
    """

    salary_data = _parse_salary_page(html, _location("GBP", "United Kingdom", "London"))

    assert salary_data is not None
    assert salary_data.salary_median == 61098.0
    assert salary_data.salary_p25 is None
    assert salary_data.salary_p75 is None
    assert salary_data.salary_p90 is None
    assert salary_data.sample_size is None
