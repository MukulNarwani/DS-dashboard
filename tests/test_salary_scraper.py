import sqlite3
from pathlib import Path

import pytest

from db import Database
from salary_scraper import (
    GlassdoorLocation,
    GlassdoorSalaryRepository,
    SalaryPageData,
    _is_valid_salary_page,
    _parse_salary_page,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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


def test_repository_upsert_uses_initialized_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(_repo_root())
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

    repository.upsert("2026-04-02", "data_scientist", location, salary_data, "https://example.com")

    with database._get_connection() as connection:
        row = connection.execute(
            """
            SELECT scraped_date, role_category, location_name, location_country, currency, salary_median
            FROM salary_benchmarks
            """
        ).fetchone()

    assert row == {
        "scraped_date": "2026-04-02",
        "role_category": "data_scientist",
        "location_name": "London",
        "location_country": "United Kingdom",
        "currency": "GBP",
        "salary_median": 61098.0,
    }


def test_schema_requires_scraped_date_for_raw_benchmarks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(_repo_root())
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
