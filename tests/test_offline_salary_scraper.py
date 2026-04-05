import os
from pathlib import Path

import pytest

from src.db import Database
from src.scrapers.offline_salary_scraper import (
    OfflineGlassdoorSalaryRun,
    parse_offline_salary_file,
)


def _offline_salary_html(
    *,
    role_name: str = "Data Scientist",
    city_name: str = "London",
    currency: str = "GBP",
    median: int = 61096,
    percentile25: int = 46370,
    percentile75: int = 82493,
    percentile90: int = 111381,
    sample_size: str = "5508",
    source_url: str = "https://www.glassdoor.com/Salaries/london-data-scientist-salary-SRCH_IL.0,6_IM1035_KO7,21.htm",
    include_faq: bool = True,
) -> str:
    faq_block = ""
    if include_faq:
        faq_block = f"""
        <script type="application/ld+json">
        {{
          "@context": "https://schema.org",
          "@type": "FAQPage",
          "mainEntity": [{{
            "@type": "Question",
            "name": "How much does a {role_name} in {city_name} make?",
            "acceptedAnswer": {{
              "@type": "Answer",
              "text": "The average salary for a {role_name} is {currency} {median:,} per year. Salary estimates are based on {sample_size} salaries submitted anonymously to Glassdoor."
            }}
          }}]
        }}
        </script>
        """

    return f"""
    <html>
      <head>
        <title>Salary: {role_name} in {city_name} 2026 | Glassdoor</title>
      </head>
      <body>
        <script nonce="">
          window.gdGlobals = [{{"page":{{"untranslatedUrl":"{source_url}"}}}}];
        </script>
        <script type="application/ld+json">
        {{
          "@context": "https://schema.org/",
          "@type": "Occupation",
          "name": "{role_name}",
          "estimatedSalary": [{{
            "@type": "MonetaryAmountDistribution",
            "name": "base",
            "currency": "{currency}",
            "duration": "P1Y",
            "percentile25": {percentile25},
            "median": {median},
            "percentile75": {percentile75},
            "percentile90": {percentile90}
          }}],
          "occupationLocation": [{{"@type": "City", "name": "{city_name}"}}]
        }}
        </script>
        {faq_block}
        <div data-test="hero-rich-text-redesigned">
          About our data Very High Confidence Last updated 4 Apr 2026 {sample_size} Salaries submitted
        </div>
      </body>
    </html>
    """


def _write_html(path: Path, html: str, mtime: int) -> None:
    path.write_text(html, encoding="utf-8")
    os.utime(path, (mtime, mtime))


def test_parse_offline_salary_file_uses_structured_metadata(tmp_path: Path) -> None:
    html_path = tmp_path / "new-york.html"
    _write_html(
        html_path,
        _offline_salary_html(
            city_name="New York, NY",
            currency="USD",
            median=166940,
            percentile25=128639,
            percentile75=219374,
            percentile90=278178,
            sample_size="6348",
            source_url="https://www.glassdoor.com/Salaries/new-york-city-data-scientist-salary-SRCH_IL.0,13_IM716_KO14,28.htm",
        ),
        1712707200,
    )

    record = parse_offline_salary_file(html_path)

    assert record.scraped_date == "2024-04-10"
    assert record.role_category == "data_scientist"
    assert record.location.display_name == "New York City"
    assert record.location.country == "United States"
    assert record.salary_data == record.salary_data.__class__(
        salary_median=166940.0,
        salary_p25=128639.0,
        salary_p75=219374.0,
        salary_p90=278178.0,
        sample_size=6348,
        currency="USD",
    )
    assert (
        record.source_url
        == "https://www.glassdoor.com/Salaries/new-york-city-data-scientist-salary-SRCH_IL.0,13_IM716_KO14,28.htm"
    )


def test_parse_offline_salary_file_falls_back_to_hero_text_sample_size(
    tmp_path: Path,
) -> None:
    html_path = tmp_path / "london.html"
    _write_html(
        html_path,
        _offline_salary_html(include_faq=False, sample_size="5.5K"),
        1714780800,
    )

    record = parse_offline_salary_file(html_path)

    assert record.salary_data.sample_size == 5500


def test_parse_offline_salary_file_rejects_unknown_locations(tmp_path: Path) -> None:
    html_path = tmp_path / "unknown-city.html"
    _write_html(
        html_path,
        _offline_salary_html(city_name="Berlin"),
        1714780800,
    )

    with pytest.raises(ValueError, match="Unknown location name: Berlin"):
        parse_offline_salary_file(html_path)


def test_offline_run_imports_valid_files_and_skips_bad_ones(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    input_dir = tmp_path / "offline_html"
    input_dir.mkdir()
    _write_html(
        input_dir / "london.html",
        _offline_salary_html(city_name="London", currency="GBP", sample_size="5508"),
        1714780800,
    )
    _write_html(
        input_dir / "bad.html",
        _offline_salary_html(role_name="Product Manager"),
        1714780800,
    )

    database = Database(str(tmp_path / "salary.db")).initialize()
    imported, skipped = OfflineGlassdoorSalaryRun(database, input_dir=input_dir).run()

    assert (imported, skipped) == (1, 1)
    assert "Skipping bad.html" in caplog.text

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
                salary_median,
                currency,
                sample_size
            FROM salary_benchmarks
            """
        ).fetchone()

    assert row == {
        "scraped_date": "2024-05-04",
        "role_category": "data_scientist",
        "location_name": "London",
        "location_country": "United Kingdom",
        "country_id": 1,
        "city_id": 1,
        "location_granularity": "city",
        "salary_median": 61096.0,
        "currency": "GBP",
        "sample_size": 5508,
    }
