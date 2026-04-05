import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import date

import requests
from bs4 import BeautifulSoup

from db import Database

logger = logging.getLogger(__name__)

# This scraper is intentionally bootstrap-oriented and brittle for now.
# It exists to seed the first dashboard salary view, not to be a durable
# long-term ingestion path. URL structure and regexes should be revisited soon.


@dataclass(frozen=True)
class GlassdoorLocation:
    display_name: str
    country: str
    currency: str
    subdomain: str
    city_slug: str
    il_param: str
    city_slug_len: int


@dataclass(frozen=True)
class SalaryPageData:
    salary_median: float
    salary_p25: float | None
    salary_p75: float | None
    salary_p90: float | None
    sample_size: int | None
    currency: str


LOCATIONS: list[GlassdoorLocation] = [
    GlassdoorLocation("United States", "United States", "USD", "com", "", "", 0),
    GlassdoorLocation(
        "San Francisco",
        "United States",
        "USD",
        "com",
        "san-francisco-",
        "IL.0,13_IM759",
        14,
    ),
    GlassdoorLocation(
        "New York City",
        "United States",
        "USD",
        "com",
        "new-york-city-",
        "IL.0,13_IM716",
        14,
    ),
    GlassdoorLocation(
        "Boston", "United States", "USD", "com", "boston-", "IL.0,6_IM49", 7
    ),
    GlassdoorLocation("United Kingdom", "United Kingdom", "GBP", "co.uk", "", "", 0),
    GlassdoorLocation(
        "London", "United Kingdom", "GBP", "co.uk", "london-", "IL.0,6_IM1035", 7
    ),
    GlassdoorLocation(
        "Edinburgh", "United Kingdom", "GBP", "co.uk", "edinburgh-", "IL.0,9_IM1091", 10
    ),
    GlassdoorLocation(
        "Melbourne", "Australia", "AUD", "com.au", "melbourne-", "IL.0,9_IM1139", 10
    ),
    GlassdoorLocation(
        "Dubai", "United Arab Emirates", "AED", "com", "dubai-", "IL.0,5_IM1520", 6
    ),
    GlassdoorLocation("Mumbai", "India", "INR", "co.in", "mumbai-", "IL.0,6_IM1100", 7),
]

ROLES: list[tuple[str, str, int]] = [
    ("data_scientist", "data-scientist", 14),
    ("ml_engineer", "machine-learning-engineer", 26),
    ("ai_engineer", "ai-engineer", 11),
    ("data_analyst", "data-analyst", 13),
    ("data_engineer", "data-engineer", 13),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

INVALID_PAGE_MARKERS = (
    "captcha",
    "access denied",
    "verify you are a human",
    "unusual traffic",
    "sign in to glassdoor",
    "join glassdoor",
    "are you a robot",
)

VALID_PAGE_MARKERS = (
    "average salary",
    "25th percentile",
    "90th percentile",
    "based on",
    "per year",
)


SALARY_VALUE_PATTERN = r"(?:A\$|AED\s+|AUD\s+|[\$£€₹])?[\d,]+"


def _build_url(loc: GlassdoorLocation, role_slug: str, role_slug_len: int) -> str:
    """Construct the Glassdoor salary URL for a given location and role."""
    base = f"https://www.glassdoor.{loc.subdomain}/Salaries"

    if loc.city_slug:
        full_slug = f"{loc.city_slug}{role_slug}"
        ko_start = loc.city_slug_len
        ko_end = loc.city_slug_len + role_slug_len
        return (
            f"{base}/{full_slug}-salary-SRCH_{loc.il_param}_KO{ko_start},{ko_end}.htm"
        )

    return f"{base}/{role_slug}-salary-SRCH_KO0,{role_slug_len}.htm"


def _parse_currency_amount(text: str) -> float | None:
    """Extract a numeric salary value from strings like '£61,098' or 'AED 154,515'."""
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _detect_currency(text: str, location: GlassdoorLocation) -> str:
    normalized_text = text.upper()
    currency_patterns = {
        "AED": (r"\bAED\b",),
        "AUD": (r"A\$", r"\bAUD\b"),
        "GBP": (r"£", r"\bGBP\b"),
        "EUR": (r"€", r"\bEUR\b"),
        "INR": (r"₹", r"\bINR\b"),
        "USD": (r"\bUSD\b",),
    }

    for currency, patterns in currency_patterns.items():
        if any(re.search(pattern, normalized_text) for pattern in patterns):
            return currency

    return location.currency


def _extract_page_text(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = soup.get_text(" ", strip=True)
    return title, text


def _is_valid_salary_page(html: str) -> bool:
    title, text = _extract_page_text(html)
    normalized = f"{title} {text}".lower()

    if any(marker in normalized for marker in INVALID_PAGE_MARKERS):
        return False

    marker_count = sum(1 for marker in VALID_PAGE_MARKERS if marker in normalized)
    has_salary_number = bool(
        re.search(
            r"(?:[\$£€₹]\s?[\d,]+|\bAED\s+[\d,]+|\bAUD\s+[\d,]+)", text, re.IGNORECASE
        )
    )
    return marker_count >= 2 and has_salary_number


def _parse_salary_page(html: str, location: GlassdoorLocation) -> SalaryPageData | None:
    """
    Parse salary details from a Glassdoor salary page.

    This parser is intentionally narrow because the file is a bootstrap scraper.
    """
    _, text = _extract_page_text(html)

    median_match = re.search(
        rf"average salary.*?(?:is|are)\s+({SALARY_VALUE_PATTERN})\s+per year",
        text,
        re.IGNORECASE,
    )
    range_match = re.search(
        rf"between\s+({SALARY_VALUE_PATTERN})\s+\(25th percentile\)\s+and\s+({SALARY_VALUE_PATTERN})",
        text,
        re.IGNORECASE,
    )
    p90_match = re.search(
        rf"up to\s+({SALARY_VALUE_PATTERN})\s+\(90th percentile\)",
        text,
        re.IGNORECASE,
    )
    sample_match = re.search(r"based on ([\d,]+) salar", text, re.IGNORECASE)

    if not median_match:
        return None

    salary_median = _parse_currency_amount(median_match.group(1))
    if salary_median is None:
        return None

    return SalaryPageData(
        salary_median=salary_median,
        salary_p25=_parse_currency_amount(range_match.group(1))
        if range_match
        else None,
        salary_p75=_parse_currency_amount(range_match.group(2))
        if range_match
        else None,
        salary_p90=_parse_currency_amount(p90_match.group(1)) if p90_match else None,
        sample_size=int(sample_match.group(1).replace(",", ""))
        if sample_match
        else None,
        currency=_detect_currency(text, location),
    )


class GlassdoorSalaryRepository:
    """Storage wrapper for bootstrap salary benchmark rows defined in schema.sql."""

    def __init__(self, database: Database):
        self.database = database

    def _resolve_location_ids(
        self, conn, location: GlassdoorLocation
    ) -> tuple[int, int | None, str]:
        conn.execute(
            """
            INSERT INTO countries (name)
            VALUES (?)
            ON CONFLICT(name) DO NOTHING
            """,
            (location.country,),
        )
        country_id = conn.execute(
            "SELECT id FROM countries WHERE name = ?",
            (location.country,),
        ).fetchone()["id"]

        is_city_level = location.display_name != location.country or bool(
            location.city_slug or location.il_param
        )
        if not is_city_level:
            return country_id, None, "country"

        conn.execute(
            """
            INSERT INTO cities (name, country_id)
            VALUES (?, ?)
            ON CONFLICT(name, country_id) DO NOTHING
            """,
            (location.display_name, country_id),
        )
        city_id = conn.execute(
            "SELECT id FROM cities WHERE name = ? AND country_id = ?",
            (location.display_name, country_id),
        ).fetchone()["id"]
        return country_id, city_id, "city"

    def upsert(
        self,
        scraped_date: str,
        role_category: str,
        location: GlassdoorLocation,
        salary_data: SalaryPageData,
        url: str,
    ) -> None:
        with self.database._get_connection() as conn:
            country_id, city_id, location_granularity = self._resolve_location_ids(
                conn, location
            )
            conn.execute(
                """
                INSERT INTO salary_benchmarks (
                    scraped_date, role_category, location_name, location_country,
                    country_id, city_id, location_granularity,
                    salary_median, salary_p25, salary_p75, salary_p90,
                    currency, sample_size, source, scraped_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    scraped_date, role_category, location_name, location_country, location_granularity
                ) DO UPDATE SET
                    location_country = excluded.location_country,
                    country_id = excluded.country_id,
                    city_id = excluded.city_id,
                    location_granularity = excluded.location_granularity,
                    salary_median = excluded.salary_median,
                    salary_p25 = excluded.salary_p25,
                    salary_p75 = excluded.salary_p75,
                    salary_p90 = excluded.salary_p90,
                    currency = excluded.currency,
                    sample_size = excluded.sample_size,
                    source = excluded.source,
                    scraped_url = excluded.scraped_url
                """,
                (
                    scraped_date,
                    role_category,
                    location.display_name,
                    location.country,
                    country_id,
                    city_id,
                    location_granularity,
                    salary_data.salary_median,
                    salary_data.salary_p25,
                    salary_data.salary_p75,
                    salary_data.salary_p90,
                    salary_data.currency,
                    salary_data.sample_size,
                    "glassdoor",
                    url,
                ),
            )


class GlassdoorSalaryRun:
    """
    Bootstrap collection run across the hardcoded role and location matrix.

    This is acceptable for initial dashboard seeding, but it should be replaced
    with a more resilient ingestion path soon.
    """

    def __init__(self, db: Database, delay_range: tuple[float, float] = (4.0, 9.0)):
        self.repo = GlassdoorSalaryRepository(db)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.scraped_date = date.today().isoformat()
        self.delay_range = delay_range

    def run(self) -> None:
        total = len(ROLES) * len(LOCATIONS)
        done = 0

        for role_category, role_slug, role_slug_len in ROLES:
            for location in LOCATIONS:
                url = _build_url(location, role_slug, role_slug_len)
                done += 1
                logger.info(
                    "[%s/%s] %s | %s -> %s",
                    done,
                    total,
                    role_category,
                    location.display_name,
                    url,
                )

                try:
                    response = self.session.get(url, timeout=15)
                    if response.status_code == 403:
                        logger.warning("Blocked by Glassdoor (403): %s", url)
                        continue
                    if response.status_code != 200:
                        logger.warning(
                            "Unexpected HTTP status %s for %s",
                            response.status_code,
                            url,
                        )
                        continue
                    if not _is_valid_salary_page(response.text):
                        logger.warning("HTTP 200 but not a valid salary page: %s", url)
                        continue

                    salary_data = _parse_salary_page(response.text, location)
                    if salary_data is None:
                        logger.warning(
                            "Valid salary page but no parseable salary data: %s", url
                        )
                        continue

                    self.repo.upsert(
                        self.scraped_date, role_category, location, salary_data, url
                    )
                    logger.info(
                        "Stored salary benchmark: currency=%s median=%.0f p25=%s p75=%s n=%s",
                        salary_data.currency,
                        salary_data.salary_median,
                        salary_data.salary_p25,
                        salary_data.salary_p75,
                        salary_data.sample_size,
                    )
                except requests.RequestException as exc:
                    logger.error("Request error for %s: %s", url, exc)

                time.sleep(random.uniform(*self.delay_range))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database = Database().initialize()
    GlassdoorSalaryRun(database).run()
