import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from src.db import Database
from src.scrapers.salary_scraper import (
    LOCATIONS,
    ROLES,
    GlassdoorLocation,
    GlassdoorSalaryRepository,
    SalaryPageData,
)

logger = logging.getLogger(__name__)
DEFAULT_INPUT_DIR = (
    Path(__file__).resolve().parent.parent / "glassdoor_offline_salaries"
)

SOURCE_URL_PATTERN = re.compile(r'"untranslatedUrl":"([^"]+)"')
FAQ_SAMPLE_SIZE_PATTERN = re.compile(
    r"based on ([\d,.]+[Kk]?) salaries submitted", re.IGNORECASE
)
GENERIC_SAMPLE_SIZE_PATTERN = re.compile(
    r"([\d,.]+[Kk]?) salaries submitted", re.IGNORECASE
)


@dataclass(frozen=True)
class OfflineSalaryRecord:
    scraped_date: str
    role_category: str
    location: GlassdoorLocation
    salary_data: SalaryPageData
    source_url: str
    file_path: Path


def _normalise_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _build_role_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for role_category, role_slug, _ in ROLES:
        aliases[_normalise_key(role_category.replace("_", " "))] = role_category
        aliases[_normalise_key(role_slug.replace("-", " "))] = role_category
    return aliases


def _build_location_aliases() -> dict[str, GlassdoorLocation]:
    locations_by_name = {location.display_name: location for location in LOCATIONS}
    aliases: dict[str, GlassdoorLocation] = {
        _normalise_key(location.display_name): location for location in LOCATIONS
    }
    manual_aliases = {
        "Boston, MA": "Boston",
        "Edinburgh, Scotland": "Edinburgh",
        "London, UK": "London",
        "Mumbai, India": "Mumbai",
        "New York, NY": "New York City",
        "San Francisco, CA": "San Francisco",
        "San Francisco, CA, United States": "San Francisco",
    }

    for alias, display_name in manual_aliases.items():
        aliases[_normalise_key(alias)] = locations_by_name[display_name]

    return aliases


ROLE_ALIASES = _build_role_aliases()
LOCATION_ALIASES = _build_location_aliases()


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    cleaned = re.sub(r"[^\d.]", "", str(value))
    if not cleaned:
        return None
    return float(cleaned)


def _parse_compact_int(value: str) -> int | None:
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None

    if cleaned.lower().endswith("k"):
        return int(float(cleaned[:-1]) * 1000)

    return int(float(cleaned))


def _load_json_ld_blocks(soup: BeautifulSoup) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.get_text(strip=True)
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue

        if isinstance(data, dict):
            blocks.append(data)
        elif isinstance(data, list):
            blocks.extend(item for item in data if isinstance(item, dict))

    return blocks


def _extract_sample_size_from_text(text: str) -> int | None:
    normalized_text = text.replace("\xa0", " ")
    match = FAQ_SAMPLE_SIZE_PATTERN.search(normalized_text)
    if not match:
        match = GENERIC_SAMPLE_SIZE_PATTERN.search(normalized_text)
    if not match:
        return None
    return _parse_compact_int(match.group(1))


def _extract_sample_size(
    soup: BeautifulSoup, faq_page: dict[str, Any] | None
) -> int | None:
    if faq_page:
        for entry in faq_page.get("mainEntity", []):
            answer = entry.get("acceptedAnswer", {})
            text = answer.get("text", "")
            sample_size = _extract_sample_size_from_text(text)
            if sample_size is not None:
                return sample_size

    hero = soup.find(attrs={"data-test": "hero-rich-text-redesigned"})
    if hero is not None:
        sample_size = _extract_sample_size_from_text(hero.get_text(" ", strip=True))
        if sample_size is not None:
            return sample_size

    return _extract_sample_size_from_text(soup.get_text(" ", strip=True))


def _extract_source_url(html: str, fallback_path: Path) -> str:
    match = SOURCE_URL_PATTERN.search(html)
    if not match:
        return str(fallback_path.resolve())
    return match.group(1)


def _map_role_category(role_name: str) -> str | None:
    return ROLE_ALIASES.get(_normalise_key(role_name))


def _map_location(raw_location_name: str) -> GlassdoorLocation | None:
    return LOCATION_ALIASES.get(_normalise_key(raw_location_name))


def parse_offline_salary_file(path: Path) -> OfflineSalaryRecord:
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    json_ld_blocks = _load_json_ld_blocks(soup)

    occupation = next(
        (block for block in json_ld_blocks if block.get("@type") == "Occupation"),
        None,
    )
    if occupation is None:
        raise ValueError("Occupation JSON-LD block not found")

    role_name = occupation.get("name")
    if not isinstance(role_name, str):
        raise ValueError("Role name missing from occupation metadata")

    role_category = _map_role_category(role_name)
    if role_category is None:
        raise ValueError(f"Unknown role name: {role_name}")

    raw_locations = occupation.get("occupationLocation") or []
    raw_location_name = None
    for location in raw_locations:
        if isinstance(location, dict) and isinstance(location.get("name"), str):
            raw_location_name = location["name"]
            break
    if raw_location_name is None:
        raise ValueError("Location name missing from occupation metadata")

    location = _map_location(raw_location_name)
    if location is None:
        raise ValueError(f"Unknown location name: {raw_location_name}")

    salary_distributions = occupation.get("estimatedSalary") or []
    salary_distribution = next(
        (item for item in salary_distributions if isinstance(item, dict)),
        None,
    )
    if salary_distribution is None:
        raise ValueError("Salary distribution missing from occupation metadata")

    salary_median = _coerce_float(salary_distribution.get("median"))
    if salary_median is None:
        raise ValueError("Median salary missing from occupation metadata")

    faq_page = next(
        (block for block in json_ld_blocks if block.get("@type") == "FAQPage"),
        None,
    )

    salary_data = SalaryPageData(
        salary_median=salary_median,
        salary_p25=_coerce_float(salary_distribution.get("percentile25")),
        salary_p75=_coerce_float(salary_distribution.get("percentile75")),
        salary_p90=_coerce_float(salary_distribution.get("percentile90")),
        sample_size=_extract_sample_size(soup, faq_page),
        currency=str(salary_distribution.get("currency") or location.currency),
    )

    scraped_date = date.fromtimestamp(path.stat().st_mtime).isoformat()
    source_url = _extract_source_url(html, path)

    return OfflineSalaryRecord(
        scraped_date=scraped_date,
        role_category=role_category,
        location=location,
        salary_data=salary_data,
        source_url=source_url,
        file_path=path,
    )


class OfflineGlassdoorSalaryRun:
    def __init__(self, db: Database, input_dir: str | Path = DEFAULT_INPUT_DIR):
        self.repo = GlassdoorSalaryRepository(db)
        self.input_dir = Path(input_dir)

    def run(self) -> tuple[int, int]:
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {self.input_dir}")

        imported = 0
        skipped = 0

        for path in sorted(self.input_dir.glob("*.html")):
            try:
                record = parse_offline_salary_file(path)
            except ValueError as exc:
                skipped += 1
                logger.warning("Skipping %s: %s", path.name, exc)
                continue

            self.repo.upsert(
                record.scraped_date,
                record.role_category,
                record.location,
                record.salary_data,
                record.source_url,
            )
            imported += 1
            logger.info(
                "Imported %s | %s from %s",
                record.role_category,
                record.location.display_name,
                path.name,
            )

        return imported, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import saved Glassdoor salary HTML files into salary_benchmarks."
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing saved Glassdoor salary HTML files.",
    )
    parser.add_argument(
        "--db-path",
        default="data.db",
        help="SQLite database path.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    database = Database(args.db_path).initialize()
    imported, skipped = OfflineGlassdoorSalaryRun(
        database, input_dir=args.input_dir
    ).run()
    logger.info("Offline import complete: imported=%s skipped=%s", imported, skipped)


if __name__ == "__main__":
    main()
