from dataclasses import dataclass

from src.db import Database


@dataclass(frozen=True)
class CityRecord:
    id: int
    name: str
    country_id: int
    country_name: str
    iso_code: str | None
    numbeo_slug: str | None


@dataclass(frozen=True)
class CostObservationRecord:
    city_id: int
    city_name: str
    country_id: int
    country_name: str
    category: str
    item: str
    snapshot_date: str
    price_avg: float
    price_min: float | None
    price_max: float | None
    currency: str
    sample_size: int | None
    data_last_updated: str | None


@dataclass(frozen=True)
class SalaryBenchmarkRecord:
    id: int
    scraped_date: str
    role_category: str
    location_name: str
    location_country: str
    country_id: int
    city_id: int | None
    location_granularity: str
    salary_median: float | None
    salary_p25: float | None
    salary_p75: float | None
    salary_p90: float | None
    currency: str
    sample_size: int | None
    source: str | None
    scraped_url: str | None


@dataclass(frozen=True)
class PPPFactorRecord:
    country_id: int
    country_name: str
    iso_code: str
    year: int
    factor: float
    source: str | None


@dataclass(frozen=True)
class FXRateRecord:
    date: str
    currency_from: str
    rate_to_usd: float


class DashboardReader:
    def __init__(self, database: Database):
        self.database = database

    def get_city(
        self,
        city_name: str,
        country_name: str | None = None,
    ) -> CityRecord | None:
        query = """
            SELECT
                ci.id,
                ci.name,
                ci.country_id,
                country.name AS country_name,
                country.iso_code,
                ci.numbeo_slug
            FROM cities ci
            JOIN countries country ON country.id = ci.country_id
            WHERE ci.name = ?
        """
        params: tuple[str, ...]
        if country_name is None:
            params = (city_name,)
        else:
            query += " AND country.name = ?"
            params = (city_name, country_name)
        query += " ORDER BY country.name, ci.name"

        with self.database._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        if not rows:
            return None
        if country_name is None and len(rows) > 1:
            raise ValueError(
                f"Multiple cities named {city_name!r} found; provide country_name."
            )

        row = rows[0]
        return CityRecord(
            id=row["id"],
            name=row["name"],
            country_id=row["country_id"],
            country_name=row["country_name"],
            iso_code=row["iso_code"],
            numbeo_slug=row["numbeo_slug"],
        )

    def get_cost_snapshot(
        self,
        city_id: int,
        snapshot_date: str | None = None,
    ) -> list[CostObservationRecord]:
        source = (
            "latest_cost_observations" if snapshot_date is None else "cost_observations"
        )
        query = f"""
            SELECT
                co.city_id,
                ci.name AS city_name,
                country.id AS country_id,
                country.name AS country_name,
                cat.name AS category,
                it.name AS item,
                co.snapshot_date,
                co.price_avg,
                co.price_min,
                co.price_max,
                co.currency,
                co.sample_size,
                co.data_last_updated
            FROM {source} co
            JOIN cities ci ON ci.id = co.city_id
            JOIN countries country ON country.id = ci.country_id
            JOIN items it ON it.id = co.item_id
            JOIN categories cat ON cat.id = it.category_id
            WHERE co.city_id = ?
        """
        params: tuple[int, ...] | tuple[int, str]
        if snapshot_date is None:
            params = (city_id,)
        else:
            query += " AND co.snapshot_date = ?"
            params = (city_id, snapshot_date)
        query += " ORDER BY cat.name, it.name"

        with self.database._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            CostObservationRecord(
                city_id=row["city_id"],
                city_name=row["city_name"],
                country_id=row["country_id"],
                country_name=row["country_name"],
                category=row["category"],
                item=row["item"],
                snapshot_date=row["snapshot_date"],
                price_avg=row["price_avg"],
                price_min=row["price_min"],
                price_max=row["price_max"],
                currency=row["currency"],
                sample_size=row["sample_size"],
                data_last_updated=row["data_last_updated"],
            )
            for row in rows
        ]

    def get_salary_benchmark(
        self,
        role_category: str,
        *,
        city_id: int | None = None,
        country_id: int | None = None,
        scraped_date: str | None = None,
    ) -> SalaryBenchmarkRecord | None:
        has_city_scope = city_id is not None
        has_country_scope = country_id is not None
        if has_city_scope == has_country_scope:
            raise ValueError("Provide exactly one of city_id or country_id.")

        query = """
            SELECT
                id,
                scraped_date,
                role_category,
                location_name,
                location_country,
                country_id,
                city_id,
                location_granularity,
                salary_median,
                salary_p25,
                salary_p75,
                salary_p90,
                currency,
                sample_size,
                source,
                scraped_url
            FROM salary_benchmarks
            WHERE role_category = ?
        """
        params: list[object] = [role_category]

        if city_id is not None:
            query += " AND city_id = ? AND location_granularity = 'city'"
            params.append(city_id)
        else:
            query += (
                " AND country_id = ? AND city_id IS NULL"
                " AND location_granularity = 'country'"
            )
            params.append(country_id)

        if scraped_date is not None:
            query += " AND scraped_date = ?"
            params.append(scraped_date)

        query += " ORDER BY scraped_date DESC LIMIT 1"

        with self.database._get_connection() as conn:
            row = conn.execute(query, tuple(params)).fetchone()

        if row is None:
            return None

        return SalaryBenchmarkRecord(
            id=row["id"],
            scraped_date=row["scraped_date"],
            role_category=row["role_category"],
            location_name=row["location_name"],
            location_country=row["location_country"],
            country_id=row["country_id"],
            city_id=row["city_id"],
            location_granularity=row["location_granularity"],
            salary_median=row["salary_median"],
            salary_p25=row["salary_p25"],
            salary_p75=row["salary_p75"],
            salary_p90=row["salary_p90"],
            currency=row["currency"],
            sample_size=row["sample_size"],
            source=row["source"],
            scraped_url=row["scraped_url"],
        )

    def get_ppp_factor(
        self,
        country_id: int,
        *,
        year: int | None = None,
    ) -> PPPFactorRecord | None:
        query = """
            SELECT
                country.id AS country_id,
                country.name AS country_name,
                ppp.iso_code,
                ppp.year,
                ppp.factor,
                ppp.source
            FROM countries country
            JOIN ppp_factors ppp ON ppp.iso_code = country.iso_code
            WHERE country.id = ?
        """
        params: tuple[int, ...] | tuple[int, int]
        if year is None:
            params = (country_id,)
        else:
            query += " AND ppp.year = ?"
            params = (country_id, year)
        query += " ORDER BY ppp.year DESC LIMIT 1"

        with self.database._get_connection() as conn:
            row = conn.execute(query, params).fetchone()

        if row is None:
            return None

        return PPPFactorRecord(
            country_id=row["country_id"],
            country_name=row["country_name"],
            iso_code=row["iso_code"],
            year=row["year"],
            factor=row["factor"],
            source=row["source"],
        )

    def get_fx_rate(
        self,
        currency: str,
        *,
        as_of: str | None = None,
    ) -> FXRateRecord | None:
        query = """
            SELECT
                date,
                currency_from,
                rate_to_usd
            FROM fx_rates
            WHERE currency_from = ?
        """
        params: tuple[str, ...] | tuple[str, str]
        normalized_currency = currency.upper()
        if as_of is None:
            params = (normalized_currency,)
        else:
            query += " AND date <= ?"
            params = (normalized_currency, as_of)
        query += " ORDER BY date DESC LIMIT 1"

        with self.database._get_connection() as conn:
            row = conn.execute(query, params).fetchone()

        if row is None:
            return None

        return FXRateRecord(
            date=row["date"],
            currency_from=row["currency_from"],
            rate_to_usd=row["rate_to_usd"],
        )
