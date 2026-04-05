PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    iso_code TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country_id INTEGER NOT NULL,
    numbeo_slug TEXT UNIQUE,
    UNIQUE(id, country_id),
    UNIQUE(name, country_id),
    FOREIGN KEY (country_id) REFERENCES countries(id)
);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    UNIQUE(name, category_id),
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS cost_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    snapshot_date TEXT NOT NULL,
    price_avg REAL NOT NULL,
    price_min REAL,
    price_max REAL,
    currency TEXT NOT NULL,
    sample_size INTEGER,
    data_last_updated TIMESTAMP,
    UNIQUE(city_id, item_id, snapshot_date),
    FOREIGN KEY (city_id) REFERENCES cities(id),
    FOREIGN KEY (item_id) REFERENCES items(id)
);

CREATE VIEW IF NOT EXISTS latest_cost_observations AS
SELECT co.*
FROM cost_observations co
JOIN (
    SELECT city_id, item_id, MAX(snapshot_date) AS snapshot_date
    FROM cost_observations
    GROUP BY city_id, item_id
) latest
    ON latest.city_id = co.city_id
   AND latest.item_id = co.item_id
   AND latest.snapshot_date = co.snapshot_date;

CREATE TABLE IF NOT EXISTS fx_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    currency_from TEXT NOT NULL,
    rate_to_usd REAL NOT NULL,
    UNIQUE(date, currency_from)
);

CREATE TABLE IF NOT EXISTS salary_benchmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scraped_date TEXT NOT NULL,
    role_category TEXT NOT NULL,
    location_name TEXT NOT NULL,
    location_country TEXT NOT NULL,
    country_id INTEGER NOT NULL,
    city_id INTEGER,
    location_granularity TEXT NOT NULL
        CHECK (location_granularity IN ('city', 'country')),
    salary_median REAL,
    salary_p25 REAL,
    salary_p75 REAL,
    salary_p90 REAL,
    currency TEXT NOT NULL,
    sample_size INTEGER,
    source TEXT DEFAULT 'glassdoor',
    scraped_url TEXT,
    UNIQUE(
        scraped_date,
        role_category,
        location_name,
        location_country,
        location_granularity
    ),
    FOREIGN KEY (country_id) REFERENCES countries(id),
    FOREIGN KEY (city_id, country_id) REFERENCES cities(id, country_id),
    CHECK (
        (location_granularity = 'city' AND city_id IS NOT NULL)
        OR (location_granularity = 'country' AND city_id IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS ppp_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL,
    iso_code TEXT NOT NULL,
    year INTEGER NOT NULL,
    factor REAL NOT NULL,
    source TEXT DEFAULT 'world_bank',
    UNIQUE(iso_code, year)
);

CREATE INDEX IF NOT EXISTS idx_fx_rates_currency_date
    ON fx_rates(currency_from, date DESC);

CREATE INDEX IF NOT EXISTS idx_cost_observations_city_item_snapshot
    ON cost_observations(city_id, item_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_cost_observations_item_city_snapshot
    ON cost_observations(item_id, city_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_salary_benchmarks_role_city_date
    ON salary_benchmarks(role_category, city_id, scraped_date DESC);

CREATE INDEX IF NOT EXISTS idx_salary_benchmarks_role_country_date
    ON salary_benchmarks(role_category, country_id, scraped_date DESC);
