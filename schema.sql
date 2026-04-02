PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url_tail TEXT UNIQUE NOT NULL,
    iso_code TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country_id INTEGER NOT NULL,
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
    price_avg REAL NOT NULL,
    price_min REAL,
    price_max REAL,
    currency TEXT NOT NULL,
    sample_size INTEGER,
    -- scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_last_updated TIMESTAMP,
    UNIQUE(city_id, item_id ),
    FOREIGN KEY (city_id) REFERENCES cities(id),
    FOREIGN KEY (item_id) REFERENCES items(id)
);
-- JOB TABLES --
-- FX rates for normalisation
CREATE TABLE IF NOT EXISTS fx_rates (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT NOT NULL,
    currency_from TEXT NOT NULL,
    rate_to_usd   REAL NOT NULL,
    UNIQUE(date, currency_from)
);

-- Canonical role taxonomy
CREATE TABLE IF NOT EXISTS role_categories (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE  -- ml_engineer, data_scientist, etc.
);

-- Individual job postings from JobSpy
CREATE TABLE IF NOT EXISTS job_postings (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date        TEXT NOT NULL,          -- ISO date of collection run
    source               TEXT NOT NULL,          -- 'indeed'
    source_job_id        TEXT,                   -- platform's own ID
    raw_title            TEXT NOT NULL,
    role_category_id     INTEGER REFERENCES role_categories(id),
    company              TEXT,
    location_city        TEXT,
    location_country     TEXT,
    city_id              INTEGER REFERENCES cities(id),  -- FK to CoL cities where matched
    is_remote            INTEGER DEFAULT 0,
    salary_min_raw       REAL,
    salary_max_raw       REAL,
    salary_currency      TEXT,
    salary_interval      TEXT,                   -- hourly/weekly/monthly/yearly
    salary_source        TEXT,                   -- 'field' / 'description' / null
    date_posted          TEXT,
    job_url              TEXT,
    first_seen_date      TEXT NOT NULL,
    last_seen_date       TEXT NOT NULL,
    UNIQUE(source, source_job_id)                -- dedup anchor
);
-- Raw Glassdoor bootstrap salary benchmarks for the first dashboard version.
-- This table intentionally stores local-currency values and scraped metadata.
CREATE TABLE IF NOT EXISTS salary_benchmarks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    scraped_date     TEXT NOT NULL,
    role_category    TEXT NOT NULL,
    location_name    TEXT NOT NULL,
    location_country TEXT NOT NULL,
    salary_median    REAL,
    salary_p25       REAL,
    salary_p75       REAL,
    salary_p90       REAL,
    currency         TEXT NOT NULL,
    sample_size      INTEGER,
    source           TEXT DEFAULT 'glassdoor',
    scraped_url      TEXT,
    UNIQUE(scraped_date, role_category, location_name)
);
CREATE TABLE IF NOT EXISTS ppp_factors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL,
    iso_code     TEXT NOT NULL,
    year         INTEGER NOT NULL,
    factor       REAL NOT NULL,  -- local currency units per 1 PPP USD
    source       TEXT DEFAULT 'world_bank',
    UNIQUE(iso_code, year)
);
