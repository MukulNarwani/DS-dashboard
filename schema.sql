PRAGMA journal_mode=WAL;
CREATE TABLE countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    iso_code TEXT UNIQUE
);
CREATE TABLE cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country_id INTEGER NOT NULL,
    UNIQUE(name, country_id),
    FOREIGN KEY (country_id) REFERENCES countries(id)
);
CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    UNIQUE(name, category_id),
    FOREIGN KEY (category_id) REFERENCES categories(id)
);
CREATE TABLE cost_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    price_avg REAL NOT NULL,
    price_min REAL,
    price_max REAL,
    currency TEXT NOT NULL,
    sample_size INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_last_updated TIMESTAMP,
    UNIQUE(city_id, item_id, scraped_at),
    FOREIGN KEY (city_id) REFERENCES cities(id),
    FOREIGN KEY (item_id) REFERENCES items(id)
);
