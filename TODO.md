# Current Work

- [x] Repair post-refactor imports and package wiring for the `src/` layout.
- [x] Update test/runtime path configuration and refactor tests to the new module paths.
- [x] Verify each repair slice with targeted checks, then run the full Ruff/test pipeline.
- [x] Create a branch, stage the validated changes, and run a reviewer subagent quality gate.
- [ ] Resolve reviewer findings, rerun the commit pipeline, and commit.
- [x] Add a read-side analytics reader for canonical city, cost snapshot, salary benchmark, PPP, and FX lookups.
- [x] Fix `main.py` indentation so the module compiles without changing placeholder behavior.
- [x] Fix `fx.py` typing/import issues so active pipeline modules import cleanly.
- [x] Rename `scrapper.py` to `qol_scraper.py` and update references.
- [x] Harden Numbeo cost-of-living scraping against whitespace, missing range spans, and final-category loss.
- [x] Make `Database.initialize()` resolve `schema.sql` independent of the current working directory.
- [x] Add or update tests for import smoke coverage, scraper parsing/fetch behavior, and cwd-independent DB initialization.
- [x] Verify with Ruff, tests, and `py_compile`.
- [ ] Deferred: design how bootstrap salary benchmark rows should map to canonical `cities` / `countries`.

# Offline Salary Bootstrap

- [x] Add `offline_salary_scraper.py` to import saved Glassdoor salary HTML from `glassdoor_offline_salaries`.
- [x] Parse structured page metadata, map saved pages to known roles and locations, and upsert into `salary_benchmarks`.
- [x] Add pytest coverage for offline parsing, alias mapping, and batch import behavior.
- [x] Verify the offline importer with Ruff, tests, and `py_compile`.

# SQL Optimization

- [ ] Regenerate the DB from a cleaned schema with no migration layer.
- [x] Keep `cost_observations` in long format with historical snapshots keyed by `(city_id, item_id, snapshot_date)`.
- [x] Make locations canonical and move source-specific identifiers like Numbeo slugs onto city-level source columns.
- [x] Add canonical `country_id`, `city_id`, and `location_granularity` to `salary_benchmarks` while retaining raw scraped location metadata.
- [x] Remove stale unused tables from the fresh schema, especially `job_postings` and `role_categories`.
- [x] Enable SQLite foreign key enforcement on every connection and add explicit indexes for FX lookup, latest observation reads, and salary benchmark lookups.
- [x] Rewrite latest-state reads and city comparison queries to use historical observations efficiently.
- [x] Add fresh-schema, constraint, query-shape, and latest-snapshot tests for the revised DB design.
