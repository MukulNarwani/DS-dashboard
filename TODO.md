# Current Work

- [x] Fix `main.py` indentation so the module compiles without changing placeholder behavior.
- [x] Fix `fx.py` typing/import issues so active pipeline modules import cleanly.
- [x] Rename `scrapper.py` to `qol_scraper.py` and update references.
- [x] Harden Numbeo cost-of-living scraping against whitespace, missing range spans, and final-category loss.
- [x] Make `Database.initialize()` resolve `schema.sql` independent of the current working directory.
- [x] Add or update tests for import smoke coverage, scraper parsing/fetch behavior, and cwd-independent DB initialization.
- [x] Verify with Ruff, tests, and `py_compile`.
- [ ] Deferred: design how bootstrap salary benchmark rows should map to canonical `cities` / `countries`.
