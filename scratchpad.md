I inspected the tracked source, ran the existing tests, and checked import/compile behavior. The current repo has a few definite blockers already.

Most important findings:
1. `main.py` does not compile. The function bodies are not indented, so Python raises `IndentationError` immediately at [main.py](/Users/mukulnarwani/Documents/Code/Dashboard/main.py#L1).
2. `fx.py` fails at import time because `Optional` is used in the return annotation but never imported. Anything importing `scrapper.py` breaks on import for that reason at [fx.py](/Users/mukulnarwani/Documents/Code/Dashboard/fx.py#L43).
3. The documented workflow is not currently executable: `ruff` is neither installed in the venv nor declared in `pyproject.toml`, so the required `ruff check` step cannot run. `pytest` works, but the lint gate is broken.
4. The cost-of-living scraper drops the last category it parses. In [scrapper.py](/Users/mukulnarwani/Documents/Code/Dashboard/scrapper.py#L35), categories are only saved when the next `break_category` row appears, so the final section is never written to `categories`.
5. `CoLScraper.get_cost_table()` is brittle against real Numbeo HTML and can crash on indexing assumptions like `child.contents[1]` / `child.contents[0]` in [scrapper.py](/Users/mukulnarwani/Documents/Code/Dashboard/scrapper.py#L43). Any extra whitespace/text node changes can raise `IndexError` or misclassify rows.
6. `NumbeoScraper.get_html()` uses `requests.post()` for a read-only page fetch in [scrapper.py](/Users/mukulnarwani/Documents/Code/Dashboard/scrapper.py#L17). That is very likely wrong for Numbeo page retrieval and increases the chance of getting unexpected responses or blocks.
7. `scrapper.py` contains two `convert_to_dollar` definitions on `City`; the first is dead code and silently overwritten by the second at [scrapper.py](/Users/mukulnarwani/Documents/Code/Dashboard/scrapper.py#L97). That is a logic smell and hides intended behavior.
8. `Database.initialize()` depends on the current working directory because it opens `"schema.sql"` by relative path in [db.py](/Users/mukulnarwani/Documents/Code/Dashboard/db.py#L17). Tests work only because they manually `chdir` to the repo root first; scripts launched elsewhere will fail to initialize the DB.
9. The salary pipeline currently has no integration with the city/country tables used by the rest of the dashboard schema. `salary_benchmarks.location_name/location_country` are raw strings, while job postings and cost-of-living data center around `cities.id` and `countries.id` in [schema.sql](/Users/mukulnarwani/Documents/Code/Dashboard/schema.sql). That will make joining salary, jobs, and living-cost data error-prone.
10. `classify_role()` falls back to a live Ollama HTTP call for any unmatched title in [classify.py](/Users/mukulnarwani/Documents/Code/Dashboard/classify.py#L19). There is no error handling, timeout recovery, or offline fallback beyond `"other"`, so classification can fail hard if Ollama is unavailable.

Checks I ran:
- `make test`: passes, 11 tests.
- `./.venv/bin/ruff check`: fails because Ruff is missing.
- `python -m py_compile ...`: fails on `main.py`.
- `python -c "import classify, db, salary_scraper, scrapper"`: fails because of `fx.py`.

One decision is still unclear before I turn this into a concrete remediation plan: do you want the next plan to focus on making the end-to-end dashboard data pipeline runnable, or on a broader cleanup of all the issues above?
