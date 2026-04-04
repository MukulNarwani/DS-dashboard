import importlib


def test_active_modules_import_cleanly() -> None:
    for module_name in ("main", "fx", "db", "salary_scraper", "qol_scraper"):
        importlib.import_module(module_name)
