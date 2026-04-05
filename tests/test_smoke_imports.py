import importlib


def test_active_modules_import_cleanly() -> None:
    for module_name in (
        "src.main",
        "src.db",
        "src.dashboard_reader",
        "src.scrapers.fx_scraper",
        "src.scrapers.salary_scraper",
        "src.scrapers.qol_scraper",
        "src.scrapers.offline_salary_scraper",
    ):
        importlib.import_module(module_name)
