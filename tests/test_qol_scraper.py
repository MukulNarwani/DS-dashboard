from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from db import Database
from qol_scraper import CoLScraper, NumbeoScraper


def _parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_get_cost_table_keeps_final_category() -> None:
    soup = _parse_html(
        """
        <table class="data_wide_table">
          <tr class="break_category"><td><div>Markets</div></td></tr>
          <tr><td>Milk</td><td><span>5.00</span></td></tr>
          <tr class="break_category"><td><div>Transport</div></td></tr>
          <tr><td>Monthly Pass</td><td><span>50.00</span></td></tr>
        </table>
        """
    )

    cost_table = CoLScraper(soup).get_cost_table()

    assert cost_table == {
        "Markets": [("Milk", 5, (None, None))],
        "Transport": [("Monthly Pass", 50, (None, None))],
    }


def test_get_cost_table_supports_original_break_then_header_shape() -> None:
    soup = _parse_html(
        """
        <table class="data_wide_table">
          <tr class="break_category"></tr>
          <tr><th><div>Markets</div></th></tr>
          <tr><td>Milk</td><td><span>5.00</span></td></tr>
          <tr class="break_category"></tr>
          <tr><th><div>Transport</div></th></tr>
          <tr><td>Monthly Pass</td><td><span>50.00</span></td></tr>
        </table>
        """
    )

    cost_table = CoLScraper(soup).get_cost_table()

    assert cost_table == {
        "Markets": [("Milk", 5, (None, None))],
        "Transport": [("Monthly Pass", 50, (None, None))],
    }


def test_get_cost_table_handles_whitespace_and_range_spans() -> None:
    soup = _parse_html(
        """
        <table class="data_wide_table">
          <tr class="break_category">
            <td>
              \n
              <div>Restaurants</div>
            </td>
          </tr>
          <tr>
            \n
            <td>Meal, Inexpensive Restaurant</td>
            <td>
              <span>22.00</span>
              <span class="barTextLeft">18.00</span>
              <span class="barTextRight">30.00</span>
            </td>
          </tr>
        </table>
        """
    )

    cost_table = CoLScraper(soup).get_cost_table()

    assert cost_table == {
        "Restaurants": [("Meal, Inexpensive Restaurant", 22, (18, 30))]
    }


def test_get_cost_table_tolerates_missing_range_spans() -> None:
    soup = _parse_html(
        """
        <table class="data_wide_table">
          <tr class="break_category"><td><div>Utilities</div></td></tr>
          <tr><td>Internet</td><td><span>40.00</span></td></tr>
        </table>
        """
    )

    cost_table = CoLScraper(soup).get_cost_table()

    assert cost_table == {"Utilities": [("Internet", 40, (None, None))]}


def test_numbeo_scraper_uses_get(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    class DummyResponse:
        content = (
            b"<html><select id='displayCurrency'>"
            b"<option selected='selected'>USD</option></select></html>"
        )

        def raise_for_status(self) -> None:
            return None

        def __repr__(self) -> str:
            return "<DummyResponse [200]>"

    def fake_get(url: str, timeout: int) -> DummyResponse:
        called["url"] = url
        called["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr("qol_scraper.requests.get", fake_get)

    scraper = NumbeoScraper("London")

    assert called == {
        "url": "https://www.numbeo.com/cost-of-living/in/London",
        "timeout": 15,
    }
    assert scraper.get_currency() == "USD"


def test_numbeo_scraper_raises_when_currency_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyResponse:
        content = b"<html><body>No selector here</body></html>"

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, timeout: int) -> DummyResponse:
        return DummyResponse()

    monkeypatch.setattr("qol_scraper.requests.get", fake_get)

    with pytest.raises(ValueError, match="Currency selector not found"):
        NumbeoScraper("London")


def test_database_initialize_is_cwd_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    database = Database(str(tmp_path / "qol.db")).initialize()

    with database._get_connection() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert "countries" in tables
    assert "cities" in tables
