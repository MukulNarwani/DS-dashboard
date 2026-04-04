from typing import Optional

import requests
from bs4 import BeautifulSoup
from rich.pretty import pprint

from db import CostOfLivingRepository, Database
from fx import get_fx_rate


class NumbeoScraper:
    BASE_URL = "https://www.numbeo.com/cost-of-living/in"

    def __init__(self, url_tail: str):
        self.soup = self.get_html(url_tail)
        self.col_scraper = CoLScraper(self.soup)
        self.currency = self.get_currency()

    def get_html(self, url_tail: str) -> BeautifulSoup:
        url = f"{self.BASE_URL}/{url_tail}"
        print("Getting:", url)
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        print(response)
        return BeautifulSoup(response.content, "html.parser")

    def get_currency(self) -> str:
        currency_select = self.soup.find(id="displayCurrency")
        if currency_select is None:
            raise ValueError("Currency selector not found in Numbeo page")

        selected_option = currency_select.find(selected="selected")
        if selected_option is None:
            raise ValueError("Selected currency option not found in Numbeo page")

        return selected_option.get_text(strip=True)

    def scrape_costs(
        self,
    ) -> dict[str, list[tuple[str, int, tuple[Optional[int], Optional[int]]]]]:
        return self.col_scraper.get_cost_table()


class CoLScraper:
    def __init__(self, soup: BeautifulSoup):
        self.soup = soup

    def get_cost_table(
        self,
    ) -> dict[str, list[tuple[str, int, tuple[Optional[int], Optional[int]]]]]:
        table = self.soup.find("table", {"class": "data_wide_table"})
        if table is None:
            return {}

        categories: dict[
            str, list[tuple[str, int, tuple[Optional[int], Optional[int]]]]
        ] = {}
        item_list: list[tuple[str, int, tuple[Optional[int], Optional[int]]]] = []
        category_title: Optional[str] = None

        for row in table.find_all("tr"):
            if "break_category" in (row.get("class") or []):
                if category_title is not None:
                    categories[category_title] = item_list
                category_title = (
                    self.process_category_row(row)
                    if self.is_category_title_row(row)
                    else None
                )
                item_list = []
                continue

            if self.is_category_title_row(row):
                category_title = self.process_category_row(row)
                continue

            if category_title is None:
                continue

            item = self.process_item_row(row)
            if item is not None:
                item_list.append(item)

        if category_title is not None:
            categories[category_title] = item_list

        return categories

    def is_category_title_row(self, row) -> bool:
        return row.find("th") is not None or row.find("div") is not None

    def process_category_row(self, row) -> str:
        title_node = row.find("div") or row.find("th")
        if title_node is None:
            raise ValueError("Category row is missing title content")
        return title_node.get_text(strip=True)

    def convert_to_int(self, int_string: str) -> int:
        # TODO: could be made faster by using regex.
        # NOTE: some EU countries switch , and .
        tmp = int_string.replace("\n", "").replace(",", "").split(".")[0]
        return int(tmp)

    def process_item_row(
        self, row
    ) -> Optional[tuple[str, int, tuple[Optional[int], Optional[int]]]]:
        item_cell = row.find("td")
        if item_cell is None:
            return None

        spans = row.find_all("span")
        if not spans:
            return None

        price_node = next(
            (span for span in spans if not span.get("class")),
            spans[0],
        )
        left_bar = row.find("span", {"class": "barTextLeft"})
        right_bar = row.find("span", {"class": "barTextRight"})

        price_min = self.convert_to_int(left_bar.get_text()) if left_bar else None
        price_max = self.convert_to_int(right_bar.get_text()) if right_bar else None

        item = item_cell.get_text(strip=True)
        price = self.convert_to_int(price_node.get_text())
        return (item, price, (price_min, price_max))


class City:
    def __init__(self, country: str, city: str, url_tail: str, db: Optional[Database]):
        self.country = country
        self.city = city
        self.url_tail = url_tail
        self.currency: Optional[str] = None
        self.db = CostOfLivingRepository(db or Database().initialize())

        self.create_scraper()
        self.get_currency()
        self.get_cost_table()

    def create_scraper(self) -> None:
        self.scraper = NumbeoScraper(self.url_tail)

    def get_currency(self) -> None:
        self.currency = self.scraper.get_currency()

    def get_cost_table(self) -> None:
        self.cost_table = self.scraper.scrape_costs()

    def convert_to_dollar(self, price: float, currency: str) -> Optional[float]:
        rate = get_fx_rate(self.db.database, currency)
        return round(price * rate, 2) if rate else None

    def save(self) -> None:
        country_id, city_id = self.db.upsert_location(
            city_name=self.city,
            country_name=self.country,
            url_tail=self.url_tail,
        )
        for category, items in self.cost_table.items():
            for item_name, price, (price_min, price_max) in items:
                item_id = self.db.upsert_item(item_name, category)
                self.db.upsert_observation(
                    city_id=city_id,
                    item_id=item_id,
                    price_avg=price,
                    currency=self.currency,
                    price_min=price_min,
                    price_max=price_max,
                )

    def read(self) -> None:
        data = self.db.get_latest_city_data(self.city)
        grouped = {}
        for row in data:
            grouped.setdefault(row["category"], []).append(row)
        pprint(grouped.keys())


if __name__ == "__main__":
    db = Database().initialize()
    city = City("United Kingdom", "London", "London", db)
    print(city.currency)
    city.save()
    city.read()
