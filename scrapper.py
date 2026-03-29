from bs4 import BeautifulSoup 
import requests
from rich.pretty import pprint
from db import *
from itertools import groupby

class NumbeoScraper:
    BASE_URL="https://www.numbeo.com/cost-of-living/in"
    def __init__(self, url_tail):
        self.soup = self.get_html(url_tail)
        self.CoLScraper = CoLScraper(self.soup)
        self.currency = self.get_currency()
        # print(self.soup)

    def  get_html(self, url_tail)->BeautifulSoup:
        url = f'{self.BASE_URL}/{url_tail}'
        print("Getting:", url)
        req = requests.post(url)
        print(req)
        return BeautifulSoup(req.content, "html.parser")

    def get_currency(self):
        return self.soup.find(id="displayCurrency").find(selected="selected").text

    def scrape_costs(self):
        return self.CoLScraper.get_cost_table()





class CoLScraper:
    def __init__(self, soup):
        self.soup = soup

    def get_cost_table(self)->dict[str:list]:
        data = self.soup
        table = data.find("table",{"class":"data_wide_table"})
        categories = {}
        item_list = []
        category_title=None

        for i,child in enumerate(table.find_all('tr')):
            if child.has_attr('class') and child['class']==['break_category']:
                assert category_title != None, "category_title cannot be None"
                categories[category_title] = item_list
                # Reset accumulators
                category_title = None
                item_list = []
                continue
            if child.contents[1].name == "th":
                category_title = self.process_category_row(child)
            if child.contents[0].name == "td":
                item_list.append(self.process_item_row(child))
        return categories

    def process_category_row(self,child):
        title= child.div.text
        return title

    def convert_to_int(self, int_string):
        # rm newline char and limit precision to int
        # TODO: could be made faster by using regex
        # NOTE: some EU countries switch , and .
        tmp = int_string.replace("\n","").replace(",","").split('.')[0]
        return int(tmp)

    def process_item_row(self, child):
        left_bar_child = child.find("span",{"class":"barTextLeft"})
        right_bar_child = child.find("span",{"class":"barTextRight"})

        if left_bar_child and right_bar_child:
            left_bar_child = self.convert_to_int(left_bar_child.text)
            right_bar_child = self.convert_to_int(right_bar_child.text)

        item = child.td.text
        price = self.convert_to_int(child.span.text)
        return (item,price, (left_bar_child,right_bar_child))




class City:
    def __init__(self,country,city,url_tail,db):
        self.country = country
        self.city = city
        self.url_tail = url_tail
        self.currency = None
        self.db = db
        self.create_scraper()
        self.get_currency()
        self.get_cost_table()
    
    def create_scraper(self):
        self.scraper = NumbeoScraper(self.url_tail)
    def get_currency(self):
        self.currency = self.scraper.get_currency()
    def get_cost_table(self):
        self.cost_table = self.scraper.scrape_costs()
    def convert_to_dollar(self, price):
    # TODO: need to use logic to convert (and save?) in USD
        raise NotImplementedError

    def save(self):
        # Save location
        # Save item
        country_id, city_id = self.db.upsert_location(
        city_name=self.city,
        country_name=self.country,  # update if you track country separately
        url_tail=self.url_tail
        )
        for category, items in self.cost_table.items():
            for (item_name, price, (price_min, price_max)) in items:
                item_id = self.db.upsert_item(item_name, category)
                self.db.upsert_observation(
                    city_id=city_id,
                    item_id=item_id,
                    price_avg=price,
                    currency=self.currency,
                    price_min=price_min,
                    price_max=price_max
                )
    def read(self):
        data = self.db.get_latest_city_data(self.city)
        grouped = {}
        for row in data:
            grouped.setdefault(row["category"], []).append(row)
        pprint(grouped.keys())
        

        






if __name__ == "__main__":
    db = CostOfLivingRepository(Database().initialize())
    city = City('United Kingdom','London','London',db)
    print(city.currency)
    city.save()
    city.read()
