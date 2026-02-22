from bs4 import BeautifulSoup 
import requests
from rich.pretty import pprint


class NumbeoScraper:
    BASE_URL="https://www.numbeo.com/cost-of-living/in"
    def __init__(self, url_tail):
        self.soup = self.get_html(url_tail)
        self.CoLScraper = CoLScraper(self.soup)
        get_currency()

    def  get_html(self, url_tail)->BeautifulSoup:
        url = f'{self.BASE_URL}/{url_tail}'
        print("Getting:", url)
        req = requests.post(url)
        print(req)
        return BeautifulSoup(req.content, "html.parser")

    def get_currency(self):
        self.currency = self.soup.find(id="displayCurrency").find(selected="selected").text

    def scrape_costs(self):
        return self.CoLScraper.get_cost_table()





class CoLScraper:
    def __init__(self, soup):
        self.soup = soup

    def get_cost_table(self)->dict[str:list]:
        data = self.soup
        table = data.find("table",{"class":"data_wide_table"})
        category = {}
        item_list = []
        category_title=None

        for i,child in enumerate(table.find_all('tr')):
            if child.has_attr('class') and child['class']==['break_category']:
                assert category_title != None, "category_title cannot be None"
                category[category_title] = item_list
                # Reset accumulators
                category_title = None
                item_list = []
                continue
            if child.contents[1].name == "th":
                category_title = self.process_category_row(child)
            if child.contents[0].name == "td":
                item_list.append(self.process_item_row(child))
        return category

    def process_category_row(self,child):
        title= child.div.text
        return title
    
    def process_item_row(self, child):
        left_bar_child = child.find("span",{"class":"barTextLeft"})
        right_bar_child = child.find("span",{"class":"barTextLeft"})
        if left_bar_child and right_bar_child:
            left_bar_child = left_bar_child.text
            right_bar_child = right_bar_child.text
        # print(child)
        item = child.td.text
        price = child.span.text
        # limit precision to int
        # NOTE: some EU countries switch , and .
        # TODO: could be made faster by using regex
        price = price.split('.')[0].replace(",","")
        return (item,int(price))




class Country:
    def __init__(self,title,url_tail):
        self.title = title
        self.url_tail = url_tail
        self.currency = None
        self.create_scraper()
        self.get_currency()
        self.get_cost_table()
    
    def create_scraper(self):
        self.scraper = NumbeoScraper(self.url_tail)
    def get_currency(self):
        self.currency = self.scraper.get_currency()
    def get_cost_table(self):
        self.cost_table = self.scraper.scrape_costs()
        pprint(self.cost_table)
    def convert_to_dollar(self, price):
    # TODO: need to use logic to convert (and save?) in USD
        raise NotImplementedError




if __name__ == "__main__":
    country = Country('Boston','Boston')
    print(country.currency)
