"""The consecutive (first) version of the scraper."""

from collections import OrderedDict

import requests
import bs4
import langdetect
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import data

productsLinks = set()


def collect_products_links(link):
    strainer = bs4.SoupStrainer(id='mfilter-content-container')
    soup = bs4.BeautifulSoup(requests.get(link).text, 'lxml', parse_only=strainer)
    pages_links = [link]
    pagination = soup.find('ul', class_='pagination')
    if pagination:
        pages_number = pagination.find_next('div', class_='pull-right results').contents[0][-2]
        if pages_number > 1:
            for i in range(int(pages_number) - 1):
                next_page = pagination.find_next('a', href=True)
                pages_links.append(next_page['href'])
    for page_link in pages_links:
        if page_link == link:
            product_page_soup = soup
        else:
            product_page_soup = bs4.BeautifulSoup(requests.get(page_link).text, 'lxml')
        products = product_page_soup.find_all('span', class_='p-name')
        for product in products:
            productsLinks.add(product.find_previous('a')['href'])


def examine_composition(product_link):
    product_soup = bs4.BeautifulSoup(requests.get(product_link).text, 'lxml')
    composition = product_soup.find('p', class_='ingr')
    if composition:
        composition = composition.text.lower()
        if langdetect.detect(composition) == 'en':
            for ingredient in data.LATIN_INGREDIENTS:
                if composition.find(ingredient):
                    break
            else:
                return get_product_data(product_soup)
        else:
            for ingredient in data.CYRILLIC_INGREDIENTS:
                if composition.find(ingredient):
                    break
            else:
                return get_product_data(product_soup)


def get_product_data(product_soup):
    product_data = OrderedDict()
    breadcrumbs = product_soup.find('ul', class_='list-unstyled breadcrumb-links')
    links = breadcrumbs.find_all('a', href=True)
    product_data['section'] = links[1].text
    product_data['kind'] = links[2].text
    if len(links) > 4:
        product_data['subkind'] = links[3].text
        product_data['title'] = links[4].text
    else:
        product_data['subkind'] = '-'
        product_data['title'] = links[3].text
    product_data['subtitle'] = product_soup.find('h2', class_='model-title-product').text
    product_data['volume'] = product_soup.find('span', class_='product_params').text
    product_data['prise'] = int(product_soup.find('span', class_='price-new').text[:-6])
    product_data['prise/volume'] = round(product_data['prise'] / int(product_data['volume'][:-2]), 2)
    product_data['link'] = temp_product_link
    old_prise = product_soup.find('span', class_='price-old')
    if old_prise:
        product_data['old prise'] = int(old_prise.text[:-6])
        product_data['discount lasts'] = product_soup.find('span', class_='special-countdown').text
    return product_data


def log_in_and_add_to_table():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(credentials)
    table = client.open('sifo.ru').sheet1
    row = 2
    for product_link in productsLinks:
        global temp_product_link
        temp_product_link = product_link
        product_data = examine_composition(product_link)
        if product_data:
            table.insert_row(product_data.values(), row)
            row += 1


if __name__ == '__main__':
    for link in data.LINKS:
        collect_products_links(link)
    log_in_and_add_to_table()
