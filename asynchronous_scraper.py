"""The asynchronous version of the scraper. Was built on the consecutive one."""

import asyncio
from collections import OrderedDict
import random
import re
import time

import aiohttp
import bs4
import gspread
import langdetect
from oauth2client.service_account import ServiceAccountCredentials

import data  # Contains three lists: start links, ingredients in English and ingredients in Russian


async def fetch(session, link, lists_links, products_links):
    """Make a request and save link to the queue/loop if it is a 5xx error."""
    delay = random.randint(0, 30)
    await asyncio.sleep(delay)

    async with session.get(link) as response:
        status = str(response.status)
        print(status)

        if status.startswith('2') or status.startswith('3'):
            html = await response.text()
            return bs4.BeautifulSoup(html, 'lxml')

        elif status.startswith('5'):
            url = response.url
            if url.count('/') > 3:  # Then this is a link to a category page.
                loop = asyncio.get_running_loop()
                loop.create_task(collect_pagination_links(session, url, lists_links, products_links))
            # Then this is a link to a single product.
            elif url.count('/') == 3:
                await products_links.put(url)

        elif status.startswith('4'):
            print('A problem with the url:', url)


async def collect_pagination_links(session, start_link, lists_links, products_links):
    """Put to the queue all the links to pages from products lists."""
    await lists_links.put(start_link)
    soup = await fetch(session, start_link, lists_links, products_links)
    pagination = soup.find('ul', class_='pagination')
    if pagination:
        pagination_links = [a['href'] for a in pagination.find_all('a', href=True)[:-2]]
        for link in pagination_links:
            await lists_links.put(link)


async def collect_products_links(session, lists_links, products_links):
    """Put every link to a products to the queue."""
    while True:
        link = await lists_links.get()
        soup = await fetch(session, link, lists_links, products_links)
        target_links = [a['href'] for a in soup.find_all('a', class_='main-thumb')]
        for link in target_links:
            await products_links.put(link)
        lists_links.task_done()


async def check_product(session, lists_links, products_links, table):
    """Take every link to a product from the queue and check it."""
    while True:
        product_link = await products_links.get()
        product_soup = await fetch(session, product_link, lists_links, products_links)
        composition = product_soup.find('p', class_='ingr')
        if composition:
            composition = composition.text.lower()
            try:
                lang = langdetect.detect(composition)
            except langdetect.lang_detect_exception.LangDetectException:
                products_links.task_done()
                continue
            else:
                ok = await exemine_composition(composition, lang)
                if ok:
                    await save_product_data(product_soup, product_link, table)
        products_links.task_done()


async def exemine_composition(composition, lang):
    """Look for any compliance with a stop list."""
    if lang == 'en':
        for ingredient in data.LATIN_INGREDIENTS:
            if ingredient in composition:
                return False
        return True
    else:
        for ingredient in data.CYRILLIC_INGREDIENTS:
            if ingredient in composition:
                return False
        return True


async def save_product_data(product_soup, product_link, table):
    """Gather data of every product with keeping the table columns order."""
    # Selectors might have been changed.
    # The data on pages varies.
    product = OrderedDict()
    links = product_soup.find('ol', class_='breadcrumbs list-inline').find_all('a', href=True)
    product['section'] = links[1].text.strip()
    product['kind'] = links[2].text.strip() if len(links) >= 3 else '-'
    product['subkind'] = links[3].text.strip() if len(links) >= 4 else '-'
    product['title'] = product_soup.find('h1', itemprop='name').text
    product['subtitle'] = product_soup.find('h2', class_='model').text

    volume = product_soup.find('li', string=re.compile(r'Объем:\s\d+'))
    if volume:
        try:
            product['volume'] = int(volume.text.split()[1])
        except ValueError:
            product['volume'] = 0
    else:
        product['volume'] = 0

    price = product_soup.find('span', class_='price-new')
    if price:
        try:
            product['prise'] = int(price.text[:-5])
        except ValueError:
            product['prise'] = 0
    else:
        product['prise'] = 0

    try:
        product['prise/volume'] = round(product['prise'] / product['volume'], 2)
    except ZeroDivisionError:
        product['prise/volume'] = 0
    product['link'] = product_link

    # Write it to the table.
    table.insert_row(list(product.values()), 2)


async def main():
    lists_links = asyncio.Queue()
    products_links = asyncio.Queue()

    headers = {'User-Agent': ''}  # Put your data here.

    # Log in in Google Sheets.
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(credentials)
    table = client.open('sifo.ru').sheet1

    async with aiohttp.ClientSession(headers=headers) as session:
        paginations = [asyncio.create_task(collect_pagination_links(session, link, lists_links, products_links)) for link in data.LINKS]
        await asyncio.gather(*paginations, return_exceptions=True)

        producer = asyncio.create_task(collect_products_links(session, lists_links, products_links))
        consumers = [check_product(session, lists_links, products_links, table) for _ in range(3)]
        await asyncio.gather(producer, *consumers, return_exceptions=True)

        await lists_links.join()
        producer.cancel()

        await products_links.join()
        for consumer in consumers:
            consumer.cancel()


asyncio.run(main())
