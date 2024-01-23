import requests
import asyncio
import scrapy
import lxml
import re
import schedule
import time

from playwright.async_api import async_playwright
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from bs4 import BeautifulSoup

import pandas as pd

API_KEY = 'd507f5285e-5834de8469-bd6d9e795a'

def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])



def proxys():
	response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
	data = response.json()
	result = []
	for item in data['list'].values():
		result.append({
			'server' : f'{item["ip"]}:{item["port"]}',
			'username' : item['user'],
			'password' : item['pass']
		})
	return result

async def _cookie(browser, proxy):
    context = await browser.new_context(
        proxy=proxy
    )
    url='https://www.auchan.ru'
    page = await context.new_page()
    while True:
        try:
            async with page.expect_request("https://www.auchan.ru/v1/animations/settings?prev_sort=0"):
                await page.goto(url=url)
        except:
            print ('fail, go trying this')
            continue

        else:
            break
    return await context.cookies(), f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"

async def main():
    list_proxy = proxys()
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        tasks = [
            asyncio.create_task(_cookie(browser=browser, proxy=proxy)) for proxy in list_proxy
        ]
        return await asyncio.gather(*tasks)
    
def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

def get_pairs():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(main())
              


class My(scrapy.Spider):
    name = 'result'
    custom_settings = {
        #'COOKIES_DEBUG' : True,
		'COOKIES_ENABLED' : True,
        'USER_AGENT' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'CONCURRENT_REQUESTS_PER_DOMAIN' :10,
        'CONCURRENT_REQUESTS' : 13,
        'AUTOTHROTTLE_START_DELAY' : 4,
        'AUTOTHROTTLE_DEBUG' : True,
        'AUTOTHROTTLE_TARGET_CONCURRENCY' : 9,
        'LOG_FILE' : 'auchan_test.log',
        'LOG_FILE_APPEND' : False,
        'DOWNLOADER_MIDDLEWARES' : {
            'testmiddleware.test' : 0,
        },
        "FEEDS" : {
            'auchan_test.jsonl' : {
                'format' : 'jsonlines',
                'overwrite' : True,
                'encoding' : 'utf-8'
            }
        }
    }
    def start_requests(self):
        p = pd.read_excel('AUCHAN/Ashan_pervy-posledniy.xlsx', sheet_name=None)
        names = list(p.keys())
        DataFrames = list(p.values())
        idx = 0
        for name, df in zip(names, DataFrames):
            df = df.to_dict('list')
            catalogs_one = df['Подкаталог 1']
            catalogs_two = df['Подкаталог 2']
            prefixs = df['Префикс']
            urls = df['Ссылка на товар']
            for url, pref, cat1, cat2 in zip(urls, prefixs, catalogs_one, catalogs_two):
                yield scrapy.Request(
                    url=url,
                    cb_kwargs=dict(
                        catalog_one=cat1,
                        catalog_two=cat2,
                        prefix=pref,
                        name=name.upper(),
                        idx=idx
                    ),
                    dont_filter=True
                )
                idx = idx + 1


    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find('a', class_='css-dsyb4t')
        if brand != None:
            brand = brand.text.strip()
        else:
            brand = "Не указан"
        price = soup.find('div', class_='css-1rwzh68')
        if price != None :
            price = re.sub(r',', '.', re.sub(r'[^0-9,]', '', price.text.strip()))
        else:
            price = "Нет в наличии"
        old_price = soup.find('div', class_='css-1he77cg')
        count = soup.find('span', class_='inStockData')
        if count != None:
            count = re.sub(r'\D', '', count.text.strip())
        else:
            count = 0
        sale = soup.find('span', class_='css-acbihw')
        if sale != None:
            if price != "Нет в наличии":
                old_price = format(float(float(price) * 2 * (1 + int(sale.text.strip()) / 100)),'.2f').replace('.', ',')
            else:
                old_price = None
            sale = sale.text.strip() + "%"
        else:
            sale = "Не указана"
        period_sale = soup.find('div', class_='css-1aty3d4')
        if period_sale != None:
            period_sale = 'до ' + re.sub(r'[^0-9.]', '', period_sale.text.strip())
        else:
            period_sale = 'Не указана'
        definition = soup.find('div', class_='css-1lr25fx')
        if definition != None:
            definition = re.sub(r'\W', ' ', definition.text)
        else:
            definition = 'Нет описания'
        images_divs = soup.find_all('div', class_='swiper-slide')
        images = []
        try:
            for item in images_divs:
                attrs = item.img.attrs
                for key in attrs.keys():
                    if re.compile('src').search(key) != None:
                        images.append(
                            'https://www.auchan.ru' + attrs[key]
                        )
                    else:
                        continue
        except Exception as e:
            self.logger.error(f'error in parse func image level - {str(e)}')
        table = soup.find('table', class_='css-p83b4h')
        if table != None:
            article = table.find('td', class_='css-1v23ygr')
            if article != None:
                article = re.sub(r'\D', '', article.text)
            else:
                pass
        name = soup.find('h1', id='productName').text.strip()
        lis = soup.find_all('li', attrs={"itemprop": "itemListElement"})
        tmp = []
        for li in lis[1:-1]:
            tmp.append(li.span.text.strip())
        catalog_one, catalog_two, prefix = kwargs.pop('catalog_one'), kwargs.pop('catalog_two'), kwargs.pop('prefix')
        result = {
            'Подкаталог 1' : catalog_one,
            'Подкаталог 2' : catalog_two,
            'Название товара или услуги' : name,
            "Размещение на сайте" : 'Каталог/' + catalog_one + '/' + '/'.join(tmp),
            'Полное описание' : definition,
            'Краткое описание' : None,
            'Артикул' : str(prefix) + article,
            'Цена продажи' : None,
            'Старая цена' : old_price,
            'Цена закупки' : re.sub('[.]',',',price),
            'Остаток' : count,
            'Параметр: Бренд' : brand,
            'Параметр: Артикул поставщика' : article,
            'Параметр: Производитель' : brand,
            'Параметр: Размер скидки' : sale,
            'Параметр: Период скидки' : period_sale,
            'Параметр: Auch' : 'Auch',
            'Параметр: Group' : str(prefix)[:-1].upper(), **kwargs
        }
        tmp = {}
        for item in soup.find('table', class_='css-p83b4h').find_all('tr'):
            prop = item.find('th').text.strip()
            key = item.find('td').text.strip()
            tmp[prop] = key

        names = [
            'Страна производства',
            "Тип товара",
            "Область применения",
            "Пол",
            "Эффект от использования",
            "Назначение",
            'Тип крупы',
            
        ]
        for name in names:
            result[f'Параметр: {name}'] = None
            for key in tmp.keys():
                if name == key:
                    result[f'Параметр: {name}'] = tmp[key]
                    break
                else:
                    continue
        result['Изображения'] = ' '.join(images)
        result['Ссылка на товар'] = response.url
        yield result

    def closed(self, reason):
        import json
        bad_brands = [
           i.upper() for i in  pd.read_excel('AUCHAN/brands.xlsx').to_dict('list')[0]
        ]
        with open('auchan_test.jsonl', encoding='utf-8', mode='r') as file:
            s = file.readlines()
        result = [json.loads(i) for i in s]
        for item in result:
            if item['Параметр: Бренд'].upper() in bad_brands:
                del item
            else:
                continue
        with pd.ExcelWriter('auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
            for name, res in df(result, 'name'):
                p = pd.DataFrame(res)
                p.to_excel(writer, sheet_name=name.upper(), index=False)
            keys = [
                'Название товара или услуги',
                'Артикул',
                'Старая цена',
                'Остаток',
                'Цена закупки',
                'Цена продажи',
                'Параметр: Group',
                'Параметр: Auch'
            ]
            key = 'Параметр: Артикул поставщика'
            p = pd.DataFrame(result)
            p = filter(p,key)
            tmp = p.to_dict('list')
            for key in list(tmp.keys()):
                if key in keys:
                    pass
                else:
                    tmp.pop(key)
            p.to_excel(writer, index=False, sheet_name='result')
            pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')

def job():
    process = CrawlerProcess(
        settings=Settings()
    )
    process.crawl(My)
    process.start()

schedule.every().day.at('08:00', 'Europe/Moscow').do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

