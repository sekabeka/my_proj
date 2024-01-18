import scrapy
import re

from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup

from AUCHAN.module_with_cookies import result_cookies

import pandas as pd


def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

def my_generator():
    iterable = result_cookies()
    while True:
        for item in iterable:
            yield item


def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])

class MySpider(scrapy.Spider):
    name = 'scraper'
    custom_settings = {
        'CONCURRENT_REQUESTS' : 50,
        "DOWNLOAD_FAIL_ON_DATALOSS" : False,
        "FEEDS" : {
            'result/auchan.jsonl' : {
                'format' : 'jsonlines',
                'overwrite' : True,
                'encoding' : 'utf-8'
            }
        },
        "LOG_FILE" : 'logs/auchan.log',
        'LOG_FILE_APPEND' : False
    }

    def start_requests(self):
        p = pd.read_excel('AUCHAN/Ashan_pervy-posledniy.xlsx', sheet_name=None)
        names = list(p.keys())
        DataFrames = list(p.values())
        for name, df in zip(names, DataFrames):
            self.logger.info('we go to initialize new cookie')
            self.iter = my_generator()
            df = df.to_dict('list')
            catalogs_one = df['Подкаталог 1']
            catalogs_two = df['Подкаталог 2']
            prefixs = df['Префикс']
            urls = df['Ссылка на товар']
            for url, pref, cat1, cat2 in zip(urls, prefixs, catalogs_one, catalogs_two):
                cookies, proxy = next(self.iter)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        'proxy' : proxy
                    },
                    cookies=cookies, 
                    cb_kwargs=dict(
                        catalog_one=cat1,
                        prefix=pref,
                        catalog_two=cat2,
                        name=name.upper()
                    )
                )
        self.iter.close()
        
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
        with open('result/auchan.jsonl', encoding='utf-8', mode='r') as file:
            s = file.readlines()
        result = [json.loads(i) for i in s]
        for item in result:
            if item['Параметр: Бренд'].upper() in bad_brands:
                del item
            else:
                continue
        with pd.ExcelWriter('result/auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
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
    with open ('AUCHAN/ready.txt', 'w') as file:
        file.write('RUN')
    process = CrawlerProcess(
        settings = Settings()
    )
    process.crawl(MySpider)
    process.start()
    with open ('AUCHAN/ready.txt', 'w') as file:
        file.write('COMPLETE')