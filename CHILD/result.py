import scrapy
import lxml
import re
import json
import requests
import logging

import pandas as pd

from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from CHILD.config import API_KEY


def proxys():
    response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
    data = response.json()
    result = []
    for item in data['list'].values():
        result.append(f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}')
    return result

def my_generator():
    lst = proxys()
    while True:
        for proxy in lst:
            yield proxy

def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

class ChildWorld(scrapy.Spider):
    name = 'ChildWorld'
    iter = my_generator()
    custom_settings = {
        'CONCURRENT_REQUESTS' : 30,
        "FEEDS" : {
            'result/child.jsonl' : {
                'format' : 'jsonlines',
                'encoding' : 'utf-8',
                'overwrite' : True
            }
        }, 
        "LOG_FILE" : 'logs/child.log',
        'LOG_FILE_APPEND' : False,
        'DOWNLOAD_FAIL_ON_DATALOSS' : False
    }
    def start_requests(self):
        p = pd.read_excel('CHILD/Detmir.xlsx').to_dict('list')
        start_urls = p['Ссылки на категории товаров']
        roots_categories = p['Корневая']
        add_categories, add2_categories = p['Подкатегория 1'], p['Подкатегория 2']
        placements = p['Размещение на сайте']
        prefixs = p['Префиксы']
        value = 1
        for url, root, add, add2, pref, place in zip(start_urls, roots_categories, add_categories, add2_categories, prefixs, placements):
            kwargs = {
                'root_category' : root,
                'add_category' : add,
                'add2_category' : add2 if add2 else None,
                'prefix' : pref,
                'placement' : place,
                'page' : 1,
                'domain' : url,
                'number' : value
            }
            yield scrapy.Request(
                url,
                cb_kwargs=kwargs,
                meta={
                    'proxy' : next(self.iter)
                }
            )
            value = value + 1
        self.iter.close()

    


    def ReceiveInfo(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find(attrs={'data-testid' : 'moreProductsItem'}).a.text.strip()
        title = soup.find('h1', attrs={'data-testid' : 'productTitle'}).text.strip()
        div_contain_sections = soup.find('div', attrs={'data-testid' : 'productSections'})
        for count, section in enumerate(div_contain_sections.find_all('section', recursive=False)):
            match count:
                case 0:
                    pictures = section.find_all("picture")
                    images = []
                    for item in [i.source["srcset"] for i in pictures]:
                        images.append(re.search(r'(.+?webp)', item)[0])
                    images = ' '.join(images)
                case 1:
                    ul = section.find('ul')
                    if ul:
                        ul = ul.find_all('li')
                        markers = ' '.join([li.text for li in ul])
                    else:
                        markers = None
                case 2:
                    if section.find('p', attrs={'data-testid' : 'price'}):
                        price = re.sub(r'[^,\.0-9]','',section.find('p', attrs={'data-testid' : 'price'}).text)
                        if '%' in section.find('p', attrs={'data-testid' : 'price'}).find_next().text:
                            sale_size = re.sub('\D', '', section.find('p', attrs={'data-testid' : 'price'}).find_next().text)
                        else:
                            sale_size = None
                    else:
                        price = 'Нет в наличии'
                        sale_size = None
                case 3:
                    description = section.find('section', attrs={'data-testid' : 'descriptionBlock'})
                    if description:
                        description = re.sub(r'\xa0', ' ', description.div.text.strip())
                    else:
                        description = None
                    characteristic = section.find('section', attrs={'data-testid' : 'characteristicBlock'})
                    tmp = {}
                    if characteristic:
                        table = characteristic.table
                        for it in table.find_all('tr'):
                            match it.th.text.strip().lower():
                                case 'артикул':
                                    article = it.td.text.strip()
                                    continue
                                case 'страна производства':
                                    name, prop = (f'Параметр: Страна-производитель', it.td.text.strip())
                                case 'продавец':
                                    continue
                                case 'вес упаковки, кг':
                                    name, prop = ('Вес', it.td.text.strip().replace('.', ','))
                                case 'тип продукта':
                                    name, prop = ('Вес', it.td.text.strip().replace('.', ','))
                                case _ :
                                    name, prop = (f'Параметр: {it.th.text.strip()}', it.td.text.strip())
                            tmp[name] = prop
                    else:
                        pass
        
        return {
            'Свойство: Вариант' : kwargs.pop("Свойство: Вариант") if "Свойство: Вариант" in kwargs.keys() else None,
            'Корневая' : kwargs.pop('root_category'),
            'Подкатегория 1' : kwargs.pop('add_category'),
            "Подкатегория 2" : kwargs.pop('add2_category'),
            'Артикул' : kwargs.pop('prefix') + tmp['Параметр: Код товара'],
            'Параметр: Тип продукта': None,
            'Параметр: Deti' : 'Deti' if 'zoozavr' not in response.url else 'Deti-Zoo',
            'Параметр: Group' : None,
            'Название товара или услуги' : title,
            'Размещение на сайте' : kwargs.pop('placement'),
            'Полное описание' : description,
            'Ссылка на товар' : response.url,
            'Цена продажи' : None,
            'Старая цена' : format(float((1 + int(sale_size) / 100) * 1.6 * float(price.replace(',', '.'))), '.2f').replace('.', ',') if price != 'Нет в наличии' and sale_size != None and sale_size else None,
            'Цена закупки' : price.replace('.', ','),
            'Изображения' : images,
            'Остаток' : 100 if price != 'Нет в наличии' else 0,
            'Параметр: Бренд' : brand,
            'Параметр: Производитель' : brand,
            'Параметр: Артикул поставщика' : article,
            'Параметр: Размер скидки' : sale_size,
            'Параметр: Метки' : markers,
            **kwargs, **tmp
        }
        
    def handler(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        if 'page' and 'domain' in kwargs.keys():
            kwargs.pop('page')
            kwargs.pop('domain')
        if soup.find('div', attrs={'data-testid' : 'variantsBlock'}):
            if 'zoozavr' in response.url:
                variants = [('https://www.zoozavr.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            else:
                variants = [('https://www.detmir.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            for url, var in variants:
                if url != response.url:
                    kwargs["Свойство: Вариант"] = var
                    yield scrapy.Request(url, callback=self.ReceiveInfo, cb_kwargs=kwargs)
                else:
                    kwargs["Свойство: Вариант"] = var
                    yield self.ReceiveInfo(response=response, **kwargs)
        else:
            yield self.ReceiveInfo(response=response, **kwargs)
            
    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')  
        domain, page = kwargs['domain'], kwargs['page']  
        if 'zoo' in domain:
            products = soup.find_all('section', id=re.compile(r'product-\d+'))
            for prod in products:
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
                    return
                link = prod.find('a')['href']
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        else:
            products = soup.find_all('section', id=re.compile(r'\d+'))
            for prod in products:
                link = prod.find(href=re.compile(r'.*?www\.detmir\.ru.*'))['href']
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
                    return
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        
        if soup.find(string=re.compile(r"показать ещё", flags=re.I)):
            new_url = domain + f'page/{page + 1}'
            kwargs['page'] += 1
            yield scrapy.Request(new_url, callback=self.parse, cb_kwargs=kwargs)  
       
    
            


    def closed(self, reason):
        with open('result/child.jsonl', 'r', encoding='utf-8') as file:
            s = file.readlines()
        result = [json.loads(item) for item in s]
        p = pd.DataFrame(result)
        with pd.ExcelWriter('result/child.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
            p.to_excel(writer, index=False, sheet_name='products')
            main_headers = [
                'Название товара или услуги',
                'Цена закупки',
                'Старая цена',
                'Артикул',
                'Параметр: Размер скидки',
                'Параметр: Остаток',
                'Цена продажи',
                'Параметр: Deti',
                'Параметр: Group'
            ]
            r = []
            for item in result:
                tmp = {}
                for key in item.keys():
                    if key in main_headers:
                        tmp[key] = item[key]
                r.append(tmp)
            p = pd.DataFrame(r)
            p.to_excel(writer, sheet_name='short_prod', index=False)
            for name, products in df(result, "number"):
                p = pd.DataFrame(products)
                p.to_excel(writer, sheet_name=f"{name} link", index=False)
          

def job():
    with open('CHILD/ready.txt', 'w') as file:
        file.write('RUN')
    process = CrawlerProcess(
        settings=Settings()
    )
    process.crawl(ChildWorld)
    process.start()
    with open('CHILD/ready.txt', 'w') as file:
        file.write('COMPLETE')