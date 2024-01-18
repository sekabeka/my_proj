import re
import asyncio
import logging
import schedule
import time

import pandas as pd

from playwright.async_api import async_playwright


def IsAvailable(data):
    if data['article'] == '':
        return False
    else:
        return True
    

def add_to_lst(iter, result_list):
    for prod in iter:
        if IsAvailable(prod):
            result_list.append(
                    f'https://www.letu.ru/s/api/product/v2/product-detail/{prod["repositoryId"]}?pushSite=storeMobileRU'
            )
        else:
            return False
    return True



async def init(context):
    page = await context.new_page()
    async with page.expect_request('https://getrcmx.com/api/v0/init') as first:
        await page.goto('https://www.letu.ru')
    await page.close()

async def request(page, url):
    async with page.expect_request(re.compile(r'https://www\.letu\.ru/s/api/product/listing/v1/products\?.*')) as first:
        await page.goto(url)
    return await first.value


async def Links(page, url):
    result_list = []
    response = await (await request(page, url)).response()
    data = await response.json()
    value = int(data['totalNumRecs'])
    add_to_lst(iter=data['products'], result_list=result_list)
    for i in range (36, value + 1, 36):
        url = re.sub(r'No=.*?&', f'No={i}&', response.url)
        data = await js(page=page, url=url)
        if add_to_lst(iter=data['products'], result_list=result_list) == False:
            break
    return result_list

async def Search(query:str, page):
    link = f'https://www.letu.ru/s/api/product/listing/v1/products?N=0&Nrpp=36&No=0&Ntt={query}&innerPath=mainContent%5B2%5D&resultListPath=%2Fcontent%2FWeb%2FSearch%2FSearch%20RU&pushSite=storeMobileRU'
    data = await js(page=page, url=link)
    value = int(data['totalNumRecs'])
    result_list = []
    add_to_lst(iter=data['products'], result_list=result_list)
    for i in range (36, value + 1, 36):
        link = re.sub(r'No=.*?&', f'No={i}&', link)
        data = await js(page=page, url=link)
        if add_to_lst(result_list=result_list, iter=data['products']) == False:
            break
    return result_list

async def js(page, url):
    count = 0
    while True:
        if count == 4:
            return None
        try:
            response = await page.goto(url)
        except:
            count += 1
            continue
        if response.ok:
            data = await response.json()
            break
        else:
            await asyncio.sleep(10)
    return data

def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])


async def init(context):
    page = await context.new_page()
    async with page.expect_request('https://getrcmx.com/api/v0/init') as first:
        await page.goto('https://www.letu.ru')

async def data(url, page, logger, **kwargs):
    flag = 0
    while flag != 5:
        try:
            response = await page.goto(url)
            # await asyncio.sleep(2)
            data = await response.json()
            new_link = f"https://www.letu.ru/s/api/product/v2/product-detail/{data['productId']}/tabs?locale=ru-RU&pushSite=storeMobileRU"
            response = await page.goto(new_link)
            # await asyncio.sleep(2)
            data2 = await response.json()
        except Exception as e:
            logger.error(str(e))
            flag += 1
        else:
            try:
                return parse(data, data2, **kwargs)
            except Exception as e:
                logger.error(str(e))
                break
    return {}



def image(imgs:list, images:list):
        for img in imgs:
            if img['type'] != 'shade':
                images.append(
                    'https://letu.ru' + img['url']
                )

def parse(data, data2, **kwargs):
    results = []
    prefix = 'VB4-'
    name = data['displayName']
    brand = data['brand']['name']
    definition = re.sub(r'<.*?>', '', data2['description']['longDescription'])
    specs_dict = {}
    for spec in data2['specsGroups']:
        for item in spec['specs']:
            specs_dict[f'Параметр: {item["name"]}'] = item['value']
    for_link = data['sefPath'].split('/')
    url = 'https://www.letu.ru/product' + for_link[-1] + '/' + data['productId']

    images = []
    image(data['media'], images)
    for item in data['skuList']:
        available = item['isInStock']
        if available:
            result = {}
            try:
                prop = item['unitOfMeasure'].strip()
            except:
                prop = ''
            weight = item['displayName']
            price = float(item['price']['amount'])
            sale_size = int(item['price']['discountPercent'])
            markers = [i['ui_name'] for i in item['appliedMarkers']]
            article = item['article']            
            price = float(item['price']['amount'])
            sale_size = int(item['price']['discountPercent'])
            try:
                img_prop = 'https://www.letu.ru' + item['shade']['image']['url']
            except Exception as e:
                img_prop = ''
            match sale_size:
                case 0:
                    result = { 
                        'Подкатегория 1' : "Косметика",
                        'Подкатегория 2' : 'Для волос',
                        'Подкатегория 3' : brand,
                        'Название товара или услуги' : name,
                        "Размещение на сайте" : 'catalog/' + '/'.join(for_link[1:-1]),
                        'Полное описание' : definition,
                        'Краткое описание' : weight,
                        'Артикул' : prefix + article,
                        'Цена продажи' : None,
                        'Старая цена' : None,
                        'Цена закупки' : str(price).replace('.', ','),
                        'Остаток' : 100,
                        'Параметр: Бренд' : brand,
                        'Параметр: Артикул поставщика' : article,
                        'Параметр: Производитель' : brand,
                        "Параметр: Размер скидки" : 'Скидки нет',
                        'Параметр: Период скидки' : None,
                        'Параметр: Метки' : ' '.join(markers),
                        'Параметр: Leto' : 'Leto', **specs_dict
                    }
                case _:
                    result = { 
                        'Подкатегория 1' : "Косметика",
                        'Подкатегория 2' : 'Для волос',
                        'Подкатегория 3' : brand,
                        'Название товара или услуги' : name,
                        "Размещение на сайте" : 'catalog/' + '/'.join(for_link[1:-1]),
                        'Полное описание' : definition,
                        'Краткое описание' : weight,
                        'Артикул' : prefix + article,
                        'Цена продажи' : None,
                        'Старая цена' : str(format(price * 1.6 * (1.0 + sale_size / 100), '.2f')).replace('.', ','),
                        'Цена закупки' : str(price).replace('.', ','),
                        'Остаток' : 100,
                        'Параметр: Бренд' : brand,
                        'Параметр: Артикул поставщика' : article,
                        'Параметр: Производитель' : brand,
                        "Параметр: Размер скидки" : str(sale_size) + '%',
                        'Параметр: Период скидки' : None,
                        'Параметр: Метки' : ' '.join(markers),
                        'Параметр: Leto' : 'Leto', **specs_dict
                    }
            result['Изображения'] = ' '.join(images)
            result['Ссылка на товар'] = url
            result['Изображения варианта'] = img_prop
            result[f'Свойство: {prop.lower()}'] = weight
            try:
                _ = result[f'Свойство: {prop.lower()}'] 
            except:
                result[f'Свойство: {prop.lower()}'] = weight
            results.append(result)
        else:
            continue
    return results
