from playwright.async_api import async_playwright
import pandas as pd
import re
import asyncio
from json import loads
from types import NoneType

def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])


async def init(context):
    page = await context.new_page()
    async with page.expect_request('https://getrcmx.com/api/v0/init') as first:
        await page.goto('https://www.letu.ru')

async def main():
    async with async_playwright() as play:
        browser = await play.chromium.launch(headless=True)
        context = await browser.new_context()
        await init(context)
        table = pd.read_excel('input_data/products2.xlsx', sheet_name=None)
        pages = [await context.new_page() for _ in range (1, 10)]
        import time
        start = time.perf_counter()
        summary_list = []
        with pd.ExcelWriter('results/letu_result.xlsx', mode='w', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
            for name, df in list(table.items()):
                s = df.to_dict('list')
                urls = s['URL']
                articles = s['ARTICLE']
                prefixs = s['PREFIX']
                result = []
                len_urls = len(urls)
                print(f'We have {len_urls} tasks')
                start_catalog = time.perf_counter()
                while urls:
                    tasks = []
                    for page in pages:
                        if urls:
                            task = asyncio.create_task(js(page, urls.pop(), prefix=prefixs.pop(), article=articles.pop(), repeat=0))
                            tasks.append(task)
                    result += await asyncio.gather(*tasks)
                    print (f'We complete {len_urls - len(urls)} tasks! Count of urls to the end {len(urls)}')
                result = [i for j in result if j != None for i in j ]
                summary_list += result
                p = pd.DataFrame(result)
                p.to_excel(writer, index=False, sheet_name=name)
                print (f'LOCAL TIME --- {time.perf_counter() - start_catalog} ---')
                print (f'We have {len(result)} items from {len_urls}')
                
            keys = [
                'Название товара или услуги',
                'Артикул',
                'Старая цена',
                'Остаток',
                'Цена закупки',
                'Цена продажи'
            ]
            key = 'Параметр: Артикул поставщика'
            p = pd.DataFrame(summary_list)
            p = filter(p, key)
            tmp = p.to_dict('list')
            for key in list(tmp.keys()):
                if key in keys:
                    pass
                else:
                    tmp.pop(key)
            p.to_excel(writer, index=False, sheet_name='result')
            pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')
            print (f'TIME --- {time.perf_counter() - start} ---')
        
async def dates(page, url):
    response = await page.goto(url)
    return loads(re.sub(r'<.*?>', '', await response.text()))
        

async def json(page, url):
    response = await page.goto(url)
    if response.ok:
        return await response.json()
    else:
        await asyncio.sleep(10)
        await json(page, url)


async def js(page, url, **kwargs):
    try:
        if kwargs['repeat'] == 4:
            return []
        try:
            data = await json(page, url)
        except:
            return []
        new_link = f"https://www.letu.ru/s/api/product/v2/product-detail/{data['productId']}/tabs?locale=ru-RU&pushSite=storeMobileRU"
        try:
            data2 = await json(page, new_link)
        except:
            return []
        if type(data2) != NoneType:
            return parse(data, data2, **kwargs)
        else:
            raise TypeError
    except TypeError:
        kwargs['repeat'] += 1
        await js (page, url, **kwargs)

def image(imgs:list, images:list):
        for img in imgs:
            if img['type'] != 'shade':
                images.append(
                    'https://letu.ru' + img['url']
                )

def parse(data, data2, **kwargs):
    results = []
    prefix = kwargs['prefix']
    article = kwargs['article']
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
    
        
    
    
            
    
            

asyncio.run(main())




            
        
        
