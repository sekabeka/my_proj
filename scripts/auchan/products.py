
from functions import *
list_of_products_errors, auchan = [], 'https://www.auchan.ru'

def filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])

async def get_info(session:aiohttp.ClientSession, link:str, prefix:str, catalog_one:str, catalog_two:str):
    try:
        async with session.get(link) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
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
                print(e)
                images = ['Нет изображений']
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
                'Параметр: Group' : str(prefix)[:-1].upper()
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
            result['Ссылка на товар'] = link
            return result
    except Exception as e:
        list_of_products_errors.append(
            (link, prefix, catalog_one, catalog_two)
        )

async def main():
    async with aiohttp.ClientSession(cookies=await get_cookies(auchan), connector=aiohttp.TCPConnector(limit=7)) as session:
        # with open('auchan/products.json', 'r') as js:
        #     items = json.load(js)
        p = pd.read_excel('input_data/Ashan_pervy-posledniy.xlsx', sheet_name=None)
        res = []
        names = list(p.keys())
        DataFrames = list(p.values())
        exceptions = read_excel('input_data/brands.xlsx')[0]
        with pd.ExcelWriter('results/auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
            for name, df in zip(names, DataFrames):
                df = df.to_dict('list')
                catalogs_one = df['Подкаталог 1']
                catalogs_two = df['Подкаталог 2']
                prefixs = df['Префикс']
                urls = df['Ссылка на товар']
                tasks = []
                for url, prefix, cat1, cat2 in zip(urls, prefixs, catalogs_one, catalogs_two):
                    task = asyncio.create_task(get_info(session, url, prefix, cat1, cat2))
                    tasks.append(task)
                result = await asyncio.gather(*tasks)
                tasks.clear()
                loop = 0
                ln = 0
                while len(list_of_products_errors) != ln:
                    ln = len(list_of_products_errors)
                    print (f'Количество ошибок на данный момент - {ln}\nИдем исправлять их :)')
                    if loop % 3 == 0:
                        await set_cookies_in_session(session, auchan)
                    while len(list_of_products_errors):
                        link, prefix, catalog_one, catalog_two = list_of_products_errors.pop()
                        task = asyncio.create_task(get_info(session, link, prefix, catalog_one, catalog_two))
                        tasks.append(task)
                    result += await asyncio.gather(*tasks)
                    loop += 1
                    print (f'Ошибок после исправления {len(list_of_products_errors)}')
                result = [i for i in result if i != None]
                for item in result:
                    if item in exceptions:
                        result.pop(result.index(item))
                    else:
                        continue
                res += result
                p = pd.DataFrame(result)
                p.to_excel(writer, index=False, sheet_name=name)
                print (f'Мы прошли {name}')
                list_of_products_errors.clear()
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
            p = pd.DataFrame(res)
            p = filter(p,key)
            tmp = p.to_dict('list')
            for key in list(tmp.keys()):
                if key in keys:
                    pass
                else:
                    tmp.pop(key)
            # for v in tmp['Артикул']:
            #     tmp['Параметр: Auch'] = 'Auch'
            #     tmp['Параметр: Group'] = v.split('-')[0].upper()
            p.to_excel(writer, index=False, sheet_name='result')
            pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')

    print (f'Количество не собранных товаров - {len(list_of_products_errors)}')
  

asyncio.run(main())