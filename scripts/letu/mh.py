from playwright.async_api import async_playwright
import asyncio
import pandas as pd
import re

def IsAvailable(data):
    if data['article'] == '':
        return False
    else:
        return True

def add_to_lst(iter, result_list, prefix):
    for prod in iter:
        if IsAvailable(prod):
            result_list.append({
                    'URL' : f'https://www.letu.ru/s/api/product/v2/product-detail/{prod["repositoryId"]}?pushSite=storeMobileRU',
                    'ARTICLE' : prod['article'],
                    'PREFIX' : prefix
                }
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

async def Links(page, url, prefix):
    result_list = []
    response = await (await request(page, url)).response()
    data = await response.json()
    value = int(data['totalNumRecs'])
    add_to_lst(iter=data['products'], result_list=result_list, prefix=prefix)
    for i in range (36, value + 1, 36):
        url = re.sub(r'No=.*?&', f'No={i}&', response.url)
        data = await js(page=page, url=url)
        if add_to_lst(iter=data['products'], result_list=result_list, prefix=prefix) == False:
            break
        print (f'We pass {i} for {value}')
    return result_list

async def Search(query:str, prefix:str, page):
    link = f'https://www.letu.ru/s/api/product/listing/v1/products?N=0&Nrpp=36&No=0&Ntt={query}&innerPath=mainContent%5B2%5D&resultListPath=%2Fcontent%2FWeb%2FSearch%2FSearch%20RU&pushSite=storeMobileRU'
    data = await js(page=page, url=link)
    value = int(data['totalNumRecs'])
    result_list = []
    add_to_lst(iter=data['products'], result_list=result_list, prefix=prefix)
    for i in range (36, value + 1, 36):
        link = re.sub(r'No=.*?&', f'No={i}&', link)
        data = await js(page=page, url=link)
        if add_to_lst(result_list=result_list, iter=data['products'], prefix=prefix) == False:
            break
        print (f'We pass {i} for {value}')
    return result_list
    
async def js(page, url):
    count = 0
    while True:
        if count == 4:
            print (f'We return None for url -- {url}')
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

async def main():
    async with async_playwright() as play:
        browser = await play.chromium.launch(headless=True)
        context = await browser.new_context()
        await init(context)
        table = pd.read_excel('input_data/Letu.xlsx', sheet_name=None)
        pages = [await context.new_page() for _ in range (1, 10)]
        import time
        start = time.perf_counter()
        with pd.ExcelWriter('input_data/products2.xlsx', mode='w', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
            for name, df in list(table.items()):
                df = df.to_dict('list')
                prefixs = df['Префикс']
                result = []
                if name == 'Ссылка':
                    urls = df['Ссылки']
                    count = 0
                    urls = urls[::-1]
                    prefixs = prefixs[::-1]
                    while urls:
                        for page in pages:
                            count += 1
                            if urls:
                                url, prefix = urls.pop(), prefixs.pop()
                                tmp_result = [i for i in await Links(page=page, url=url, prefix=prefix) if i != None]
                            else:
                                break
                            print (f'We have +1 passed link for {len(urls)}')
                            p = pd.DataFrame(tmp_result)
                            p.to_excel(writer, index=False, sheet_name=f'{count} link')
                else:
                    urls = df['Название Бренда']
                    len_urls = len(urls)
                    print(f'We have {len_urls} tasks')
                    while urls:
                        tasks = []
                        for page in pages:
                            if urls:
                                task = asyncio.create_task(Search(page=page, query=urls.pop(), prefix=prefixs.pop()))
                                tasks.append(task)
                        result += await asyncio.gather(*tasks)
                        print (f'We get {len(result)} items for {name}')
                    result = [i for j in result if j != None for i in j ]
                    p = pd.DataFrame(result)
                    p.to_excel(writer, index=False, sheet_name=name)
            print (f'TIME --- {time.perf_counter() - start} ---')
        

asyncio.run(main())





    

