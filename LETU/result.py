from LETU.functions import *


logging.basicConfig(filename='logs/letu.log', filemode='w')
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(level=logging.DEBUG)



async def main():
    start = time.perf_counter()
    async with async_playwright() as play:
        browser = await play.chromium.launch(headless=True)
        context = await browser.new_context()
        while True:
            try:
                await init(context)
            except:
                schedule_logger.info('we get error in init method, we trying that repeat')
                continue
            else:
                break
        schedule_logger.info('we was initialize of context')
        table = pd.read_excel('LETU/Letu.xlsx', sheet_name=None)
        pages = [await context.new_page() for _ in range (1, 10)]
        start = time.perf_counter()
        summary_list = []
        schedule_logger.info('we walk to main code')
        with pd.ExcelWriter('result/letu_result.xlsx', mode='w', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
            for name, df in list(table.items()):
                schedule_logger.info(f'we have {name} items')
                start_local = time.perf_counter()
                df = df.to_dict('list')
                if name == 'Ссылка':
                    urls = df['Ссылки']
                    count = 0
                    urls = urls[::-1]
                    result_for_links = []
                    while urls:
                        for page in pages:
                            count += 1
                            if urls:
                                url = urls.pop()
                                result_for_links.append({f'{count}' : await Links(page=page, url=url)})
                                schedule_logger.info(f'we pass link {url}')
                    for d in result_for_links:
                        for k, v in list(d.items()):
                            local = []
                            while v:
                                tasks = []
                                for page in pages:
                                    if v:
                                        task = asyncio.create_task(data(page=page, url=v.pop(), logger=schedule_logger))
                                        tasks.append(task)
                                local += [i for j in await asyncio.gather(*tasks) for i in j if j != None]
                                summary_list += local
                                schedule_logger.info(f'we have {len(v)} products in this url')

                            p = pd.DataFrame(local)
                            p.to_excel(writer, index=False, sheet_name=f'{k} link')
                            schedule_logger.info(f'we pass all products in №{k} link')

                else:
                    result_for_brands = []
                    urls = df['Название Бренда']
                    while urls:
                        tasks = []
                        for page in pages:
                            if urls:
                                task = asyncio.create_task(Search(page=page, query=urls.pop()))
                                tasks.append(task)
                        result_for_brands += [i for j in await asyncio.gather(*tasks) for i in j]
                    schedule_logger.info('we pass sheet with brands')
                    local = []
                    while result_for_brands:
                        tasks = []
                        for page in pages:
                            if result_for_brands:
                                task = asyncio.create_task(data(page=page, url=result_for_brands.pop(), logger=schedule_logger))
                                tasks.append(task)
                        local += [i for j in await asyncio.gather(*tasks) for i in j]
                        summary_list += local
                        schedule_logger.info(f'we have {len(result_for_brands)} products for this brand')
                    p = pd.DataFrame(local)
                    p.to_excel(writer, index=False, sheet_name=name)
                schedule_logger.info(f'local time --- {time.perf_counter() - start_local}')



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
    schedule_logger.info(f'all time of execution code {time.perf_counter() - start}')




def job():
    with open('LETU/ready.txt', 'w') as file:
        file.write('RUN')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    with open('LETU/ready.txt', 'w') as file:
        file.write('COMPLETE')

