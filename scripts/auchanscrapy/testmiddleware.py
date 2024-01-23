
from scrapy.http import Request, Response
from scrapy.spiders import Spider

import requests
import asyncio

from playwright.async_api import async_playwright
API_KEY = 'd507f5285e-5834de8469-bd6d9e795a'


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
            continue
        else:
            break
    await page.close()
    return await context.cookies(), f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"

async def main():
    list_proxy = proxys()
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        tasks = [
            asyncio.create_task(_cookie(browser=browser, proxy=proxy)) for proxy in list_proxy
        ]
        return await asyncio.gather(*tasks)

def get_pairs():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(main())



def my_gen():
    flag = False
    lst = get_pairs()
    while True:
        for cookies, proxy in lst:
            flag = yield (cookies, proxy)
            if flag:
                lst = get_pairs()
                break




class test():
    iter = my_gen()
    def process_request(self, request:Request, spider:Spider):
        idx = request.cb_kwargs['idx']
        if idx in range(2000, 20000, 2000):
            print (idx)
            self.iter = my_gen()
        cookies, proxy = next(self.iter)
        request.meta.update(
            proxy=proxy
        )
        request.cookies = cookies
        return None

    def process_response(self, request:Request, response:Response, spider:Spider):
        if response.status in [401, 503]:
            return request
        return response

                        

    def process_exception(self, request, exception, spider):
        spider.logger.error(str(exception))



async def cookies():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        list_proxy = proxys()
        contexts = [
            await browser.new_context(
                proxy=proxy 
            ) for proxy in list_proxy
        ]
        for context, proxy in zip(contexts, list_proxy):
            proxy = f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"
            page = await context.new_page()
            while True:
                try:
                    async with page.expect_request("https://www.auchan.ru/v1/animations/settings?prev_sort=0"):
                        await page.goto(url='https://www.auchan.ru')
                except:
                    continue
                else:
                    break
        
            await page.reload()


            
        




