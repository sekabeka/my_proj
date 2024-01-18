import requests
import asyncio

from playwright.async_api import async_playwright, Browser

from AUCHAN.config import API_KEY

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

async def data(browser:Browser, proxy):
	context = await browser.new_context(
		proxy = proxy
	)
	page = await context.new_page()
	while True:
		try:
			await page.goto('https://www.auchan.ru/')
			await asyncio.sleep(5)
			await page.wait_for_load_state()
		except:
			continue
		else:
			break
	cookies = {i['name'] : i['value'] for i in (await context.cookies())}
	proxies = f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"

	return cookies, proxies


async def main():
	async with async_playwright() as p:
		browser = await p.firefox.launch(headless=True)
		proxy_list = proxys()
		tasks = [
			asyncio.create_task(
				data(browser=browser, proxy=proxy)
			) for proxy in proxy_list
		]
		return await asyncio.gather(*tasks)
		

def result_cookies():
	loop = asyncio.get_event_loop()
	return loop.run_until_complete(main())
	

