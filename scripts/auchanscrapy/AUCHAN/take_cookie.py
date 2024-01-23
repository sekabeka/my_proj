import requests

from playwright.sync_api import sync_playwright
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

def proxy_and_cookies():
	lst_proxy = proxys()
	with sync_playwright() as p:
		browser = p.firefox.launch(headless=True)
		while True:
			for proxy in lst_proxy:
				context = browser.new_context(
					proxy=proxy
                )
				page = context.new_page()
				seconds, attempt = 5, 1
				while attempt != 3:
					try:
						page.goto(
							url='https://www.auchan.ru'
                        )
						page.wait_for_timeout(seconds)
					except:
						seconds += 1
						continue
					else:
						cookies = {
							item['name'] : item['value'] for item in context.cookies()
                        }
						if len(cookies) > 10:
							break
						else:
							attempt += 1
				if attempt == 3:
					continue
				yield cookies, f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"
				context.close()
				



						
