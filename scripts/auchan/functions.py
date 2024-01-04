from lib import *

def read_excel(path_to_file:str):
    return pd.read_excel(path_to_file).to_dict('list')

async def set_cookies_in_session(session:aiohttp.ClientSession, url:str):
    session.cookie_jar.update_cookies(await get_cookies(url))

async def get_cookies(url:str):
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url)
        time.sleep(7)

        # loc = page.locator('#checkAddressHeader > span')
        # await loc.wait_for(state='attached')
        # await loc.click()

        # await page.locator('#deliveryAddressInput').fill('г Москва, ул Щербаковская, д 8')
        # await page.get_by_role('button', name='Сохранить').click()

        await page.close()
        return {i['name']: i['value'] for i in await context.cookies()}
    