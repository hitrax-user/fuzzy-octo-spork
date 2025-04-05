import json
from playwright.async_api import async_playwright
import asyncio

async def extract_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False, чтобы видеть браузер
        page = await browser.new_page()
        await page.goto("https://www.avito.ru/")
        print("Браузер открыт. Пожалуйста, войдите в аккаунт или пройдите CAPTCHA, если требуется.")
        await asyncio.sleep(30)  # Даем время на ручные действия
        cookies = await page.context.cookies()
        with open("cookies_avito.json", "w", encoding="utf-8") as f:
            json.dump(cookies, f, indent=4)
        print("Куки сохранены в cookies_avito.json")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(extract_cookies())