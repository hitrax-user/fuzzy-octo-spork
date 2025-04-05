
import logging
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Логирование
import os
import logging

log_path = os.path.join(os.path.dirname(__file__), "bot.log")

logging.basicConfig(
       level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info("🔥 Логгер инициализирован успешно. Лог-файл: %s", log_path)
print("📂 Текущая директория запуска:", os.getcwd())
print("📄 Ожидаемый лог-файл:", log_path)


# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1OiUKuuJhHXNmTr-KWYdVl7UapIgAbDuuf9w34hbQNFU/edit#gid=0").sheet1

BOT_TOKEN = "7645593337:AAHWs1_kIdpUBZkdQxKd_OcN9IEzTC7umVs"

def parse_cian(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1", {"data-mark": "title"}).text.strip()
        address = soup.find("span", {"data-mark": "address"}).text.strip()
        price_raw = soup.find("span", {"data-mark": "MainPrice"}).text.strip()
        price = re.sub(r"[^\d]", "", price_raw)

        details = soup.find_all("li", class_="a10a3f92e9--item--_ipjW")
        area = year = balcony = None

        area_text = next((d.text for d in details if "м²" in d.text), None)
        if area_text:
            match = re.search(r"\d+,\d+|\d+", area_text)
            area = float(match.group().replace(",", ".")) if match else None

        year_text = next((d.text for d in details if "год" in d.text.lower()), None)
        if year_text:
            match = re.search(r'\d{4}', year_text)
            if match:
                y = int(match.group())
                if 1800 <= y <= 2025:
                    year = y

        balcony_text = next((d.text for d in details if "балкон" in d.text.lower()), None)
        if balcony_text:
            balcony_match = re.search(r"(есть|нет)", balcony_text.lower())
            balcony = balcony_match.group(1) if balcony_match else balcony_text

        try:
            district = soup.find("span", {"data-mark": "district"}).text.strip()
        except:
            district = address.split(",")[1].strip() if "," in address else None

        logging.info(f"✅ Успешно распарсили Avito: {url}")
        return {
            "title": title,
            "address": address,
            "district": district,
            "area": area,
            "year": year,
            "price": price,
            "url": url,
            "balcony": balcony
        }

    except Exception as e:
        logging.exception(f"Ошибка парсинга CIAN для {url}: {e}")
        return None


# Финальная версия parse_avito с исправленными регулярками и логированием



def parse_avito(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        title = address = price = district = balcony = None
        area = year = None

        try:
            title_raw = driver.title
            title_match = re.match(r"^(.*?)\s*\|", title_raw)
            if title_match:
                title = title_match.group(1).strip()
        except:
            pass

        try:
            address = driver.find_element(By.CLASS_NAME, "style-item-address__string-wt61A").text
        except Exception as e:
            logging.warning(f"Адрес не найден: {e}")

        try:
            price_element = driver.find_element(By.CSS_SELECTOR, '[itemprop="price"]')
            price = price_element.get_attribute("content")
        except:
            try:
                price_raw = driver.find_element(By.CLASS_NAME, "js-item-price").text
                price = re.sub(r"[^\d]", "", price_raw)
            except Exception as e:
                logging.warning(f"Цена не найдена: {e}")

        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".params-paramsList__item--2Y20")))
            all_params = driver.find_elements(By.CSS_SELECTOR, ".params-paramsList__item--2Y20")
        except Exception as e:
            logging.warning(f"Ошибка ожидания параметров: {e}")
            all_params = []

        if all_params:
            for item in all_params:
                text = item.text
                logging.info(f"Параметр: {text}")
                if "Общая площадь" in text:
                    match = re.search(r"\d+,\d+|\d+", text)
                    if match:
                        area = float(match.group().replace(",", "."))
                elif "Год постройки" in text or "Год сдачи" in text:
                    match = re.search(r'\d{4}', text)
                    if match:
                        y = int(match.group())
                        if 1800 <= y <= 2025:
                            year = y
                elif "Балкон" in text:
                    if "нет" in text.lower():
                        balcony = "нет"
                    elif "есть" in text.lower() or re.search(r"\d+", text):
                        balcony = "есть"
                    else:
                        parts = text.split(":")
                        if len(parts) > 1:
                            balcony = parts[1].strip()
        else:
            logging.warning("❗ Не удалось найти параметры через Selenium, пробуем BeautifulSoup")
            try:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                param_items = soup.find_all("li", class_=re.compile("params-paramsList.*"))
                for item in param_items:
                    text = item.get_text(strip=True)
                    logging.info(f"[BS] Параметр: {text}")
                    if "Общая площадь" in text:
                        match = re.search(r"\d+,\d+|\d+", text)
                        if match:
                            area = float(match.group().replace(",", "."))
                    elif "Год постройки" in text or "Год сдачи" in text:
                        match = re.search(r'\d{4}', text)
                        if match:
                            y = int(match.group())
                            if 1800 <= y <= 2025:
                                year = y
                    elif "Балкон" in text:
                        if "нет" in text.lower():
                            balcony = "нет"
                        elif "есть" in text.lower() or re.search(r"\d+", text):
                            balcony = "есть"
                        else:
                            parts = text.split(":")
                            if len(parts) > 1:
                                balcony = parts[1].strip()
            except Exception as e_bs:
                logging.warning(f"BeautifulSoup не помог: {e_bs}")

        if address and "," in address:
            parts = address.split(",")
            if len(parts) > 1:
                district = parts[1].strip()

        return {
            "title": title,
            "address": address,
            "district": district,
            "area": area,
            "year": year,
            "price": price,
            "url": url,
            "balcony": balcony
        }

    except Exception as e:
        logging.exception(f"Ошибка парсинга Avito для {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        title = address = price = district = balcony = None
        area = year = None

        try:
            title_raw = driver.title
            title_match = re.match(r"^(.*?)\s*\|", title_raw)
            if title_match:
                title = title_match.group(1).strip()
        except:
            pass

        try:
            address = driver.find_element(By.CLASS_NAME, "style-item-address__string-wt61A").text
        except Exception as e:
            logging.warning(f"Адрес не найден: {e}")

        try:
            price_element = driver.find_element(By.CSS_SELECTOR, '[itemprop="price"]')
            price = price_element.get_attribute("content")
        except:
            try:
                price_raw = driver.find_element(By.CLASS_NAME, "js-item-price").text
                price = re.sub(r"[^\d]", "", price_raw)
            except Exception as e:
                logging.warning(f"Цена не найдена: {e}")

        try:
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "params-paramsList__item--2Y20")))
            all_params = driver.find_elements(By.CLASS_NAME, "params-paramsList__item--2Y20")
            for item in all_params:
                text = item.text.replace("\xa0", " ")
                logging.info(f"Параметр: {text}")
                if "Общая площадь" in text:
                    match = re.search(r"\d+,\d+|\d+", text)
                    if match:
                        area = float(match.group().replace(",", "."))
                elif "Год постройки" in text or "Год сдачи" in text:
                    match = re.search(r'\d{4}', text)
                    if match:
                        y = int(match.group())
                        if 1800 <= y <= 2025:
                            year = y
                elif "Балкон" in text:
                    if "нет" in text.lower():
                        balcony = "нет"
                    elif "есть" in text.lower() or re.search(r"\d+", text):
                        balcony = "есть"
                    else:
                        parts = text.split(":")
                        if len(parts) > 1:
                            balcony = parts[1].strip()
        except Exception as e:
            logging.warning(f"Ошибка парсинга параметров: {e}")

        if address and "," in address:
            parts = address.split(",")
            if len(parts) > 1:
                district = parts[1].strip()

        logging.info(f"✅ Распарсили Avito:\n📌 {title}\n🏠 {address}, {district}\n📐 {area} м², 🏗 {year}, 🪟 Балкон: {balcony}\n💰 {price} ₽")

        return {
            "title": title,
            "address": address,
            "district": district,
            "area": area,
            "year": year,
            "price": price,
            "url": url,
            "balcony": balcony
        }

    except Exception as e:
        logging.exception(f"Ошибка парсинга Avito для {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()




async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Отправь ссылку на Avito или CIAN, и я добавлю её в таблицу 📄")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    logging.info("Бот получил ссылку: " + url)

    if "avito.ru" in url:
        data = parse_avito(url)
    elif "cian.ru" in url:
        data = parse_cian(url)
    else:
        await update.message.reply_text("Пожалуйста, пришли ссылку с Avito или CIAN 🙏")
        return

    if not data:
        await update.message.reply_text("❌ Не удалось обработать ссылку.")
        return

    message = (
        f"✅ Добавлено в таблицу!\n"
        f"📝 Название: {data['title']}\n"
        f"🏡 Адрес: {data['address']}\n"
        f"📍 Район: {data['district']}\n"
        f"📐 Площадь: {data['area']} м²\n"
        f"🕰 Год: {data['year']}\n"
        f"💰 Цена: {data['price']} ₽\n"
        f"🪟 Балкон: {data['balcony']}"
    )

    await update.message.reply_text(message)

    try:
        sheet.append_row([
            data["title"],
            data["address"],
            data["district"],
            data["area"],
            data["year"],
            data["price"],
            data["url"],
            data["balcony"],
            ""  # комментарий
        ])
    except Exception as e:
        logging.exception(f"Ошибка записи в таблицу для {url}: {e}")


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == "__main__":
    main()
