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
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # хотим видеть свои INFO-сообщения

# «Заглушим» шумные логи httpx, httpcore, telegram.request
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)
logging.info("🔥 Логгер инициализирован успешно. Лог-файл: %s", log_path)
print("📂 Текущая директория запуска:", os.getcwd())
print("📄 Ожидаемый лог-файл:", log_path)

# Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1OiUKuuJhHXNmTr-KWYdVl7UapIgAbDuuf9w34hbQNFU/edit#gid=0"
).sheet1

BOT_TOKEN = "7645593337:AAHWs1_kIdpUBZkdQxKd_OcN9IEzTC7umVs"

def parse_cian(url):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    import logging
    import re
    import json

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://spb.cian.ru")

        try:
            with open("cookies_cian.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
                for cookie in cookies:
                    if "domain" in cookie and not (
                        "cian.ru" in cookie["domain"] or "spb.cian.ru" in cookie["domain"]
                    ):
                        continue
                    try:
                        driver.add_cookie(cookie)
                    except Exception as ce:
                        logging.debug(f"⛔ Ошибка добавления cookie {cookie.get('name')}: {ce}")
            logging.info("✅ Куки подставлены в браузер")
        except Exception as e:
            logging.warning(f"❌ Не удалось загрузить куки: {e}")

        driver.get(url)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        soup = BeautifulSoup(driver.page_source, "html.parser")

        def extract_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        title = extract_text("h1[class*='--title--']")
        price_raw = extract_text('[data-testid="price-amount"]')
        price = re.sub(r"[^\d]", "", price_raw or "")

        address_parts = soup.select('a[data-name="AddressItem"]')
        address_texts = [a.get_text(strip=True) for a in address_parts]
        address = ", ".join(address_texts)
        district = next((x for x in address_texts if "р-н" in x), None)

        # Площадь
        area = None
        for div in soup.select('div[data-name="OfferSummaryInfoItem"]'):
            label = div.select_one("p")
            if label and "Общая площадь" in label.text:
                value = div.select("p")[1].get_text(strip=True)
                area_match = re.search(r"\d+([.,]\d+)?", value)
                if area_match:
                    area = float(area_match.group(0).replace(",", "."))

        # Этаж
        floor = None
        floor_match = soup.find("span", string=re.compile(r"\d+\s+из\s+\d+"))
        if floor_match:
            floor = floor_match.get_text(strip=True)

        # Год постройки
        year = None
        year_tag = soup.find("p", string=re.compile(r"^\d{4}$"))
        if year_tag:
            try:
                y_val = int(year_tag.text.strip())
                if 1800 <= y_val <= 2100:
                    year = y_val
            except:
                pass

        # Балкон
        balcony = None
        balcony_tag = soup.find("p", string=re.compile(r"балкон", re.IGNORECASE))
        if balcony_tag:
            balcony = balcony_tag.get_text(strip=True)

        return {
            "title": title,
            "address": address,
            "district": district,
            "area": area,
            "year": year,
            "price": price,
            "url": url,
            "balcony": balcony,
            "floor": floor,
            "source": "CIAN-Selenium"
        }

    except Exception as e:
        logging.exception(f"Ошибка парсинга CIAN через Selenium для {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()

# ===========================
# parse_avito
# ===========================
def parse_avito(url: str) -> dict:
    """
    Парсит Avito:
    - Название (title)
    - Адрес (без 'Санкт-Петербург, ')
    - Район (по блоку 'р-н Приморский')
    - Площадь (Общая площадь)
    - Год постройки
    - Балкон
    - Этаж
    - Цена
    - Исходная ссылка (url)
    """

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

        # 1) Название из тайтла:
        title = driver.title
        if "|" in title:
            title = title.split("|")[0].strip()

        # 2) Цена
        price = None
        try:
            price_el = driver.find_element(By.CSS_SELECTOR, '[itemprop="price"]')
            price = price_el.get_attribute("content")
        except Exception:
            logging.warning("Не нашли price в itemprop='price' - fallback ниже")

        # Будем искать адрес и район
        address = None
        district = None

        # 3) Адрес
        try:
            # Элемент с адресом: <span class="style-item-address__string-wt61A">
            address_el = driver.find_element(By.CSS_SELECTOR, ".style-item-address__string-wt61A")
            address = address_el.text.strip()
            # Убираем "Санкт-Петербург, " если есть
            if address.startswith("Санкт-Петербург, "):
                address = address.replace("Санкт-Петербург, ", "").strip()
        except Exception as e:
            logging.warning(f"Адрес не найден: {e}")

        # 4) Район
        try:
            # Блок: <span class="style-item-address-georeferences-item-TZsrp">
            # Внутри ещё <span>р-н Приморский</span>
            district_el = driver.find_element(By.CSS_SELECTOR, ".style-item-address-georeferences-item-TZsrp span:nth-child(2)")
            district = district_el.text.strip()  # Например: "р-н Приморский"
        except Exception as e:
            logging.warning(f"Район не найден: {e}")

        # Подготовим переменные для характеристик
        area = None
        floor = None
        balcony = None
        year = None

        # Для регулярных выражений
        re_area = r"\d+(?:[.,]\d+)?"
        re_floor = r"(\d+\s*из\s*\d+)|(\d+/\d+)"

        try:
            # Ожидаем <li>, чей класс начинается на "params-paramsList__item"
            wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "li[class^='params-paramsList__item']")
            ))
            all_params = driver.find_elements(By.CSS_SELECTOR, "li[class^='params-paramsList__item']")

            if all_params:
                for item in all_params:
                    text = item.text.strip()
                    logging.info(f"Параметр: {text}")

                    # Общая площадь
                    if "Общая площадь" in text:
                        match = re.search(re_area, text)
                        if match:
                            area_val = match.group(0).replace(",", ".")
                            area = float(area_val)

                    # Год постройки или Год сдачи
                    elif "Год постройки" in text or "Год сдачи" in text:
                        match = re.search(r"\d{4}", text)
                        if match:
                            y = int(match.group())
                            if 1800 <= y <= 2050:
                                year = y

                    # Балкон
                    elif "Балкон" in text:
                        text_low = text.lower()
                        if "нет" in text_low:
                            balcony = "нет"
                        elif "есть" in text_low or re.search(r"\d+", text_low):
                            balcony = "есть"
                        else:
                            parts = text.split(":")
                            if len(parts) > 1:
                                balcony = parts[1].strip()

                    # Этаж
                    elif "Этаж" in text:
                        fm = re.search(re_floor, text)
                        if fm:
                            floor = fm.group(0)  # "6 из 24" или "6/24"
                        else:
                            # fallback: берем всё после двоеточия
                            parts = text.split(":")
                            if len(parts) > 1:
                                floor = parts[1].strip()

            else:
                logging.warning("Не нашли params-paramsList__item через Selenium, пробуем BS fallback")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                param_items = soup.find_all("li", class_=re.compile(r"params-paramsList__item"))

                for li_ in param_items:
                    text = li_.get_text(strip=True)
                    logging.info(f"[BS] Параметр: {text}")

                    if "Общая площадь" in text:
                        m = re.search(re_area, text)
                        if m:
                            area = float(m.group(0).replace(",", "."))
                    elif "Год постройки" in text or "Год сдачи" in text:
                        m = re.search(r"\d{4}", text)
                        if m:
                            y = int(m.group())
                            if 1800 <= y <= 2050:
                                year = y
                    elif "Балкон" in text:
                        if "нет" in text.lower():
                            balcony = "нет"
                        elif "есть" in text.lower() or re.search(r"\d+", text.lower()):
                            balcony = "есть"
                        else:
                            parts = text.split(":")
                            if len(parts) > 1:
                                balcony = parts[1].strip()
                    elif "Этаж" in text:
                        fm = re.search(re_floor, text)
                        if fm:
                            floor = fm.group(0)

        except Exception as e:
            logging.warning(f"Ошибка Selenium в блоке параметров: {e}, пробуем BS fallback")
            soup = BeautifulSoup(driver.page_source, "html.parser")
            param_items = soup.find_all("li", class_=re.compile(r"params-paramsList__item"))
            for li_ in param_items:
                text = li_.get_text(strip=True)
                # ... аналогично парсим

        # Формируем итог:
        result = {
            "title": title,          # "1-к. квартира, 34 м², 6/24 эт. ..."
            "price": price,          # "7299000"
            "area": area,            # float (e.g. 34.5)
            "year": year,            # int (e.g. 1975)
            "balcony": balcony,      # "есть" / "нет" / ...
            "floor": floor,          # "6 из 24" / "6/24" / ...
            "address": address,      # e.g. "Планерная ул., 97к2"
            "district": district,    # e.g. "р-н Приморский"
            "url": url               # сам переданный аргумент
        }
        return result

    except Exception as e:
        logging.exception(f"Ошибка парсинга Avito для {url}: {e}")
        return None

    finally:
        if driver:
            driver.quit()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Отправь ссылку на Avito или CIAN, и я добавлю её в таблицу 📄"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    url = user_message
    chat_id = update.message.chat_id

    logging.info(f"Бот получил ссылку: {user_message}")
    await update.message.reply_text("📩 Ссылка получена! Сейчас обрабатываю…")

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

    # Если какого-то поля нет, подставим заглушку
    floor_value = data.get("floor", "Не указано")

    message = (
        f"✅ Добавлено в таблицу!\n"
        f"📝 Название: {data.get('title', 'Не найдено')}\n"
        f"🏡 Адрес: {data.get('address', 'Не найдено')}\n"
        f"📍 Район: {data.get('district', 'Не найдено')}\n"
        f"📐 Площадь: {data.get('area', 'Не найдено')} м²\n"
        f"🕰 Год: {data.get('year', 'Не указан')}\n"
        f"💰 Цена: {data.get('price', 'Не указана')} ₽\n"
        f"🪟 Балкон: {data.get('balcony', 'Не указан')}\n"
        f"🏢 Этаж: {floor_value}\n"
    )

    await update.message.reply_text(message)

    try:
        sheet.append_row([
            data.get("title", ""),
            data.get("address", ""),
            data.get("district", ""),
            data.get("area", ""),
            data.get("year", ""),
            data.get("price", ""),
            data.get("url", ""),
            data.get("balcony", ""),
            floor_value,
            ""  # место под комментарий, если нужно
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
