import logging
import re
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright
from gspread_formatting import *
import os
import json

# Логирование
log_path = os.path.join(os.path.dirname(__file__), "bot.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
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
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OiUKuuJhHXNmTr-KWYdVl7UapIgAbDuuf9w34hbQNFU/edit#gid=0"

# Определяем заголовки таблицы
headers = ["Название", "Адрес", "Район", "Площадь", "Год", "Цена", "Балкон", "Этаж", "Комментарии"]
if sheet.row_values(1) != headers:
    sheet.update(values=[headers], range_name="A1:I1")

    # Форматирование заголовков
    header_format = CellFormat(
        backgroundColor=Color(0.2, 0.6, 0.8),  # Голубой фон
        textFormat=TextFormat(bold=True, fontSize=12, foregroundColor=Color(1, 1, 1)),  # Белый жирный текст
        horizontalAlignment="CENTER",
        verticalAlignment="MIDDLE",
        wrapStrategy="WRAP"
    )
    format_cell_range(sheet, "A1:I1", header_format)

    # Настройка ширины столбцов
    set_column_width(sheet, "A", 200)  # Название
    set_column_width(sheet, "B", 250)  # Адрес
    set_column_width(sheet, "C", 150)  # Район
    set_column_width(sheet, "D", 80)   # Площадь
    set_column_width(sheet, "E", 80)   # Год
    set_column_width(sheet, "F", 100)  # Цена
    set_column_width(sheet, "G", 100)  # Балкон
    set_column_width(sheet, "H", 80)   # Этаж
    set_column_width(sheet, "I", 150)  # Комментарии

BOT_TOKEN = "7645593337:AAHWs1_kIdpUBZkdQxKd_OcN9IEzTC7umVs"

# Inline-кнопки
inline_keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton("Начать", callback_data="start")],
    [InlineKeyboardButton("Показать таблицу", callback_data="table")]
])

async def parse_cian_playwright(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        
        logging.info(f"Загружаю начальную страницу ЦИАН...")
        await page.goto("https://spb.cian.ru", wait_until="domcontentloaded")
        try:
            with open("cookies_cian.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
                await page.context.add_cookies(cookies)
            logging.info("✅ Куки подставлены в браузер")
        except Exception as e:
            logging.warning(f"❌ Не удалось загрузить куки: {e}")

        logging.info(f"Загружаю страницу: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector("h1[class*='--title--']", timeout=30000)

        logging.info("Страница загружена, начинаю парсинг...")
        soup = BeautifulSoup(await page.content(), "html.parser")

        def extract_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        title = extract_text("h1[class*='--title--']")
        price_raw = extract_text('[data-testid="price-amount"]')
        price = re.sub(r"[^\d]", "", price_raw or "")

        address_parts = soup.select('a[data-name="AddressItem"]')
        address_texts = [a.get_text(strip=True) for a in address_parts]
        address = None
        for i, part in enumerate(address_texts):
            if "ул." in part or "пр." in part or "наб." in part:
                if i + 1 < len(address_texts) and re.search(r"\d", address_texts[i + 1]):
                    address = f"{part}, {address_texts[i + 1]}"
                else:
                    address = part
                break
        district = next((x for x in address_texts if "р-н" in x), None)

        area = year = balcony = floor = None
        info_divs = soup.select("div[class*='a10a3f92e9--text']")
        for div in info_divs:
            spans = div.find_all("span")
            if len(spans) >= 2:
                label = spans[0].text.strip()
                value = spans[1].text.strip()
                if "Общая площадь" in label:
                    area_text = value.replace("\xa0", " ").replace("м²", "").strip()
                    area = float(area_text.replace(",", "."))
                elif "Этаж" in label:
                    floor = value
                elif "Год" in label and re.match(r"^\d{4}$", value):
                    y_val = int(value)
                    if 1800 <= y_val <= 2100:
                        year = y_val
                elif "балкон" in label.lower():
                    balcony = value

        await browser.close()
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
            "source": "CIAN-Playwright"
        }

async def parse_avito_playwright(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        
        # Загружаем главную страницу Avito для установки куки
        logging.info("Загружаю начальную страницу Avito...")
        await page.goto("https://www.avito.ru", wait_until="domcontentloaded")
        try:
            with open("cookies_avito.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
                for cookie in cookies:
                    if "domain" in cookie and not ("avito.ru" in cookie["domain"]):
                        continue
                    await page.context.add_cookies([cookie])
            logging.info("✅ Куки подставлены в браузер для Avito")
        except Exception as e:
            logging.warning(f"❌ Не удалось загрузить куки для Avito: {e}")

        # Переходим на страницу объявления
        logging.info(f"Загружаю страницу: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        try:
            await page.wait_for_selector("[class^='style-item-address__string']", timeout=30000)
        except Exception as e:
            logging.warning(f"Адрес не найден за 30 секунд, пробую заголовок: {e}")
            await page.wait_for_selector("h1", timeout=10000)

        logging.info("Страница загружена, начинаю парсинг...")
        soup = BeautifulSoup(await page.content(), "html.parser")

        title = None
        try:
            title = soup.select_one("h1").text.strip()
            logging.info(f"Title: {title}")
        except Exception as e:
            logging.warning(f"Не удалось получить title: {e}")

        price = None
        try:
            price = soup.select_one('[itemprop="price"]').get("content")
            logging.info(f"Price: {price}")
        except:
            try:
                price_raw = soup.select_one(".js-item-price").text.strip()
                price = re.sub(r"[^\d]", "", price_raw)
                logging.info(f"Price (fallback): {price}")
            except Exception as e:
                logging.warning(f"Не удалось получить цену: {e}")

        address = None
        district = None
        try:
            address_el = soup.select_one("[class^='style-item-address__string']")
            if address_el:
                address = address_el.text.strip()
                if address.startswith("Санкт-Петербург, "):
                    address = address.replace("Санкт-Петербург, ", "").strip()
                parts = address.split(", ")
                if len(parts) > 1:
                    district = next((part for part in parts if "р-н" in part), None)
            logging.info(f"Address: {address}, District: {district}")
        except Exception as e:
            logging.warning(f"Адрес не найден: {e}")

        area = year = balcony = floor = None
        try:
            params = soup.select("li[class*='params-paramsList__item']")
            for param in params:
                text = param.text.strip()
                logging.info(f"Параметр: {text}")
                if "Общая площадь" in text:
                    match = re.search(r"\d+(?:[.,]\d+)?", text)
                    if match:
                        area = float(match.group().replace(",", "."))
                elif "Год постройки" in text or "Год сдачи" in text:
                    match = re.search(r"\d{4}", text)
                    if match and 1800 <= int(match.group()) <= 2050:
                        year = int(match.group())
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
                elif "Этаж" in text:
                    match = re.search(r"(\d+\s*из\s*\d+)|(\d+/\d+)", text)
                    if match:
                        floor = match.group()
            logging.info(f"Area: {area}, Year: {year}, Balcony: {balcony}, Floor: {floor}")
        except Exception as e:
            logging.warning(f"Ошибка парсинга параметров: {e}")

        await browser.close()
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
            "source": "Avito-Playwright"
        }

def is_duplicate_link_or_address(sheet, url, address):
    all_rows = sheet.get_all_values()
    for row in all_rows:
        if len(row) > 1 and (url in row[0] or (address and address in row[1])):
            return True
    return False

async def table_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    values = sheet.get_all_values()
    if not values:
        await update.message.reply_text("Таблица пуста!", reply_markup=inline_keyboard)
        return

    table_text = ["<b>Последние записи в таблице:</b>"]
    start_row = max(1, len(values) - 4) if "full" not in args else 1
    for row in values[start_row-1:]:
        formatted_row = (
            f"Назв: {row[0][:15]:<15} | "
            f"Адр: {row[1]:<20} | "
            f"Р-н: {row[2]:<15} | "
            f"Пл: {str(row[3]):<6} | "
            f"Год: {str(row[4]):<6} | "
            f"Цена: {str(row[5]):<10} | "
            f"Бал: {str(row[6]):<10} | "
            f"Эт: {str(row[7]):<10}"
        )
        table_text.append(f"<pre>{formatted_row}</pre>")
    if "full" not in args:
        table_text.append("<i>Для полной таблицы используйте /table full</i>")
    await update.message.reply_text("\n".join(table_text), parse_mode="HTML", reply_markup=inline_keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_message = (
        f"Привет! Отправь ссылку на Avito или CIAN\n"
        f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>"
    )
    await update.message.reply_text(welcome_message, parse_mode="HTML", reply_markup=inline_keyboard)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "start":
        welcome_message = (
            f"Привет! Отправь ссылку на Avito или CIAN\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>"
        )
        await query.message.reply_text(welcome_message, parse_mode="HTML", reply_markup=inline_keyboard)
    elif query.data == "table":
        await table_command(update, context)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    logging.info(f"Бот получил ссылку: {url}")
    await update.message.reply_text(
        f"📩 Ссылка получена! Сейчас обрабатываю…\n"
        f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
        parse_mode="HTML",
        reply_markup=inline_keyboard
    )

    if "avito.ru" in url:
        data = await parse_avito_playwright(url)
    elif "cian.ru" in url:
        data = await parse_cian_playwright(url)
    else:
        await update.message.reply_text(
            f"Это не ссылка на Avito или CIAN\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
            parse_mode="HTML",
            reply_markup=inline_keyboard
        )
        return

    if not data:
        await update.message.reply_text(
            f"❌ Не удалось обработать ссылку.\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
            parse_mode="HTML",
            reply_markup=inline_keyboard
        )
        return

    if is_duplicate_link_or_address(sheet, data["url"], data["address"]):
        await update.message.reply_text(
            f"⚠️ Это объявление или адрес уже есть в таблице!\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
            parse_mode="HTML",
            reply_markup=inline_keyboard
        )
        return

    message = (
        f"✅ Добавлено в таблицу!\n"
        f"📝 Название: {data.get('title', 'Не найдено')}\n"
        f"🏡 Адрес: {data.get('address', 'Не найдено')}\n"
        f"📍 Район: {data.get('district', 'Не найдено')}\n"
        f"📐 Площадь: {data.get('area', 'Не найдено')} м²\n"
        f"🕰 Год: {data.get('year', 'Не указан')}\n"
        f"💰 Цена: {data.get('price', 'Не указана')} ₽\n"
        f"🪟 Балкон: {data.get('balcony', 'Не указан')}\n"
        f"🏢 Этаж: {data.get('floor', 'Не указан')}\n"
        f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>"
    )
    await update.message.reply_text(message, parse_mode="HTML", reply_markup=inline_keyboard)

    hyperlink_formula = f'=HYPERLINK("{data.get("url", "")}", "{data.get("title", "Без названия")}")'
    try:
        # Добавляем новую строку
        sheet.append_row(
            [
                hyperlink_formula,
                data.get("address", ""),
                data.get("district", ""),
                data.get("area", ""),
                data.get("year", ""),
                data.get("price", ""),
                data.get("balcony", ""),
                data.get("floor", ""),
                ""
            ],
            value_input_option='USER_ENTERED'
        )

        # Форматирование новой строки
        row_count = len(sheet.get_all_values())
        if row_count > 1:  # Пропускаем заголовок
            row_range = f"A{row_count}:I{row_count}"
            # Чередуем цвета фона для строк
            background_color = Color(0.9, 0.9, 0.9) if row_count % 2 == 0 else Color(1, 1, 1)
            row_format = CellFormat(
                backgroundColor=background_color,
                textFormat=TextFormat(fontSize=10),
                horizontalAlignment="LEFT",
                verticalAlignment="MIDDLE",
                wrapStrategy="WRAP"
            )
            format_cell_range(sheet, row_range, row_format)

    except Exception as e:
        logging.exception(f"Ошибка записи в таблицу для {url}: {e}")
        await update.message.reply_text(
            f"Не удалось записать данные в Google Таблицу :(\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
            parse_mode="HTML",
            reply_markup=inline_keyboard
        )

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("table", table_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    logging.info("Бот запущен и готов к работе!")
    app.run_polling()
    logging.info("Бот завершил работу (это не должно произойти в норме)")

if __name__ == "__main__":
    main()