import logging
import re
import os
import json
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright
from gspread_formatting import *

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
creds_dict = {
    "type": "service_account",
    "project_id": "marine-cable-247015",
    "project_number": "1068635987895",
    "private_key_id": "4d3a8ece5e3f168a1ad6c94071043b4f96814df0",
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY"),
    "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
    "client_id": "109191276252841949564",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/formybot%40marine-cable-247015.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)
sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1OiUKuuJhHXNmTr-KWYdVl7UapIgAbDuuf9w34hbQNFU/edit?gid=0#gid=0"
).sheet1
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OiUKuuJhHXNmTr-KWYdVl7UapIgAbDuuf9w34hbQNFU/edit?gid=0#gid=0"

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
        wrapStrategy="WRAP",
        borders=Borders(
            top=Border("SOLID", Color(0, 0, 0)),
            bottom=Border("SOLID", Color(0, 0, 0)),
            left=Border("SOLID", Color(0, 0, 0)),
            right=Border("SOLID", Color(0, 0, 0))
        )
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

    # Замораживаем первую строку (заголовки)
    set_frozen(sheet, rows=1)

BOT_TOKEN = os.getenv("BOT_TOKEN")

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
            cookies_cian = json.loads(os.getenv("COOKIES_CIAN", "[]"))
            await page.context.add_cookies(cookies_cian)
            logging.info("✅ Куки подставлены в браузер для CIAN")
        except Exception as e:
            logging.warning(f"❌ Не удалось загрузить куки для CIAN: {e}")

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
        price = re.sub(r"[^\d]", "", price_raw or "") if price_raw else ""

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
                    area = float(area_text.replace(",", ".")) if area_text else None
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
            cookies_avito = json.loads(os.getenv("COOKIES_AVITO", "[]"))
            await page.context.add_cookies(cookies_avito)
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
    logging.info("Вызвана команда table_command")
    # Проверяем, откуда вызвана команда: через сообщение или callback
    message = update.message if update.message else update.callback_query.message
    args = context.args if context.args is not None else []

    # Получаем текущую страницу из user_data или устанавливаем 0
    user_data = context.user_data
    current_page = user_data.get('table_page', 0)

    # Определяем количество записей на страницу
    records_per_page = 5

    values = sheet.get_all_values()
    if not values:
        await message.reply_text("Таблица пуста!", reply_markup=inline_keyboard)
        return

    # Вычисляем общее количество страниц
    total_records = len(values) - 1  # Исключаем заголовок
    total_pages = (total_records + records_per_page - 1) // records_per_page

    # Проверяем, запрошена ли полная таблица
    if "full" in args:
        start_row = 1
        end_row = total_records
        current_page = 0
    else:
        # Вычисляем диапазон записей для текущей страницы
        start_row = current_page * records_per_page + 1
        end_row = min(start_row + records_per_page - 1, total_records)

    # Формируем заголовок таблицы
    table_text = ["<b>📊 Записи в таблице:</b>"]
    table_text.append("┌──────────────┬────────────────────┬──────────────┬──────┬──────┬──────────┬──────────┬──────┐")
    table_text.append(
        "│ Назв         │ Адр                │ Р-н          │ Пл   │ Год  │ Цена     │ Бал      │ Эт   │"
    )
    table_text.append("├──────────────┼────────────────────┼──────────────┼──────┼──────┼──────────┼──────────┼──────┤")

    # Отображаем записи для текущей страницы
    for idx, row in enumerate(values[start_row:end_row+1], start=start_row):
        title = row[0][:12] + "..." if len(row[0]) > 12 else row[0]
        address = row[1][:16] + "..." if len(row[1]) > 16 else row[1]
        district = row[2][:12] + "..." if len(row[2]) > 12 else row[2]
        area = str(row[3])[:5] if row[3] else "N/A"
        year = str(row[4])[:4] if row[4] else "N/A"
        price = str(row[5])[:8] if row[5] else "N/A"
        balcony = str(row[6])[:8] if row[6] else "N/A"
        floor = str(row[7])[:5] if row[7] else "N/A"

        formatted_row = (
            f"│ {title:<12} │ {address:<18} │ {district:<12} │ {area:<4} │ {year:<4} │ {price:<8} │ {balcony:<8} │ {floor:<4} │"
        )
        table_text.append(formatted_row)

    table_text.append("└──────────────┴────────────────────┴──────────────┴──────┴──────┴──────────┴──────────┴──────┘")
    table_text.append(f"<i>Страница {current_page + 1} из {total_pages}</i>")

    # Формируем кнопки пагинации
    pagination_buttons = []
    if current_page > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Предыдущие 5", callback_data=f"table_prev_{current_page}"))
    if end_row < total_records:
        pagination_buttons.append(InlineKeyboardButton("Следующие 5 ➡️", callback_data=f"table_next_{current_page}"))

    # Добавляем стандартные кнопки
    buttons = [
        [InlineKeyboardButton("Начать", callback_data="start")],
        [InlineKeyboardButton("Показать таблицу", callback_data="table")]
    ]
    if pagination_buttons:
        buttons.insert(1, pagination_buttons)

    await message.reply_text("\n".join(table_text), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_message = (
        f"Привет! Отправь ссылку на Avito или CIAN\n"
        f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>"
    )
    await update.message.reply_text(welcome_message, parse_mode="HTML", reply_markup=inline_keyboard)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    logging.info(f"Callback data: {query.data}")
    if query.data == "start":
        welcome_message = (
            f"Привет! Отправь ссылку на Avito или CIAN\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>"
        )
        await query.message.reply_text(welcome_message, parse_mode="HTML", reply_markup=inline_keyboard)
    elif query.data == "table":
        await table_command(update, context)
    elif query.data.startswith("table_prev_"):
        current_page = int(query.data.split("_")[2])
        context.user_data['table_page'] = max(0, current_page - 1)
        await table_command(update, context)
    elif query.data.startswith("table_next_"):
        current_page = int(query.data.split("_")[2])
        context.user_data['table_page'] = current_page + 1
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
                wrapStrategy="WRAP",
                borders=Borders(
                    top=Border("SOLID", Color(0, 0, 0)),
                    bottom=Border("SOLID", Color(0, 0, 0)),
                    left=Border("SOLID", Color(0, 0, 0)),
                    right=Border("SOLID", Color(0, 0, 0))
                )
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