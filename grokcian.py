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
import asyncio
from aiohttp import web

# Глобальные переменные для Playwright и браузера
_playwright = None
_browser = None

# Путь к лог-файлу
log_path = os.path.join(os.path.dirname(__file__), "bot.log")

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)

class PollingFilter(logging.Filter):
    def filter(self, record):
        if record.levelno == logging.DEBUG:
            if "No new updates found" in record.msg or "Calling Bot API endpoint `getUpdates`" in record.msg:
                return False
        return True

telegram_logger = logging.getLogger("telegram")
telegram_logger.addFilter(PollingFilter())
telegram_ext_logger = logging.getLogger("telegram.ext")
telegram_ext_logger.addFilter(PollingFilter())

logging.info("🔥 Логгер инициализирован успешно. Лог-файл: %s", log_path)

if not os.path.exists("/app/files/cian-cookie.json"):
    logging.error("Файл куки /app/files/cian-cookie.json не найден")
if not os.path.exists("/app/files/avito-cookie.json"):
    logging.error("Файл куки /app/files/avito-cookie.json не найден")

print("📂 Текущая директория запуска:", os.getcwd())
print("📄 Ожидаемый лог-файл:", log_path)

# Настройка Google Sheets
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

headers = ["Название", "Адрес", "Район", "Площадь", "Год", "Цена", "Балкон", "Этаж", "Комментарии"]
if sheet.row_values(1) != headers:
    sheet.update(values=[headers], range_name="A1:I1")
    header_format = CellFormat(
        backgroundColor=Color(0.2, 0.6, 0.8),
        textFormat=TextFormat(bold=True, fontSize=12, foregroundColor=Color(1, 1, 1)),
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
    set_column_width(sheet, "A", 200)
    set_column_width(sheet, "B", 250)
    set_column_width(sheet, "C", 150)
    set_column_width(sheet, "D", 80)
    set_column_width(sheet, "E", 80)
    set_column_width(sheet, "F", 100)
    set_column_width(sheet, "G", 100)
    set_column_width(sheet, "H", 80)
    set_column_width(sheet, "I", 150)
    set_frozen(sheet, rows=1)

BOT_TOKEN = os.getenv("BOT_TOKEN")

inline_keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton("Начать", callback_data="start")],
    [InlineKeyboardButton("Показать таблицу", callback_data="table")]
])

async def get_or_launch_browser():
    global _playwright, _browser
    if _browser is None or not _browser.is_connected():
        logging.debug("Запуск нового браузера через Playwright")
        try:
            if _playwright is None:
                _playwright = await async_playwright().start()
            _browser = await _playwright.chromium.launch(headless=True)
            logging.info("Браузер успешно запущен")
        except Exception as e:
            logging.error(f"Ошибка запуска браузера: {e}")
            _browser = None
            return None
    return _browser

async def get_or_launch_browser():
    global _playwright, _browser
    if _browser is None or not _browser.is_connected():
        logging.debug("Запуск нового браузера через Playwright")
        try:
            if _playwright is None:
                _playwright = await async_playwright().start()
            # Добавляем --no-sandbox для обхода проблем в контейнерах
            _browser = await _playwright.chromium.launch(headless=True, args=["--no-sandbox"])
            logging.info("Браузер успешно запущен")
        except Exception as e:
            logging.error(f"Ошибка запуска браузера: {e}")
            _browser = None
            return None
    return _browser

async def parse_cian_playwright(url):
    logging.info(f"Начинаю парсинг ссылки: {url}")
    browser = await get_or_launch_browser()
    if browser is None:
        logging.error("Не удалось получить или запустить браузер")
        return None

    logging.debug("Создаю новую страницу")
    try:
        # Увеличиваем тайм-аут до 60 секунд
        page = await asyncio.wait_for(
            browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            timeout=60
        )
        logging.info("Новая страница создана")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при создании новой страницы")
        return None
    except Exception as e:
        logging.error(f"Ошибка создания страницы: {e}")
        return None

    # Остальной код остаётся без изменений
    logging.info("Загружаю начальную страницу ЦИАН...")
    try:
        await asyncio.wait_for(
            page.goto("https://spb.cian.ru", wait_until="domcontentloaded"),
            timeout=60
        )
        logging.info("Начальная страница загружена")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при загрузке начальной страницы ЦИАН")
        await page.close()
        return None
    except Exception as e:
        logging.error(f"Ошибка загрузки начальной страницы: {e}")
        await page.close()
        return None

    try:
        with open("/app/files/cian-cookie.json", "r", encoding="utf-8") as f:
            cookies_cian = json.load(f)
        await page.context.add_cookies(cookies_cian)
        logging.info("✅ Куки подставлены в браузер для CIAN")
    except Exception as e:
        logging.warning(f"❌ Не удалось загрузить куки для CIAN: {e}")

    logging.info(f"Загружаю страницу объявления: {url}")
    try:
        await asyncio.wait_for(
            page.goto(url, wait_until="domcontentloaded"),
            timeout=60
        )
        await asyncio.wait_for(
            page.wait_for_selector("h1[class*='--title--']"),
            timeout=30
        )
        logging.info("Страница объявления загружена")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при загрузке страницы объявления")
        await page.close()
        return None
    except Exception as e:
        logging.error(f"Ошибка загрузки страницы объявления: {e}")
        await page.close()
        return None

    logging.debug("Получаю HTML контент")
    html_content = await page.content()
    soup = BeautifulSoup(html_content, "html.parser")
    logging.debug("Начало HTML страницы (500 символов): %s", soup.prettify()[:500])

    def extract_text(selector):
        el = soup.select_one(selector)
        result = el.get_text(strip=True) if el else None
        logging.debug("Результат извлечения по селектору '%s': %s", selector, result)
        return result

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
            logging.debug("Найдена характеристика: %s -> %s", label, value)
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

    await page.close()
    logging.info("Парсинг завершен успешно")
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
    logging.info(f"Начинаю парсинг ссылки: {url}")
    browser = await get_or_launch_browser()
    if browser is None:
        logging.error("Не удалось получить или запустить браузер")
        return None

    logging.debug("Создаю новую страницу")
    try:
        page = await asyncio.wait_for(
            browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            timeout=30
        )
        logging.info("Новая страница создана")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при создании новой страницы")
        return None
    except Exception as e:
        logging.error(f"Ошибка создания страницы: {e}")
        return None

    logging.info("Загружаю начальную страницу Avito...")
    try:
        await asyncio.wait_for(
            page.goto("https://www.avito.ru", wait_until="domcontentloaded"),
            timeout=60
        )
        logging.info("Начальная страница загружена")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при загрузке начальной страницы Avito")
        await page.close()
        return None
    except Exception as e:
        logging.error(f"Ошибка загрузки начальной страницы: {e}")
        await page.close()
        return None

    try:
        with open("/app/files/avito-cookie.json", "r", encoding="utf-8") as f:
            cookies_avito = json.load(f)
        await page.context.add_cookies(cookies_avito)
        logging.info("✅ Куки подставлены в браузер для Avito")
    except Exception as e:
        logging.warning(f"❌ Не удалось загрузить куки для Avito: {e}")

    logging.info(f"Загружаю страницу объявления: {url}")
    try:
        await asyncio.wait_for(
            page.goto(url, wait_until="domcontentloaded"),
            timeout=60
        )
        await asyncio.wait_for(
            page.wait_for_selector("[class^='style-item-address__string']"),
            timeout=30
        )
        logging.info("Страница объявления загружена")
    except asyncio.TimeoutError:
        logging.error("Тайм-аут при загрузке страницы объявления")
        await page.close()
        return None
    except Exception as e:
        logging.error(f"Ошибка загрузки страницы объявления: {e}")
        await page.close()
        return None

    logging.debug("Получаю HTML контент")
    html_content = await page.content()
    soup = BeautifulSoup(html_content, "html.parser")

    title = soup.select_one("h1").text.strip() if soup.select_one("h1") else None
    price = soup.select_one('[itemprop="price"]').get("content") if soup.select_one('[itemprop="price"]') else None
    if not price:
        price_raw = soup.select_one(".js-item-price").text.strip() if soup.select_one(".js-item-price") else None
        price = re.sub(r"[^\d]", "", price_raw) if price_raw else None

    address_el = soup.select_one("[class^='style-item-address__string']")
    address = address_el.text.strip().replace("Санкт-Петербург, ", "") if address_el else None
    district = None
    if address:
        parts = address.split(", ")
        district = next((part for part in parts if "р-н" in part), None)

    area = year = balcony = floor = None
    params = soup.select("li[class*='params-paramsList__item']")
    for param in params:
        text = param.text.strip()
        if "Общая площадь" in text:
            match = re.search(r"\d+(?:[.,]\d+)?", text)
            area = float(match.group().replace(",", ".")) if match else None
        elif "Год постройки" in text or "Год сдачи" in text:
            match = re.search(r"\d{4}", text)
            year = int(match.group()) if match and 1800 <= int(match.group()) <= 2050 else None
        elif "Балкон" in text:
            text_low = text.lower()
            balcony = "нет" if "нет" in text_low else "есть" if "есть" in text_low or re.search(r"\d+", text_low) else None
        elif "Этаж" in text:
            match = re.search(r"(\d+\s*из\s*\d+)|(\d+/\d+)", text)
            floor = match.group() if match else None

    await page.close()
    logging.info("Парсинг завершен успешно")
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
    message = update.message if update.message else update.callback_query.message
    args = context.args if context.args is not None else []
    user_data = context.user_data
    current_page = user_data.get('table_page', 0)
    records_per_page = 5

    values = sheet.get_all_values()
    if not values:
        await message.reply_text("Таблица пуста!", reply_markup=inline_keyboard)
        return

    total_records = len(values) - 1
    total_pages = (total_records + records_per_page - 1) // records_per_page

    if "full" in args:
        start_row = 1
        end_row = total_records
        current_page = 0
    else:
        start_row = current_page * records_per_page + 1
        end_row = min(start_row + records_per_page - 1, total_records)

    table_text = ["<b>📊 Записи в таблице:</b>"]
    table_text.append("┌──────────────┬────────────────────┬──────────────┬──────┬──────┬──────────┬──────────┬──────┐")
    table_text.append("│ Назв         │ Адр                │ Р-н          │ Пл   │ Год  │ Цена     │ Бал      │ Эт   │")
    table_text.append("├──────────────┼────────────────────┼──────────────┼──────┼──────┼──────────┼──────────┼──────┤")

    for idx, row in enumerate(values[start_row:end_row+1], start=start_row):
        title = row[0][:12] + "..." if len(row[0]) > 12 else row[0]
        address = row[1][:16] + "..." if len(row[1]) > 16 else row[1]
        district = row[2][:12] + "..." if len(row[2]) > 12 else row[2]
        area = str(row[3])[:5] if row[3] else "N/A"
        year = str(row[4])[:4] if row[4] else "N/A"
        price = str(row[5])[:8] if row[5] else "N/A"
        balcony = str(row[6])[:8] if row[6] else "N/A"
        floor = str(row[7])[:5] if row[7] else "N/A"
        formatted_row = f"│ {title:<12} │ {address:<18} │ {district:<12} │ {area:<4} │ {year:<4} │ {price:<8} │ {balcony:<8} │ {floor:<4} │"
        table_text.append(formatted_row)

    table_text.append("└──────────────┴────────────────────┴──────────────┴──────┴──────┴──────────┴──────────┴──────┘")
    table_text.append(f"<i>Страница {current_page + 1} из {total_pages}</i>")

    pagination_buttons = []
    if current_page > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Предыдущие 5", callback_data=f"table_prev_{current_page}"))
    if end_row < total_records:
        pagination_buttons.append(InlineKeyboardButton("Следующие 5 ➡️", callback_data=f"table_next_{current_page}"))

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

    if not context.application.running:
        await update.message.reply_text(
            f"❌ Бот остановлен. Попробуйте позже.\n"
            f"Таблица: <a href='{SHEET_URL}'>Google Sheets</a>",
            parse_mode="HTML",
            reply_markup=inline_keyboard
        )
        return

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
            f"❌ Не удалось обработать ссылку. Проверьте логи или попробуйте позже.\n"
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
        row_count = len(sheet.get_all_values())
        if row_count > 1:
            row_range = f"A{row_count}:I{row_count}"
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

async def handle_health(request):
    return web.Response(text="OK")

async def init_health_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8000)
    await site.start()
    logging.info("Health check server started on port 8000")
    return runner

async def main():
    global _playwright, _browser
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("table", table_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))

    await init_health_server()

    await application.initialize()
    logging.info("Бот инициализирован")

    # Инициализация Playwright
    _playwright = await async_playwright().start()
    logging.info("Playwright инициализирован")

    logging.info("Бот запущен и готов к работе!")
    await application.start()
    await application.updater.start_polling()

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("Получен сигнал завершения")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        if _browser:
            await _browser.close()
        if _playwright:
            await _playwright.stop()
        logging.info("Бот остановлен")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()