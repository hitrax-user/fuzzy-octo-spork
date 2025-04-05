
import logging
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def parse_cian(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)

    driver = None
    source = "Selenium"
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        title = address = price = district = balcony = None
        area = year = None

        try:
            title = driver.find_element(By.CSS_SELECTOR, '[data-mark="title"]').text.strip()
        except Exception as e:
            logging.warning(f"Не удалось получить title через Selenium: {e}")

        try:
            address = driver.find_element(By.CSS_SELECTOR, '[data-mark="address"]').text.strip()
        except Exception as e:
            logging.warning(f"Не удалось получить address через Selenium: {e}")

        try:
            price_raw = driver.find_element(By.CSS_SELECTOR, '[data-mark="MainPrice"]').text
            price = re.sub(r"[^\d]", "", price_raw)
        except Exception as e:
            logging.warning(f"Не удалось получить цену через Selenium: {e}")

        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.a10a3f92e9--item--_ipjW")))
            params = driver.find_elements(By.CSS_SELECTOR, "li.a10a3f92e9--item--_ipjW")
        except Exception as e:
            logging.warning(f"Ошибка ожидания параметров: {e}")
            params = []

        if params:
            for item in params:
                text = item.text
                if "м²" in text and not area:
                    match = re.search(r"\d+[\.,]?\d*", text)
                    if match:
                        area = float(match.group().replace(",", "."))
                elif "год" in text.lower() and not year:
                    match = re.search(r"\d{4}", text)
                    if match:
                        y = int(match.group())
                        if 1800 <= y <= 2025:
                            year = y
                elif "балкон" in text.lower() and not balcony:
                    if "нет" in text.lower():
                        balcony = "нет"
                    elif "есть" in text.lower() or re.search(r"\d+", text):
                        balcony = "есть"
                    else:
                        balcony = text.split(":")[-1].strip()
        else:
            source = "BS"
            soup = BeautifulSoup(driver.page_source, "html.parser")
            bs_items = soup.find_all("li", class_=re.compile("a10a3f92e9--item"))
            for item in bs_items:
                text = item.get_text(strip=True)
                logging.info(f"[BS] Параметр: {text}")
                if "м²" in text and not area:
                    match = re.search(r"\d+[\.,]?\d*", text)
                    if match:
                        area = float(match.group().replace(",", "."))
                elif "год" in text.lower() and not year:
                    match = re.search(r"\d{4}", text)
                    if match:
                        y = int(match.group())
                        if 1800 <= y <= 2025:
                            year = y
                elif "балкон" in text.lower() and not balcony:
                    if "нет" in text.lower():
                        balcony = "нет"
                    elif "есть" in text.lower() or re.search(r"\d+", text):
                        balcony = "есть"
                    else:
                        balcony = text.split(":")[-1].strip()

        try:
            district_elem = driver.find_element(By.CSS_SELECTOR, '[data-mark="district"]')
            district = district_elem.text.strip()
        except:
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
            "balcony": balcony,
            "source": source
        }

    except Exception as e:
        logging.exception(f"Ошибка парсинга CIAN для {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()
