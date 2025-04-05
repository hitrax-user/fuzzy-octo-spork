
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
        except Exception as e:
            logging.warning(f"Цена не найдена: {e}")

        try:
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "params-paramsList__item--2Y20")))
            all_params = driver.find_elements(By.CLASS_NAME, "params-paramsList__item--2Y20")
            for item in all_params:
                spans = item.find_elements(By.TAG_NAME, "span")
                if len(spans) >= 2:
                    key, value = spans[0].text.strip(), spans[1].text.strip()
                    logging.info(f"Параметр найден: {key} — {value}")

                    if "Общая площадь" in key:
                        match = re.search(r"\d+,\d+|\d+", value)
                        if match:
                            area = float(match.group().replace(",", "."))

                    elif "Год постройки" in key:
                        match = re.search(r'\d{4}', value)
                        if match:
                            y = int(match.group())
                            if 1800 <= y <= 2025:
                                year = y

                    elif "Балкон" in key:
                        balcony_match = re.search(r"(есть|нет)", value.lower())
                        balcony = balcony_match.group(1) if balcony_match else value
        except Exception as e:
            logging.warning(f"Ошибка парсинга параметров: {e}")

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
