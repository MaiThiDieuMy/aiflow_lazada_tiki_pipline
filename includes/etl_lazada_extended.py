import time
import logging
import re
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def _init_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service()
    return webdriver.Chrome(service=service, options=options)


def extract_lazada_data_extended(search_query: str, pages: int = 3) -> List[Dict]:
    driver = _init_driver()
    products: List[Dict] = []
    try:
        for page in range(1, pages + 1):
            url = f"https://www.lazada.vn/catalog/?q={search_query}&page={page}"
            driver.get(url)
            PRODUCT_ITEM_SELECTOR = "div[data-qa-locator='product-item']"
            PRODUCT_NAME_SELECTOR = "a[title]"
            PRODUCT_PRICE_SELECTOR = "span.ooOxS"
            PRODUCT_SOLD_SELECTOR = "span.sales"
            PRODUCT_LINK_SELECTOR = "a[href]"
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR))
                )
            except Exception as e:
                logging.warning(f"Lazada-extended | Không tìm thấy sản phẩm trang {page}: {e}")
                break
            items = driver.find_elements(By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR)
            for it in items:
                name, price_text, sold_text, link = "", "", "", ""
                try:
                    name = it.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR).text
                except: pass
                try:
                    price_text = it.find_element(By.CSS_SELECTOR, PRODUCT_PRICE_SELECTOR).text
                except: pass
                try:
                    sold_text = it.find_element(By.CSS_SELECTOR, PRODUCT_SOLD_SELECTOR).text
                except: pass
                try:
                    link = it.find_element(By.CSS_SELECTOR, PRODUCT_LINK_SELECTOR).get_attribute("href")
                except: pass
                if not name or not price_text:
                    continue
                # parse price
                try:
                    price_clean = re.sub(r'[^\d]', '', price_text)
                    price = int(price_clean) if price_clean else 0
                except Exception:
                    price = 0
                # parse sold
                sold = 0
                try:
                    m = re.search(r'([\d\.]+)', sold_text or "")
                    if m:
                        val = m.group(1)
                        sold = int(float(val) * 1000) if 'k' in (sold_text or "").lower() else int(float(val))
                except Exception:
                    sold = 0
                products.append({
                    'name': name,
                    'price': price,
                    'sold_count': sold,
                    'product_url': link,
                    'image_url': None,
                    'source': 'Lazada',
                })
            time.sleep(1)
    finally:
        driver.quit()
    logging.info(f"Lazada-extended | Thu thập {len(products)} sản phẩm cho query '{search_query}'")
    return products


def run_lazada_etl_extended(search_query: str, pages: int = 3) -> List[Dict]:
    return extract_lazada_data_extended(search_query=search_query, pages=pages)


