# dags/scripts/etl_lazada.py
import time
import logging
import re
import os
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Hàm 1: Extract (Selenium)
def extract_lazada_data(search_query="iphone 15", pages: int = 8) -> List[Dict]:
    logging.info("Bắt đầu crawl Lazada bằng Selenium...")
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
    
    # Sử dụng Chromium (theo Dockerfile)
    chromium_paths = ["/usr/bin/chromium", "/usr/bin/chromium-browser"]
    for path in chromium_paths:
        if os.path.exists(path):
            options.binary_location = path
            logging.info(f"Lazada - Sử dụng Chromium tại: {path}")
            break
    
    try:
        # Tìm chromedriver (chromium-driver trong Docker thường ở các path sau)
        chromedriver_paths = [
            "/usr/bin/chromedriver",
            "/usr/lib/chromium-browser/chromedriver",
            "/usr/lib/chromium/chromedriver"
        ]
        chromedriver_path = None
        for path in chromedriver_paths:
            if os.path.exists(path):
                chromedriver_path = path
                logging.info(f"Lazada - Tìm thấy chromedriver tại: {path}")
                break
        
        if chromedriver_path:
            service = Service(chromedriver_path)
        else:
            logging.warning("Lazada - Không tìm thấy chromedriver trong các path thông thường, thử tự động tìm")
            service = Service()
        
        driver = webdriver.Chrome(service=service, options=options)
        logging.info("Lazada - Khởi tạo ChromeDriver thành công")
    except Exception as e:
        logging.error(f"Lazada - Lỗi khởi tạo ChromeDriver: {e}")
        raise

    # Các selector (có thể thay đổi)
    PRODUCT_ITEM_SELECTOR = "div[data-qa-locator='product-item']" 
    PRODUCT_NAME_SELECTOR = "a[title]"
    PRODUCT_PRICE_SELECTOR = "span.ooOxS"
    PRODUCT_SOLD_SELECTOR = "span.sales" # Selector cho "Đã bán X"

    raw_products_data = []
    try:
        for page in range(1, max(1, pages) + 1):
            url = f"https://www.lazada.vn/catalog/?q={search_query}&page={page}"
            logging.info(f"Lazada - Tải trang {page}/{pages}: {url}")
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR))
                )
            except Exception as e:
                logging.warning(f"Lazada - Không tìm thấy sản phẩm ở trang {page}: {e}")
                continue

            # Kéo trang vài lần để lazy-load thêm item
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.2)

            product_elements = driver.find_elements(By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR)
            logging.info(f"Lazada - Trang {page} tìm thấy {len(product_elements)} sản phẩm.")

            for item in product_elements:
                name, price, sold_text = "", "", ""
                try:
                    name = item.find_element(By.CSS_SELECTOR, PRODUCT_NAME_SELECTOR).text
                except: pass
                try:
                    price = item.find_element(By.CSS_SELECTOR, PRODUCT_PRICE_SELECTOR).text
                except: pass
                try:
                    sold_text = item.find_element(By.CSS_SELECTOR, PRODUCT_SOLD_SELECTOR).text
                except: pass

                if name and price:
                    raw_products_data.append({
                        "name_raw": name,
                        "price_raw": price,
                        "sold_raw": sold_text
                    })
    finally:
        driver.quit()
    logging.info(f"Lazada - Tổng cộng thu {len(raw_products_data)} sản phẩm từ {pages} trang.")
    return raw_products_data

# Hàm 2: Transform (Logic giống Tiki)
def transform_lazada_data(data: List[Dict]) -> List[Dict]:
    logging.info(f"Bắt đầu transform {len(data)} sản phẩm Lazada...")
    transformed_products = []
    
    for product in data:
        # 1. Clean data thô
        name_clean = product['name_raw'].replace('...', '').strip()
        
        # Clean giá: '16.390.000 ₫' -> 16390000
        price_clean = 0
        try:
            price_clean = re.sub(r'[.\s₫]', '', product['price_raw'])
            price_clean = int(price_clean)
        except: pass
        
        # Clean đã bán: 'Đã bán 1.2k' -> 1200, 'Đã bán 5' -> 5
        sold_clean = 0
        try:
            sold_match = re.search(r'[\d\.]+', product['sold_raw'])
            if sold_match:
                sold_str = sold_match.group(0)
                if 'k' in product['sold_raw'].lower():
                    sold_clean = int(float(sold_str) * 1000)
                else:
                    sold_clean = int(sold_str)
        except: pass
        
        name_lower = name_clean.lower()
        
        # 2. Chuẩn hóa thương hiệu (Logic y hệt Tiki)
        brand = "Unknown"
        if "iphone" in name_lower or "macbook" in name_lower or "ipad" in name_lower:
            brand = "Apple"
        elif "samsung" in name_lower or "galaxy" in name_lower:
            brand = "Samsung"
        elif "xiaomi" in name_lower or "redmi" in name_lower:
            brand = "Xiaomi"
        elif "oppo" in name_lower:
            brand = "OPPO"
        
        # 3. Phân loại sản phẩm (Logic y hệt Tiki)
        category = "Accessory"
        if "điện thoại" in name_lower or "iphone" in name_lower:
            category = "Điện thoại"
        elif "máy tính bảng" in name_lower or "ipad" in name_lower or "galaxy tab" in name_lower:
            category = "Máy tính bảng"

        transformed_products.append({
            'name': name_clean,
            'price': price_clean,
            'sold_count': sold_clean,
            'brand': brand,
            'category': category,
            'source': 'Lazada' # Cực kỳ quan trọng
        })
            
    logging.info(f"Lazada - Transform hoàn tất! {len(transformed_products)} sản phẩm sạch.")
    return transformed_products

# Hàm 3: Hàm chính để Airflow gọi
def run_lazada_etl(search_query: str = "dien thoai", pages: int = 8) -> List[Dict]:
    raw_data = extract_lazada_data(search_query=search_query, pages=pages)
    if not raw_data:
        logging.warning("Lazada - Không có dữ liệu thô, dừng pipeline.")
        return []
    transformed_data = transform_lazada_data(raw_data)
    return transformed_data