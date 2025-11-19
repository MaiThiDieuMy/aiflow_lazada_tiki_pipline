# dags/scripts/etl_tiki.py
# Đây là code của bạn, được refactor thành các hàm có thể gọi
import requests
import json
import time
import logging
from typing import List, Dict

# Hàm 1: Extract
def extract_tiki_data() -> List[Dict]:
    logging.info("Bắt đầu crawl dữ liệu từ API của Tiki (5 trang)...")
    base_api_url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&include=advertisement&aggregations=2&version=home-persionalized&trackity_id=f2b5815e-421b-72c3-45c5-e1787f30c8d1&urlKey=dien-thoai-may-tinh-bang&category=1789&page={page_number}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    all_products_data = []

    for page in range(1, 6): # Crawl 5 trang
        api_url = base_api_url.format(page_number=page)
        logging.info(f"Tiki - Đang crawl trang số: {page}")
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            api_data = response.json()
        except Exception as e:
            logging.warning(f"Tiki - Lỗi khi gọi API trang {page}: {e}. Dừng.")
            break
            
        if 'data' not in api_data or not isinstance(api_data['data'], list):
            logging.warning(f"Tiki - Dữ liệu từ trang {page} không hợp lệ. Dừng.")
            break

        # Thêm dữ liệu thô vào list
        all_products_data.extend(api_data['data'])
        time.sleep(1) # Rất tốt!
            
    logging.info(f"Tiki - Crawl hoàn tất! Đã thu thập {len(all_products_data)} sản phẩm thô.")
    return all_products_data

# Hàm 2: Transform (Giống code của bạn)
def transform_tiki_data(data: List[Dict]) -> List[Dict]:
    logging.info(f"Bắt đầu transform {len(data)} sản phẩm Tiki...")
    transformed_products = []
    
    for product in data:
        product_name = product.get('name')
        product_price = product.get('price')
        sold_info = product.get('quantity_sold', {})
        sold_count = 0
        if sold_info and isinstance(sold_info, dict):
            sold_count = sold_info.get('value', 0)

        if not (product_name and product_price is not None):
            continue

        # Chuyển đổi price sang int (xử lý cả float và string)
        try:
            price_int = int(float(product_price))
        except (ValueError, TypeError):
            logging.warning(f"Tiki - Không thể chuyển đổi price '{product_price}' cho sản phẩm '{product_name}'. Bỏ qua.")
            continue

        name_lower = product_name.lower()
        
        # 1. Chuẩn hóa thương hiệu
        brand = "Unknown"
        if "iphone" in name_lower or "macbook" in name_lower or "ipad" in name_lower:
            brand = "Apple"
        elif "samsung" in name_lower or "galaxy" in name_lower:
            brand = "Samsung"
        elif "xiaomi" in name_lower or "redmi" in name_lower:
            brand = "Xiaomi"
        elif "oppo" in name_lower:
            brand = "OPPO"
            
        # 2. Phân loại sản phẩm
        category = "Accessory"
        if "điện thoại" in name_lower or "iphone" in name_lower:
            category = "Điện thoại"
        elif "máy tính bảng" in name_lower or "ipad" in name_lower or "galaxy tab" in name_lower:
            category = "Máy tính bảng"
            
        transformed_products.append({
            'name': product_name,
            'price': price_int,
            'sold_count': int(sold_count),
            'brand': brand,
            'category': category,
            'source': 'Tiki' # Cực kỳ quan trọng để gộp bảng
        })
            
    logging.info(f"Tiki - Transform hoàn tất! {len(transformed_products)} sản phẩm sạch.")
    return transformed_products

# Hàm 3: Hàm chính để Airflow gọi
def run_tiki_etl() -> List[Dict]:
    raw_data = extract_tiki_data()
    if not raw_data:
        logging.warning("Tiki - Không có dữ liệu thô, dừng pipeline.")
        return []
    transformed_data = transform_tiki_data(raw_data)
    return transformed_data