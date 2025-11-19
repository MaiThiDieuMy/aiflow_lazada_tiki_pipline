import requests
import time
import logging
from typing import List, Dict, Optional


def _calculate_discount(price: Optional[int], original_price: Optional[int]) -> Optional[float]:
    if not price or not original_price or original_price <= 0:
        return None
    if price >= original_price:
        return 0.0
    return round((original_price - price) * 100.0 / original_price, 2)


def extract_tiki_data_extended(category: int, url_key: str, pages: int = 5) -> List[Dict]:
    logging.info(f"Tiki-extended | category={category} url_key={url_key} pages={pages}")
    base_api_url = (
        "https://tiki.vn/api/personalish/v1/blocks/listings?"
        "limit=40&include=advertisement&aggregations=2&version=home-persionalized&"
        f"urlKey={url_key}&category={category}&page={{page_number}}"
    )
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'application/json',
    }
    products: List[Dict] = []
    for page in range(1, pages + 1):
        api_url = base_api_url.format(page_number=page)
        try:
            resp = requests.get(api_url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.warning(f"Tiki-extended | Lỗi gọi API trang {page}: {e}")
            break
        items = data.get('data', [])
        if not isinstance(items, list) or not items:
            logging.info("Tiki-extended | Hết dữ liệu hoặc cấu trúc thay đổi.")
            break
        for it in items:
            name = it.get('name')
            price = it.get('price')
            if not name or price is None:
                continue
            try:
                price_int = int(float(price))
            except Exception:
                continue
            original_price = it.get('original_price')
            try:
                original_price_int = int(float(original_price)) if original_price else None
            except Exception:
                original_price_int = None
            sold_info = it.get('quantity_sold') or {}
            sold_count = 0
            if isinstance(sold_info, dict):
                sold_count = int(sold_info.get('value') or 0)
            rating = it.get('rating_average') or 0
            review_count = it.get('review_count') or 0
            thumbnail = it.get('thumbnail_url') or ""
            url_path = it.get('url_path') or ""
            product_url = f"https://tiki.vn{url_path}" if url_path else ""
            products.append({
                'name': name,
                'price': price_int,
                'original_price': original_price_int,
                'discount_percentage': _calculate_discount(price_int, original_price_int),
                'sold_count': sold_count,
                'rating': float(rating) if rating else 0.0,
                'review_count': int(review_count),
                'image_url': thumbnail,
                'product_url': product_url,
                'source': 'Tiki',
            })
        time.sleep(1)
    logging.info(f"Tiki-extended | Thu thập {len(products)} sản phẩm")
    return products


def run_tiki_etl_extended(category: int, url_key: str, pages: int = 5) -> List[Dict]:
    return extract_tiki_data_extended(category=category, url_key=url_key, pages=pages)


