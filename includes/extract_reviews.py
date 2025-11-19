import logging
import requests
from typing import List, Dict


def extract_tiki_reviews(product_url: str, limit: int = 50) -> List[Dict]:
    """
    Crawl reviews từ Tiki bằng API v2 (nếu có product_id trong URL)
    product_url dạng: https://tiki.vn/....-p{product_id}.html
    """
    import re
    m = re.search(r'-p(\d+)\.html', product_url or '')
    if not m:
        return []
    product_id = m.group(1)
    api = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&limit=20&page={{page}}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    collected: List[Dict] = []
    page = 1
    while len(collected) < limit:
        try:
            resp = requests.get(api.format(page=page), headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.warning(f"Tiki-Reviews | Lỗi gọi API page={page}: {e}")
            break
        items = (data or {}).get('data') or []
        if not items:
            break
        for it in items:
            collected.append({
                'review_id_source': str(it.get('id')),
                'rating': it.get('rating') or 0,
                'review_text': it.get('content') or '',
                'review_date': it.get('created_at') or None,
                'reviewer_name': it.get('customer_name') or 'Anonymous',
                'source': 'Tiki',
            })
            if len(collected) >= limit:
                break
        page += 1
    logging.info(f"Tiki-Reviews | Thu thập {len(collected)} review cho product_id={product_id}")
    return collected
