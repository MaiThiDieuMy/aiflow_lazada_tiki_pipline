from datetime import datetime
from typing import Dict, Any

# Cấu hình xoay vòng category/query theo ngày
CRAWL_ROTATION: Dict[str, Dict[str, Any]] = {
    'monday': {
        'tiki': {'category': 1789, 'url_key': 'dien-thoai-may-tinh-bang', 'pages': 5},
        'lazada': {'query': 'iphone 15', 'pages': 3},
    },
    'tuesday': {
        'tiki': {'category': 1795, 'url_key': 'laptop', 'pages': 5},
        'lazada': {'query': 'laptop gaming', 'pages': 3},
    },
    'wednesday': {
        'tiki': {'category': 1801, 'url_key': 'tai-nghe', 'pages': 5},
        'lazada': {'query': 'tai nghe bluetooth', 'pages': 3},
    },
    'thursday': {
        'tiki': {'category': 1806, 'url_key': 'dong-ho', 'pages': 5},
        'lazada': {'query': 'dong ho thong minh', 'pages': 3},
    },
    'friday': {
        'tiki': {'category': 1810, 'url_key': 'may-anh', 'pages': 5},
        'lazada': {'query': 'may anh', 'pages': 3},
    },
    'saturday': {
        'tiki': {'category': 1789, 'url_key': 'dien-thoai-may-tinh-bang', 'pages': 10},
        'lazada': {'query': 'iphone 15', 'pages': 5},
    },
    'sunday': {
        'tiki': {'category': 1795, 'url_key': 'laptop', 'pages': 10},
        'lazada': {'query': 'laptop gaming', 'pages': 5},
    },
}


def get_crawl_config_for_today() -> Dict[str, Any]:
    day_name = datetime.now().strftime('%A').lower()
    return CRAWL_ROTATION.get(day_name, CRAWL_ROTATION['monday'])
