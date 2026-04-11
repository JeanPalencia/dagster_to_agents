"""Utilities for URL and spot_id extraction and processing."""
import pandas as pd
import re
from typing import Optional, Union, List


def extract_urls_from_text(text: str) -> list[str]:
    """Extract all URLs from text using regex."""
    if not text or pd.isna(text):
        return []
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    return re.findall(url_pattern, str(text))


def extract_spot_id_robust(url: str) -> Optional[str]:
    """Extract spot_id from spot2.mx URL. Handles /spots/NUMBER and /spots/.../NUMBER formats."""
    if not url or pd.isna(url):
        return None
    
    url_str = str(url)
    
    # Pattern 1: /spots/NUMBER
    pattern1 = r'/spots/(\d+)'
    match = re.search(pattern1, url_str)
    if match:
        spot_id = match.group(1)
        if spot_id.isdigit() and 0 < len(spot_id) < 10:
            return spot_id
    
    # Pattern 2: /spots/...text.../NUMBER
    pattern2 = r'/spots/[^/]+/(\d+)'
    match = re.search(pattern2, url_str)
    if match:
        spot_id = match.group(1)
        if spot_id.isdigit() and 0 < len(spot_id) < 10:
            return spot_id
    
    return None


def has_spot_url_structure(url: str) -> bool:
    """Check if URL has spot2.mx/spots/ structure."""
    if not url or pd.isna(url):
        return False
    url_str = str(url).lower()
    return '/spots/' in url_str and 'spot2.mx' in url_str


def normalize_spot_id_for_comparison(spot_id: Union[str, int, float]) -> Union[int, str]:
    """Normalize spot_id for efficient comparison. Tries int conversion, falls back to string."""
    if spot_id is None or pd.isna(spot_id):
        return None
    
    spot_id_str = str(spot_id).strip()
    if not spot_id_str:
        return None
    
    try:
        if '.' in spot_id_str:
            spot_id_str = spot_id_str.split('.')[0]
        return int(spot_id_str)
    except (ValueError, OverflowError):
        return spot_id_str


def extract_all_spot_ids_from_user_messages(conversation: dict) -> List[str]:
    """Extract all unique spot_ids from user messages in the conversation."""
    messages = conversation.get('messages', [])
    if not messages:
        return []
    
    spot_ids = set()
    for msg in messages:
        if msg.get('type') != 'user':
            continue
        message_body = msg.get('message_body', '') or ''
        urls = extract_urls_from_text(message_body)
        for url in urls:
            extracted_spot_id = extract_spot_id_robust(url)
            if extracted_spot_id:
                spot_ids.add(extracted_spot_id)
    
    return list(spot_ids)

