"""Text normalization and conversation labeling utilities."""
import re

import pandas as pd


def normalize_text_for_matching(text: str) -> str:
    """Normalize text for pattern matching: lowercase, remove accents, special punctuation."""
    if not text or pd.isna(text):
        return ""

    try:
        text = str(text).strip().lower()

        accent_map = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u',
            'Á': 'a', 'É': 'e', 'Í': 'i', 'Ó': 'o', 'Ú': 'u', 'Ü': 'u',
            'ñ': 'n', 'Ñ': 'n'
        }
        for accented, unaccented in accent_map.items():
            text = text.replace(accented, unaccented)

        url_pattern = r'https?://[^\s]+'
        urls = re.findall(url_pattern, text)
        text_without_urls = re.sub(url_pattern, 'URL_PLACEHOLDER', text)
        text_without_urls = re.sub(r'[^\w\s]', ' ', text_without_urls)

        for url in urls:
            text_without_urls = text_without_urls.replace('URL_PLACEHOLDER', url, 1)

        text_without_urls = re.sub(r'\s+', ' ', text_without_urls)

        return text_without_urls.strip()
    except Exception:
        return ""


def label_conversation_by_first_message(first_message: str) -> str:
    """Label conversation based on first message patterns."""
    if not first_message or pd.isna(first_message):
        return "no_pattern"

    try:
        normalized = normalize_text_for_matching(str(first_message))

        if "estoy interesado en revisar mas opciones de espacios" in normalized:
            return "popup_100seg"

        if "hola me interesa este spot" in normalized:
            return "whatsapp"

        if "quisiera solicitar una consultoria personalizada" in normalized:
            return "solicitar_consultoria"

        return "no_pattern"
    except Exception:
        return "no_pattern"


def label_conversations(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'label' column to conversations DataFrame based on first message patterns."""
    df = df.copy()
    df['label'] = df['first_message'].apply(label_conversation_by_first_message)
    return df
