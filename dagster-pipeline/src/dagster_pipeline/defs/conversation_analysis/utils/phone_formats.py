"""Phone number formatting utilities for robust matching."""


def _digits_only(phone) -> str:
    """Extract only digits from phone (string)."""
    try:
        return "".join(c for c in str(phone) if c.isdigit())
    except Exception:
        return ""


def get_canonical_phone(phone) -> str:
    """
    Return a single canonical string for the phone number so that all variants
    (+52..., 52..., last 10 digits, etc.) map to the same key. Use for groupby
    to avoid duplicate conversation_id when the same number appears with different formats.
    """
    digits = _digits_only(phone)
    if not digits:
        return str(phone).strip() or "_"
    if len(digits) >= 10:
        return "52" + digits[-10:]
    return "52" + digits


def phone_formats(phone) -> set:
    """
    Generate all possible phone number formats for robust matching.
    Includes last 10 and 9 digits for long numbers.
    Handles numbers with or without + prefix.
    
    Args:
        phone: Phone number (string or numeric)
    
    Returns:
        Set of all possible phone number formats
    """
    try:
        phone = str(int(phone))
    except:
        phone = str(phone)
    
    formats = set()
    
    # Always add original number
    formats.add(phone)
    
    # Normalize: remove + if exists for processing
    phone_clean = phone.lstrip('+')
    formats.add(phone_clean)
    
    # If more than 10 digits, add last 10 (Mexican base number)
    if len(phone_clean) > 10:
        last_10 = phone_clean[-10:]
        formats.add(last_10)
        formats.add("521" + last_10)
        formats.add("52" + last_10)
        formats.add("1" + last_10)
        formats.add("+52" + last_10)
        formats.add("+521" + last_10)
    
    # Add last 9 digits (for 9-digit or truncated numbers)
    if len(phone_clean) >= 9:
        last_9 = phone_clean[-9:]
        formats.add(last_9)
        formats.add("521" + last_9)
        formats.add("52" + last_9)
        formats.add("1" + last_9)
        formats.add("+52" + last_9)
        formats.add("+521" + last_9)
    
    # Generate variants based on prefix (without +)
    if phone_clean.startswith("521"):
        sin_521 = phone_clean[3:]
        formats.add(sin_521)
        formats.add("+" + phone_clean)
        formats.add("52" + sin_521)
        formats.add("1" + sin_521)
    elif phone_clean.startswith("52"):
        sin_52 = phone_clean[2:]
        formats.add(sin_52)
        formats.add("521" + sin_52)
        formats.add("1" + sin_52)
        formats.add("+" + phone_clean)
    elif phone_clean.startswith("1") and len(phone_clean) in (11, 12):
        sin_1 = phone_clean[1:]
        formats.add(sin_1)
        formats.add("521" + sin_1)
        formats.add("52" + sin_1)
        formats.add("+" + phone_clean)
    else:
        formats.add("521" + phone_clean)
        formats.add("52" + phone_clean)
        formats.add("1" + phone_clean)
        formats.add("+" + phone_clean)
    
    return formats

