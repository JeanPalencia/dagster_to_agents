"""Formatos de teléfono para matching robusto (copia lakehouse; sin depender de conversation_analysis)."""


def _digits_only(phone) -> str:
    try:
        return "".join(c for c in str(phone) if c.isdigit())
    except Exception:
        return ""


def get_canonical_phone(phone) -> str:
    digits = _digits_only(phone)
    if not digits:
        return str(phone).strip() or "_"
    if len(digits) >= 10:
        return "52" + digits[-10:]
    return "52" + digits


def phone_formats(phone) -> set:
    try:
        phone = str(int(phone))
    except Exception:
        phone = str(phone)

    formats = set()
    formats.add(phone)
    phone_clean = phone.lstrip("+")
    formats.add(phone_clean)

    if len(phone_clean) > 10:
        last_10 = phone_clean[-10:]
        formats.add(last_10)
        formats.add("521" + last_10)
        formats.add("52" + last_10)
        formats.add("1" + last_10)
        formats.add("+52" + last_10)
        formats.add("+521" + last_10)

    if len(phone_clean) >= 9:
        last_9 = phone_clean[-9:]
        formats.add(last_9)
        formats.add("521" + last_9)
        formats.add("52" + last_9)
        formats.add("1" + last_9)
        formats.add("+52" + last_9)
        formats.add("+521" + last_9)

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
