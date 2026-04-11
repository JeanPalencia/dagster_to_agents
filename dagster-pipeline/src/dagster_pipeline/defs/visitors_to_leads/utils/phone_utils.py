"""Phone number normalization utilities for matching (VTL pipeline)."""
import pandas as pd
from typing import Optional

import phonenumbers
from phonenumbers import NumberParseException
from phonenumbers.phonenumberutil import COUNTRY_CODE_TO_REGION_CODE


def phone_indicator_to_region(phone_indicator) -> Optional[str]:
    """
    Map phone_indicator (e.g. '+52', '52') to phonenumbers region code (e.g. 'MX').
    Uses phonenumbers built-in COUNTRY_CODE_TO_REGION_CODE for worldwide support.
    """
    if pd.isna(phone_indicator) or not str(phone_indicator).strip():
        return None
    s = str(phone_indicator).strip().lstrip("+")
    if not s.isdigit():
        return None
    try:
        regions = COUNTRY_CODE_TO_REGION_CODE.get(int(s))
        return regions[0] if regions else None
    except (ValueError, TypeError):
        return None


def normalize_phone(num, region: Optional[str] = None) -> Optional[str]:
    """
    Normalize phone number for matching.
    Uses phonenumbers library when available for robust worldwide support,
    falls back to simple prefix removal for compatibility.
    When region is provided (e.g. 'AR', 'MX'), uses it for disambiguation.
    """
    if pd.isna(num) or num is None:
        return None

    try:
        # Avoid float representation adding extra digit (e.g. 5585647482.0 -> "55856474820")
        if isinstance(num, float) and num == int(num):
            num = int(num)
        num_str = str(num).strip()
        digits_only = ''.join(filter(str.isdigit, num_str))

        if not digits_only:
            return None

        # Strategy 1: Try with phonenumbers library
        try:
            common_regions = [
                "MX", "US", "CA", "ES", "CO", "AR", "CL", "PE", "BR",
                "GT", "CR", "PA", "SV", "HN", "NI", "DO", "EC", "PY", "UY", "BO"
            ]

            if not num_str.startswith("+"):
                phone_with_plus = "+" + digits_only
            else:
                phone_with_plus = num_str

            # When region provided (e.g. from phone_indicator), try it first
            if region is not None:
                try:
                    parsed = phonenumbers.parse(num_str, region)
                    if phonenumbers.is_possible_number(parsed):
                        national = str(parsed.national_number)
                        if len(national) >= 10:
                            return national[-10:]
                        if len(national) >= 7:
                            return national
                except NumberParseException:
                    pass

            try:
                parsed = phonenumbers.parse(phone_with_plus, None)
                if phonenumbers.is_possible_number(parsed):
                    national = str(parsed.national_number)
                    if len(national) >= 10:
                        return national[-10:]
                    elif len(national) >= 7:
                        return national
            except NumberParseException:
                pass

            for r in common_regions:
                try:
                    parsed = phonenumbers.parse(num_str, r)
                    if phonenumbers.is_possible_number(parsed):
                        national = str(parsed.national_number)
                        if len(national) >= 10:
                            return national[-10:]
                        elif len(national) >= 7:
                            return national
                except NumberParseException:
                    continue
        except ImportError:
            pass
        except Exception:
            pass

        # Strategy 2: Fallback to original logic
        num = digits_only

        if num.startswith('521'):
            num = num[3:]
        elif num.startswith('52'):
            num = num[2:]
        elif num.startswith('1') and len(num) == 11:
            num = num[1:]

        if len(num) >= 10:
            return num[-10:]
        else:
            return None

    except Exception:
        return None


def compute_client_phone_clean(client_row) -> Optional[str]:
    """Normalize client phone, using phone_indicator for region when available."""
    region = phone_indicator_to_region(client_row.get('phone_indicator'))
    return normalize_phone(client_row.get('phone_number'), region)
