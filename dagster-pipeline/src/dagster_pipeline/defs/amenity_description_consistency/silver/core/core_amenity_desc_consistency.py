"""
Silver Core: Amenity-Description Consistency Analysis.

For each spot that has tagged amenities, checks whether the spot
description mentions each tagged amenity using synonym dictionaries
compiled as regex patterns. Classifies each spot into:
  - Todas mencionadas: description covers all tagged amenities
  - Omision parcial:   description covers some but not all
  - Omision total:     description covers none of the tagged amenities

MODIFIED 2026-04-12: adc_mention_rate rounding changed from 4 to 2 decimals.
Reason: Reduce precision to match reporting requirements.
Affects: gold_amenity_desc_consistency, rpt_amenity_desc_consistency (S3 + GeoSpot).

MODIFIED 2026-04-13: adc_mention_rate rounding changed from 3 to 4 decimals.
Reason: Increase precision per user request.
Affects: gold_amenity_desc_consistency, rpt_amenity_desc_consistency (S3 + GeoSpot).

MODIFIED 2026-04-13: adc_mention_rate rounding changed from 4 to 2 decimals.
Reason: Reduce precision to match reporting requirements.
Affects: gold_amenity_desc_consistency, rpt_amenity_desc_consistency (S3 + GeoSpot).

MODIFIED 2026-04-14: adc_mention_rate changed to fixed 3-decimal format.
Reason: User requested consistent 3-decimal output (0.500, 1.000, etc.) regardless of trailing zeros.
Implementation: Changed from round(rate, 4) to f-string format f"{rate:.3f}" to guarantee fixed width.
Affects: Column becomes string type; gold_amenity_desc_consistency, rpt_amenity_desc_consistency (S3 + GeoSpot).
"""
import re
from typing import Callable

import dagster as dg
import polars as pl


# ---------------------------------------------------------------------------
# Synonym dictionaries per amenity
# ---------------------------------------------------------------------------
# Each entry maps an amenity name (as it appears in bt_spot_amenities) to a
# callable that receives the lowercased description and returns True if the
# amenity concept is mentioned.
#
# Order matters: "cocina equipada" and "Planta de luz" must be checked before
# "Cocina" and "Luz" to avoid false positives from partial matches.
# ---------------------------------------------------------------------------

def _compile(*patterns: str) -> re.Pattern:
    """Compile multiple patterns into a single OR regex (case-insensitive)."""
    combined = "|".join(f"(?:{p})" for p in patterns)
    return re.compile(combined, re.IGNORECASE)


def _make_simple_checker(*patterns: str) -> Callable[[str], bool]:
    rx = _compile(*patterns)
    return lambda text: rx.search(text) is not None


def _check_bodega(text: str) -> bool:
    """Match 'bodega(s)' but not 'bodega industrial' or 'bodega comercial'."""
    _synonyms = r"\balmac[eé]n(?:es)?\b|\balmacenamiento\b"
    if re.search(r"\bbodegas?\s+(?:industrial(?:es)?|comercial(?:es)?)\b", text, re.IGNORECASE):
        clean = re.sub(r"\bbodegas?\s+(?:industrial(?:es)?|comercial(?:es)?)\b", "", text, flags=re.IGNORECASE)
        if re.search(r"\bbodegas?\b", clean, re.IGNORECASE):
            return True
        return re.search(_synonyms, text, re.IGNORECASE) is not None
    return re.search(
        r"\bbodegas?\b|" + _synonyms, text, re.IGNORECASE,
    ) is not None


def _check_luz(text: str) -> bool:
    """Match 'luz' as amenity but not 'planta de luz' or 'luz trifásica'.

    'luz natural' and 'iluminación' ARE accepted because there is no
    separate amenity for them and when they appear in descriptions of
    spots with the 'Luz' tag they refer to this amenity.
    """
    if re.search(
        r"\bsuministro el[eé]ctrico\b|\benerg[ií]a el[eé]ctrica\b|\bservicio de luz\b"
        r"|\bluz\s+natural\b|\biluminaci[oó]n\b",
        text,
        re.IGNORECASE,
    ):
        return True
    clean = text
    clean = re.sub(r"\bplantas?\s+de\s+luz\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bluz\s+trif[aá]sica\b", "", clean, flags=re.IGNORECASE)
    return re.search(r"\bluz\b", clean, re.IGNORECASE) is not None


def _check_cocina(text: str) -> bool:
    """Match 'cocina(s)' but not 'cocina equipada' or 'cocina integral'."""
    if re.search(r"\bcocinetas?\b|\bkitchenettes?\b", text, re.IGNORECASE):
        return True
    clean = text
    clean = re.sub(r"\bcocinas?\s+equipadas?\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bcocinas?\s+integral(?:es)?\b", "", clean, flags=re.IGNORECASE)
    return re.search(r"\bcocinas?\b", clean, re.IGNORECASE) is not None


AMENITY_CHECKERS: dict[str, Callable[[str], bool]] = {
    "Baños": _make_simple_checker(
        r"\bba[ñn]os?\b", r"\bsanitarios\b", r"\bwc\b",
        r"\bmedios?\s+ba[ñn]os?\b",
    ),
    "Wifi": _make_simple_checker(
        r"\bwi[-\s]?fi\b", r"\binternet\b",
    ),
    "A/C": _make_simple_checker(
        r"\ba/c\b", r"\baire\s+acondicionado\b",
        r"\bclimatizad[oa]\b", r"\bclimatizaci[oó]n\b",
    ),
    "Estacionamiento": _make_simple_checker(
        r"\bestacionamientos?\b",
        r"\bcajones?\s+de\s+estacionamiento\b",
        r"\bparking\b", r"\bcocheras?\b", r"\bgarage\b",
    ),
    "Bodega": _check_bodega,
    "Accesibilidad": _make_simple_checker(
        r"\baccesibilidad\b",
        r"\bacceso\s+para\s+discapacitados\b",
        r"\brampa\s+de\s+acceso\b",
    ),
    "Luz": _check_luz,
    "Sistema de seguridad": _make_simple_checker(
        r"\bsistema\s+de\s+seguridad\b",
        r"\bseguridad\s+24\b",
        r"\bvigilancia\b",
        r"\bc[aá]maras\s+de\s+seguridad\b",
        r"\bcircuito\s+cerrado\b",
        r"\bcctv\b",
    ),
    "Montacargas": _make_simple_checker(r"\bmontacargas\b"),
    "Pizarrón": _make_simple_checker(r"\bpizarr[oó]n(?:es)?\b"),
    "Elevador": _make_simple_checker(r"\belevador(?:es)?\b", r"\bascensor(?:es)?\b"),
    "Terraza": _make_simple_checker(
        r"\bterrazas?\b", r"\broof\s*garden\b", r"\brooftop\b",
    ),
    "Zona de limpieza": _make_simple_checker(
        r"\bzona\s+de\s+limpieza\b",
        r"\b[aá]rea\s+de\s+limpieza\b",
        r"\bcuarto\s+de\s+limpieza\b",
    ),
    "Posibilidad a dividirse": _make_simple_checker(
        r"\bdividirse\b",
        r"\bposibilidad\s+(?:a|de)\s+divisi[oó]n\b",
        r"\bposibilidad\s+(?:a|de)\s+dividir\b",
        r"\bsubdividir\b", r"\bseccionar\b",
        r"\bopci[oó]n\s+de\s+unir\b",
    ),
    "Mezzanine": _make_simple_checker(r"\bmez[sz]anin[e]?s?\b", r"\bentrepisos?\b"),
    "cocina equipada": _make_simple_checker(
        r"\bcocinas?\s+equipadas?\b", r"\bcocinas?\s+integral(?:es)?\b",
    ),
    "Planta de luz": _make_simple_checker(
        r"\bplantas?\s+de\s+luz\b", r"\bplantas?\s+el[eé]ctricas?\b",
        r"\bgenerador(?:es)?\s+el[eé]ctricos?\b", r"\bsubestaci[oó]n(?:es)?\s+el[eé]ctricas?\b",
    ),
    "Cocina": _check_cocina,
    "Tapanco": _make_simple_checker(r"\btapancos?\b"),
}


def _check_amenities_for_spot(
    description: str,
    tagged_amenities: list[str],
) -> tuple[list[str], list[str]]:
    """Return (mentioned, omitted) lists for the given tagged amenities."""
    desc_lower = description.lower()
    mentioned = []
    omitted = []
    for amenity in tagged_amenities:
        checker = AMENITY_CHECKERS.get(amenity)
        if checker is None:
            omitted.append(amenity)
            continue
        if checker(desc_lower):
            mentioned.append(amenity)
        else:
            omitted.append(amenity)
    return mentioned, omitted


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------

_CATEGORY_ALL_MENTIONED = 1
_CATEGORY_PARTIAL_OMISSION = 2
_CATEGORY_TOTAL_OMISSION = 3

_CATEGORY_LABELS = {
    _CATEGORY_ALL_MENTIONED: "All mentioned",
    _CATEGORY_PARTIAL_OMISSION: "Partial omission",
    _CATEGORY_TOTAL_OMISSION: "Total omission",
}

_OUTPUT_COLUMNS = [
    "spot_id",
    "spot_type",
    "spot_status_full",
    "spot_description",
    "adc_tagged_amenities",
    "adc_mentioned_amenities",
    "adc_omitted_amenities",
    "adc_total_tagged",
    "adc_total_mentioned",
    "adc_total_omitted",
    "adc_mention_rate",
    "adc_category_id",
    "adc_category",
]


@dg.asset(
    group_name="adc_silver",
    description=(
        "Silver Core: classifies spots by whether their description "
        "mentions their tagged amenities (all, partial, none)."
    ),
)
def core_amenity_desc_consistency(
    context: dg.AssetExecutionContext,
    adc_stg_gs_lk_spots: pl.DataFrame,
    adc_stg_gs_bt_spot_amenities: pl.DataFrame,
) -> pl.DataFrame:
    amenities_per_spot = (
        adc_stg_gs_bt_spot_amenities
        .group_by("spot_id")
        .agg(pl.col("spa_amenity_name").alias("_tagged_list"))
    )

    df = (
        adc_stg_gs_lk_spots
        .join(amenities_per_spot, on="spot_id", how="inner")
    )
    context.log.info(
        f"Spots with tagged amenities + description: {df.height:,}"
    )

    results: list[dict] = []
    for row in df.iter_rows(named=True):
        tagged = sorted(row["_tagged_list"])
        mentioned, omitted = _check_amenities_for_spot(
            row["spot_description"], tagged,
        )
        total_t = len(tagged)
        total_m = len(mentioned)
        total_o = len(omitted)
        rate = total_m / total_t if total_t > 0 else 0.0

        if total_o == 0:
            cat_id = _CATEGORY_ALL_MENTIONED
        elif total_m == 0:
            cat_id = _CATEGORY_TOTAL_OMISSION
        else:
            cat_id = _CATEGORY_PARTIAL_OMISSION

        results.append({
            "spot_id": row["spot_id"],
            "spot_type": row["spot_type"],
            "spot_status_full": row["spot_status_full"],
            "spot_description": row["spot_description"],
            "adc_tagged_amenities": ", ".join(tagged),
            "adc_mentioned_amenities": ", ".join(mentioned) if mentioned else "",
            "adc_omitted_amenities": ", ".join(omitted) if omitted else "",
            "adc_total_tagged": total_t,
            "adc_total_mentioned": total_m,
            "adc_total_omitted": total_o,
            "adc_mention_rate": f"{rate:.3f}",  # Always 3 decimals (0.500, 1.000, etc.) — string type
            "adc_category_id": cat_id,
            "adc_category": _CATEGORY_LABELS[cat_id],
        })

    df_out = pl.DataFrame(results).select(_OUTPUT_COLUMNS)
    context.log.info(f"core_amenity_desc_consistency: {df_out.height:,} rows")

    for cat_id, label in _CATEGORY_LABELS.items():
        n = df_out.filter(pl.col("adc_category_id") == cat_id).height
        pct = n / df_out.height * 100 if df_out.height > 0 else 0
        context.log.info(f"  {label}: {n:,} ({pct:.1f}%)")

    return df_out
