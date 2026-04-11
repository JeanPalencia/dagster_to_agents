# Reusable mappings for case_when (ID to label translation).
# Future improvement: replace with LEFT JOIN to master tables if they exist in bronze/silver.

STATUS_LABEL_MAP = {1: "Public", 2: "Draft", 3: "Disabled", 4: "Archived"}
STATUS_REASON_MAP = {
    1: "User", 2: "Onboarding API", 3: "Onboarding Scraping", 4: "QA",
    5: "Publisher", 6: "Occupied", 7: "Won", 8: "QA", 9: "Internal Use",
    10: "Internal", 11: "Owner", 12: "Unpublished", 13: "Outdated", 14: "Quality",
}
REPORT_REASON_MAP = {
    1: "Fotos de mala calidad", 2: "Fotos con marca de agua", 3: "Fotos en collage",
    4: "Fotos insuficientes", 5: "Error en precio de renta/venta",
    6: "Error en precio de mantenimiento", 7: "Fotos con publicidad de lonas",
}
SPOT_SECTOR_MAP = {9: "Industrial", 11: "Office", 13: "Retail", 15: "Land"}
SPOT_TYPE_MAP = {1: "Single", 2: "Complex", 3: "Subspace"}
CONTACT_SUBGROUP_MAP = {
    1: "Individual Broker", 2: "Independent Property Owners",
    3: "Institutional Developers & REITS", 4: "Legacy Broker",
    5: "Franchise Broker", 6: "SMB Developers",
}
CONTACT_CATEGORY_MAP = {1: "DI", 2: "Broker", 3: "Owner"}
SPOT_QUALITY_MAP = {1: "Pending", 2: "Reported", 3: "Verified"}
SPOT_CLASS_MAP = {1: "A+", 2: "A", 3: "B", 4: "C"}
SPOT_CONDITION_MAP = {1: "Shell condition", 2: "Conditioned", 3: "Furnished"}
SPOT_ORIGIN_MAP = {1: "Spot2", 2: "NocNok", 3: "Tokko"}
USER_INDUSTRIA_ROLE_MAP = {1: "Tenant", 2: "Broker", 4: "Landlord", 5: "Developer"}
USER_LEVEL_MAP = {1: "Gold", 2: "Platinum", 3: "Titanium"}
PRICE_AREA_MAP = {1: "Total Price", 2: "Price per SQM"}
CURRENCY_MAP = {1: "MXN", 2: "USD"}
