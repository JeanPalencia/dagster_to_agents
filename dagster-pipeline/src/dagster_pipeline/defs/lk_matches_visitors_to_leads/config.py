"""Configuration for the Visitors to Leads (VTL) pipeline."""
from datetime import datetime

from dagster import Config


class VTLConfig(Config):
    """Configuration for the Visitors to Leads pipeline."""
    start_date: str = "2025-01-01"
    end_date: str = datetime.now().strftime("%Y-%m-%d")
    time_buffer_seconds: int = 60
    try_subsequent_events: bool = True
    max_time_diff_minutes: int = 1440
    deduplicate_matches: bool = True
    keep_all_client_matches: bool = True
    include_spot2_emails: bool = True
    upload_to_s3: bool = True
    save_local: bool = False
    output_format: str = "csv"
    output_dir: str = "output"
    # Prefijo opcional para nombres de tablas y rutas S3 (ej: "test_") para no sobreescribir producción en esta branch.
    # Si se define (ej. test_), la tabla será test_lk_mat_matches_visitors_to_leads.
    table_name_prefix: str = ""
