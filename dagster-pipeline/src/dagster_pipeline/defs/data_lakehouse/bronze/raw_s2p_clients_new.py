# defs/data_lakehouse/bronze/raw_s2p_clients_new.py
"""
Bronze: raw extraction of clients from Spot2 Platform (MySQL).
Uses ORDER BY id, created_at DESC so downstream (silver/gold) row order matches
conversation_analysis clients.sql for consistent lead_id when multiple clients share a phone.
"""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

CLIENTS_QUERY = "SELECT * FROM clients ORDER BY id, created_at DESC"

raw_s2p_clients_new = make_bronze_asset(
    "mysql_prod",
    query=CLIENTS_QUERY,
    asset_name="raw_s2p_clients_new",
    description="Bronze: raw extraction of clients from Spot2 Platform (MySQL), ordered by id, created_at DESC.",
    partitioned=False,
)
