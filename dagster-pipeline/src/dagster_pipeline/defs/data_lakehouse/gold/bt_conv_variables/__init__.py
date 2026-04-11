"""
BT conv in the lakehouse: lakehouse-owned config and utilities; legacy logic via `vendor`.

- `config`: ConvVariablesConfig.
- `phone_formats`: phone matching without depending on the legacy package layout.
- `vendor`: re-exports current implementations from conversation_analysis (single migration surface).
"""

from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.config import ConvVariablesConfig

__all__ = ["ConvVariablesConfig"]
