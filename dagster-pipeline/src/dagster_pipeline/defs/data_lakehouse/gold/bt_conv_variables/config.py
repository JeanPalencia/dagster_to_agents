"""BT conv configuration for the data lakehouse (lakehouse-owned).

Do not import from conversation_analysis.config: if that pipeline is removed, these values
remain the lakehouse source of truth. Keep aligned with legacy only when parity is explicitly documented.
"""
from dagster import Config


class ConvVariablesConfig(Config):
    """Config for the conv_variables flow in gold_bt_conv."""

    use_incremental_mask: bool = True
