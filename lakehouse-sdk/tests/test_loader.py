"""
Tests for Lakehouse SDK
"""
import pytest
from lakehouse_sdk import (
    load_all_assets,
    load_asset,
    create_test_context,
    ValidationRule,
    CommonRules,
)


def test_load_all_assets():
    """Test that assets can be discovered and loaded."""
    assets = load_all_assets()
    
    assert len(assets) > 0, "Should discover at least one asset"
    assert isinstance(assets, dict), "Should return a dictionary"
    
    # Check for expected bronze assets
    assert any(name.startswith("raw_") for name in assets.keys()), \
        "Should find bronze assets starting with 'raw_'"


def test_load_specific_asset():
    """Test loading a specific asset by name."""
    asset_func = load_asset("raw_s2p_clients")
    
    assert asset_func is not None, "Should find raw_s2p_clients asset"
    # DecoratedOpFunction has a decorated_fn attribute
    assert (callable(asset_func) or hasattr(asset_func, "decorated_fn")), \
        "Asset should be callable or have decorated_fn"


def test_load_nonexistent_asset():
    """Test loading an asset that doesn't exist."""
    asset_func = load_asset("nonexistent_asset_name")
    
    assert asset_func is None, "Should return None for nonexistent asset"


def test_create_test_context():
    """Test context creation."""
    context = create_test_context("2024-12-01")
    
    assert context is not None
    assert context.partition_key == "2024-12-01"


def test_create_test_context_default():
    """Test context creation with default partition (yesterday)."""
    context = create_test_context()
    
    assert context is not None
    assert context.partition_key is not None


def test_validation_rule_creation():
    """Test creating validation rules."""
    rule = ValidationRule(
        column="id",
        rule=CommonRules.not_null(),
        description="ID should not be null"
    )
    
    assert rule.column == "id"
    assert rule.description == "ID should not be null"
    assert callable(rule.rule)


def test_common_rules_exist():
    """Test that common rules are available."""
    assert callable(CommonRules.not_null())
    assert callable(CommonRules.unique())
    assert callable(CommonRules.not_empty_string())
    assert callable(CommonRules.in_range(0, 100))
    assert callable(CommonRules.in_set({1, 2, 3}))

