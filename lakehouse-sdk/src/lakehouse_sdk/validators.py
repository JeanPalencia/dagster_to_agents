"""
Data validation utilities
"""
import polars as pl
from dataclasses import dataclass
from typing import Callable, Dict, List, Union


@dataclass
class ValidationRule:
    """
    Defines a validation rule for a DataFrame.
    
    Attributes:
        column: Column name to validate
        rule: Function that takes a Polars expression and returns a boolean expression
        description: Rule description
    
    Example:
        >>> rule = ValidationRule(
        ...     "email",
        ...     lambda col: col.is_not_null(),
        ...     "Email must not be null"
        ... )
    """
    column: str
    rule: Callable[[pl.Expr], pl.Expr]
    description: str


def validate_asset(
    df: pl.DataFrame, 
    rules: List[ValidationRule]
) -> Dict[str, Union[bool, str]]:
    """
    Validates a DataFrame against a set of rules.
    
    Args:
        df: DataFrame to validate
        rules: List of ValidationRule
    
    Returns:
        Dict with results per rule. True if passes, False if fails,
        or string with error if validation couldn't be executed.
    
    Example:
        >>> rules = [
        ...     ValidationRule("id", lambda col: col.is_not_null(), "ID not null"),
        ...     ValidationRule("id", lambda col: col.is_unique(), "ID unique"),
        ... ]
        >>> results = validate_asset(df, rules)
        >>> print_validation_results(results)
    """
    results = {}
    
    for rule in rules:
        rule_key = f"{rule.column}: {rule.description}"
        try:
            # Execute the rule and check if all values pass
            result = df.select(rule.rule(pl.col(rule.column))).to_numpy().all()
            results[rule_key] = bool(result)
        except Exception as e:
            results[rule_key] = f"ERROR: {str(e)}"
    
    return results


def print_validation_results(results: Dict[str, Union[bool, str]]) -> None:
    """
    Pretty print of validation results.
    
    Args:
        results: Dict returned by validate_asset()
    
    Example:
        >>> results = validate_asset(df, rules)
        >>> print_validation_results(results)
    """
    print(f"\n{'='*60}")
    print("🔍 Validation Results")
    print(f"{'='*60}")
    
    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    errors = sum(1 for v in results.values() if isinstance(v, str))
    total = len(results)
    
    for rule, result in results.items():
        if result is True:
            print(f"✅ {rule}")
        elif result is False:
            print(f"❌ {rule}")
        else:
            print(f"⚠️  {rule}: {result}")
    
    print(f"\n{'='*60}")
    print(f"📊 Summary:")
    print(f"   ✅ Passed: {passed}/{total}")
    if failed > 0:
        print(f"   ❌ Failed: {failed}/{total}")
    if errors > 0:
        print(f"   ⚠️  Errors: {errors}/{total}")


# Common validation rules for convenience
class CommonRules:
    """
    Collection of common validation rules.
    
    Example:
        >>> from lakehouse_sdk.validators import CommonRules
        >>> rules = [
        ...     ValidationRule("id", CommonRules.not_null(), "ID not null"),
        ...     ValidationRule("id", CommonRules.unique(), "ID unique"),
        ...     ValidationRule("email", CommonRules.not_empty_string(), "Email not empty"),
        ... ]
    """
    
    @staticmethod
    def not_null() -> Callable[[pl.Expr], pl.Expr]:
        """Validate that there are no nulls."""
        return lambda col: col.is_not_null()
    
    @staticmethod
    def unique() -> Callable[[pl.Expr], pl.Expr]:
        """Validate that all values are unique."""
        return lambda col: col.is_unique()
    
    @staticmethod
    def not_empty_string() -> Callable[[pl.Expr], pl.Expr]:
        """Validate that strings are not empty."""
        return lambda col: (col.is_not_null()) & (col.str.len_chars() > 0)
    
    @staticmethod
    def in_range(min_val: float, max_val: float) -> Callable[[pl.Expr], pl.Expr]:
        """Validate that numeric values are in range."""
        return lambda col: (col >= min_val) & (col <= max_val)
    
    @staticmethod
    def in_set(valid_values: set) -> Callable[[pl.Expr], pl.Expr]:
        """Validate that values are in a specific set."""
        return lambda col: col.is_in(list(valid_values))

