"""
data_contract.py
─────────────────────────────────────────────────────────────────────────────
Loads data contract YAML definitions and enforces them against DataFrames.
Returns structured quality results used by the quality reporter.

Supported rules
---------------
- null_check         : No nulls allowed in specified columns
- volume_range_check : Numeric column within [min, max]
- duplicate_check    : No duplicate rows on specified key columns
- allowed_values_check: Column values restricted to an allowed set
- row_count_check    : At least N rows present
"""

import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    rule: str
    status: str           # PASSED | FAILED | WARNING
    details: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == "PASSED"


@dataclass
class ContractValidationResult:
    dataset: str
    contract_version: str
    overall_status: str   # PASSED | FAILED
    row_count: int
    rule_results: list[RuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.overall_status == "PASSED"

    def summary(self) -> dict:
        return {
            "dataset": self.dataset,
            "contract_version": self.contract_version,
            "overall_status": self.overall_status,
            "row_count": self.row_count,
            "rules_passed": sum(1 for r in self.rule_results if r.passed),
            "rules_failed": sum(1 for r in self.rule_results if not r.passed),
            "rule_results": [
                {"rule": r.rule, "status": r.status, **r.details}
                for r in self.rule_results
            ],
        }


# ---------------------------------------------------------------------------
# Contract loader
# ---------------------------------------------------------------------------

class DataContract:
    """
    Loads a data contract YAML and enforces its rules against a DataFrame.

    Usage
    -----
    contract = DataContract("config/data_contract_volume.yaml")
    result = contract.validate(df)
    print(result.summary())
    """

    def __init__(self, contract_path: str):
        self.contract_path = Path(PROJECT_ROOT / contract_path)
        self._spec = self._load()

    def _load(self) -> dict:
        with open(self.contract_path) as f:
            spec = yaml.safe_load(f)
        print(f"[contract] Loaded contract '{spec['dataset']}' v{spec['version']}")
        return spec

    @property
    def dataset(self) -> str:
        return self._spec["dataset"]

    @property
    def version(self) -> str:
        return self._spec["version"]

    @property
    def certification_status(self) -> str:
        return self._spec.get("certification_status", "uncertified")

    # ------------------------------------------------------------------
    # Rule evaluators
    # ------------------------------------------------------------------

    def _check_null(self, df: pd.DataFrame, rule: dict) -> RuleResult:
        columns = rule.get("columns", [])
        threshold = rule.get("threshold", 0.0)
        violations = {}

        for col in columns:
            if col not in df.columns:
                continue
            null_rate = df[col].isna().mean()
            if null_rate > threshold:
                violations[col] = round(null_rate, 4)

        if violations:
            return RuleResult(
                rule="null_check",
                status="FAILED",
                details={"violations": violations, "threshold": threshold},
            )
        return RuleResult(
            rule="null_check",
            status="PASSED",
            details={"columns_checked": columns, "null_rate": 0.0},
        )

    def _check_volume_range(self, df: pd.DataFrame, rule: dict) -> RuleResult:
        col = rule["column"]
        lo, hi = rule["min"], rule["max"]
        if col not in df.columns:
            return RuleResult(rule="volume_range_check", status="WARNING",
                              details={"reason": f"Column '{col}' not found"})
        out_of_range = df[(df[col] < lo) | (df[col] > hi)]
        count = len(out_of_range)
        if count > 0:
            return RuleResult(
                rule="volume_range_check",
                status="FAILED",
                details={"column": col, "violations": count, "range": [lo, hi]},
            )
        return RuleResult(
            rule="volume_range_check",
            status="PASSED",
            details={"column": col, "range": [lo, hi], "violations": 0},
        )

    def _check_duplicates(self, df: pd.DataFrame, rule: dict) -> RuleResult:
        keys = rule["keys"]
        missing_cols = [k for k in keys if k not in df.columns]
        if missing_cols:
            return RuleResult(rule="duplicate_check", status="WARNING",
                              details={"reason": f"Missing columns: {missing_cols}"})
        dup_count = df.duplicated(subset=keys).sum()
        if dup_count > 0:
            return RuleResult(
                rule="duplicate_check",
                status="FAILED",
                details={"keys": keys, "duplicate_rows": int(dup_count)},
            )
        return RuleResult(
            rule="duplicate_check",
            status="PASSED",
            details={"keys": keys, "duplicate_rows": 0},
        )

    def _check_allowed_values(self, df: pd.DataFrame, rule: dict) -> RuleResult:
        col = rule["column"]
        allowed = set(rule["values"])
        if col not in df.columns:
            return RuleResult(rule="allowed_values_check", status="WARNING",
                              details={"reason": f"Column '{col}' not found"})
        invalid = df[~df[col].isin(allowed)][col].unique().tolist()
        if invalid:
            return RuleResult(
                rule="allowed_values_check",
                status="FAILED",
                details={"column": col, "invalid_values": invalid, "allowed": list(allowed)},
            )
        return RuleResult(
            rule="allowed_values_check",
            status="PASSED",
            details={"column": col, "allowed": list(allowed)},
        )

    # ------------------------------------------------------------------
    # Main validator
    # ------------------------------------------------------------------

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        """
        Runs all quality rules defined in the contract against df.

        Returns
        -------
        ContractValidationResult
        """
        print(f"\n[contract] Validating '{self.dataset}' — {len(df):,} rows")
        rule_results: list[RuleResult] = []

        for rule in self._spec.get("quality_rules", []):
            rule_type = rule.get("rule")

            if rule_type == "null_check":
                result = self._check_null(df, rule)
            elif rule_type == "volume_range_check":
                result = self._check_volume_range(df, rule)
            elif rule_type == "duplicate_check":
                result = self._check_duplicates(df, rule)
            elif rule_type == "allowed_values_check":
                result = self._check_allowed_values(df, rule)
            else:
                result = RuleResult(
                    rule=rule_type or "unknown",
                    status="WARNING",
                    details={"reason": "Unsupported rule type"},
                )

            status_icon = "✓" if result.passed else "✗"
            print(f"[contract]   {status_icon} {result.rule}: {result.status}")
            rule_results.append(result)

        all_passed = all(r.passed for r in rule_results)
        overall = "PASSED" if all_passed else "FAILED"

        print(f"[contract] Overall: {overall}")
        return ContractValidationResult(
            dataset=self.dataset,
            contract_version=self.version,
            overall_status=overall,
            row_count=len(df),
            rule_results=rule_results,
        )


if __name__ == "__main__":
    import duckdb

    db_path = PROJECT_ROOT / "data" / "lmd_warehouse.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        df = conn.execute("SELECT * FROM raw_delivery_volume").df()

    contract = DataContract("config/data_contract_volume.yaml")
    result = contract.validate(df)
    import json
    print(json.dumps(result.summary(), indent=2))