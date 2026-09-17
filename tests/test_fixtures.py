import hashlib
import json
from pathlib import Path

import pytest

from finfry_inbox.domain import validate_receipt_amounts

PROJECT_ROOT = Path(__file__).parents[1]
FIXTURES = PROJECT_ROOT / "fixtures"


def load_receipt(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.mark.parametrize(
    ("name", "expected_valid"),
    [
        ("receipt_001", True),
        ("receipt_002", True),
        ("receipt_003", False),
    ],
)
def test_receipt_arithmetic_matches_demo_story(name: str, expected_valid: bool) -> None:
    receipt = load_receipt(name)

    result = validate_receipt_amounts(
        subtotal=receipt["subtotal"],
        tax=receipt["tax"],
        total=receipt["total"],
        line_totals=tuple(item["line_total"] for item in receipt["line_items"]),
    )

    assert result.valid is expected_valid


@pytest.mark.parametrize("name", ["receipt_001", "receipt_002", "receipt_003"])
def test_png_hash_is_assigned_to_expected_book(name: str) -> None:
    receipt = load_receipt(name)
    receipt_id = hashlib.sha256((FIXTURES / f"{name}.png").read_bytes()).hexdigest()
    seed_sql = (FIXTURES / "seed_reference_data.sql").read_text()

    assert f"('{receipt_id}', '{receipt['book_id']}')" in seed_sql
