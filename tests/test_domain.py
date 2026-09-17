import pytest

from finfry_inbox.domain import (
    InvalidAmount,
    amount_to_cents,
    propose_expense,
    validate_account_recommendation,
    validate_receipt_amounts,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("12.34", 1234),
        ("42.70", 4270),
        ("100", 10000),
    ],
)
def test_amount_to_cents(value: str, expected: int) -> None:
    assert amount_to_cents(value) == expected


@pytest.mark.parametrize("value", ["12.345", "-1.00", "NaN", "not money"])
def test_amount_to_cents_rejects_invalid_values(value: str) -> None:
    with pytest.raises(InvalidAmount):
        amount_to_cents(value)


def test_expense_proposal_is_balanced_and_finfry_compatible() -> None:
    proposal = propose_expense(
        date="2026-09-14",
        merchant="Northstar Market",
        total="42.70",
        expense_account="Expenses:Food:Groceries",
        payment_account="Assets:Checking",
    )

    assert proposal.balanced
    assert proposal.as_dict() == {
        "date": "2026-09-14",
        "description": "Northstar Market",
        "postings": (
            {"account": "Expenses:Food:Groceries", "amount": 4270},
            {"account": "Assets:Checking", "amount": -4270},
        ),
    }


def test_receipt_amounts_reconcile() -> None:
    validation = validate_receipt_amounts(
        subtotal="39.54",
        tax="3.16",
        total="42.70",
        line_totals=("6.49", "8.76", "5.99", "7.85", "10.45"),
    )

    assert validation.subtotal_cents == 3954
    assert validation.tax_cents == 316
    assert validation.total_cents == 4270
    assert validation.line_items_total_cents == 3954
    assert validation.totals_reconcile
    assert validation.line_items_reconcile
    assert validation.valid


def test_receipt_validation_detects_header_mismatch() -> None:
    validation = validate_receipt_amounts(
        subtotal="39.54",
        tax="3.16",
        total="42.71",
        line_totals=("39.54",),
    )

    assert not validation.totals_reconcile
    assert validation.line_items_reconcile
    assert not validation.valid


def test_receipt_validation_detects_line_item_mismatch() -> None:
    validation = validate_receipt_amounts(
        subtotal="39.54",
        tax="3.16",
        total="42.70",
        line_totals=("39.53",),
    )

    assert validation.totals_reconcile
    assert not validation.line_items_reconcile
    assert not validation.valid


def test_existing_account_recommendation_must_use_book_account() -> None:
    validation = validate_account_recommendation(
        known_accounts=("Assets:Checking", "Expenses:Food"),
        decision="existing_account",
        existing_account="Expenses:Food",
    )

    assert validation.valid
    assert validation.account == "Expenses:Food"
    assert validation.account_action == "reuse"


def test_existing_account_recommendation_rejects_hallucinated_account() -> None:
    validation = validate_account_recommendation(
        known_accounts=("Assets:Checking", "Expenses:Food"),
        decision="existing_account",
        existing_account="Expenses:Groceries",
    )

    assert not validation.valid
    assert validation.error == "existing account is not known to this book"


def test_new_expense_account_recommendation_is_allowed_for_review() -> None:
    validation = validate_account_recommendation(
        known_accounts=("Assets:Checking", "Expenses:Food"),
        decision="new_account",
        suggested_new_account="Expenses:Pets:Veterinary",
    )

    assert validation.valid
    assert validation.account == "Expenses:Pets:Veterinary"
    assert validation.account_action == "create"


@pytest.mark.parametrize(
    "suggestion",
    ["Assets:Checking", "Expenses", "Expenses::Veterinary", ""],
)
def test_new_account_recommendation_requires_expense_hierarchy(suggestion: str) -> None:
    validation = validate_account_recommendation(
        known_accounts=("Assets:Checking", "Expenses:Food"),
        decision="new_account",
        suggested_new_account=suggestion,
    )

    assert not validation.valid
