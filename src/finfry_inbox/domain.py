"""Portable Finfry transaction proposal logic.

This module deliberately has no Spark or Databricks imports, so its invariants
can be tested locally and reused in cloud jobs.
"""

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation


class InvalidAmount(ValueError):
    """Raised when extracted money cannot be represented as integer cents."""


def amount_to_cents(value: str) -> int:
    """Convert a decimal amount to cents without binary floating-point loss."""
    try:
        amount = Decimal(value)
    except InvalidOperation as error:
        raise InvalidAmount(f"invalid monetary amount: {value!r}") from error

    if not amount.is_finite() or amount < 0:
        raise InvalidAmount(f"amount must be finite and non-negative: {value!r}")

    cents = amount * 100
    if cents != cents.to_integral_value():
        raise InvalidAmount(f"amount has more than two decimal places: {value!r}")

    return int(cents)


@dataclass(frozen=True)
class Posting:
    account: str
    amount: int


@dataclass(frozen=True)
class TransactionProposal:
    date: str
    description: str
    postings: tuple[Posting, Posting]

    @property
    def balanced(self) -> bool:
        return sum(posting.amount for posting in self.postings) == 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ReceiptAmountValidation:
    """Deterministic checks applied after probabilistic document extraction."""

    subtotal_cents: int
    tax_cents: int
    total_cents: int
    line_items_total_cents: int

    @property
    def totals_reconcile(self) -> bool:
        return self.subtotal_cents + self.tax_cents == self.total_cents

    @property
    def line_items_reconcile(self) -> bool:
        return self.line_items_total_cents == self.subtotal_cents

    @property
    def valid(self) -> bool:
        return self.totals_reconcile and self.line_items_reconcile


@dataclass(frozen=True)
class AccountRecommendationValidation:
    """A constrained account decision that is safe to present for review."""

    decision: str
    account: str | None
    account_action: str | None
    valid: bool
    error: str | None = None


def validate_account_recommendation(
    *,
    known_accounts: Sequence[str],
    decision: str,
    existing_account: str | None = None,
    suggested_new_account: str | None = None,
) -> AccountRecommendationValidation:
    """Reject hallucinated existing accounts and malformed new account names."""
    known = set(known_accounts)

    if decision == "existing_account":
        if existing_account not in known:
            return AccountRecommendationValidation(
                decision=decision,
                account=existing_account,
                account_action=None,
                valid=False,
                error="existing account is not known to this book",
            )
        if not existing_account.startswith("Expenses:"):
            return AccountRecommendationValidation(
                decision=decision,
                account=existing_account,
                account_action=None,
                valid=False,
                error="receipt categorization account must be an expense account",
            )
        return AccountRecommendationValidation(decision, existing_account, "reuse", True)

    if decision == "new_account":
        parts = suggested_new_account.split(":") if suggested_new_account else []
        well_formed = len(parts) >= 2 and parts[0] == "Expenses" and all(parts)
        if not well_formed:
            return AccountRecommendationValidation(
                decision=decision,
                account=suggested_new_account,
                account_action=None,
                valid=False,
                error="new account must be a non-empty Expenses hierarchy",
            )
        if suggested_new_account in known:
            return AccountRecommendationValidation(
                decision=decision,
                account=suggested_new_account,
                account_action=None,
                valid=False,
                error="suggested account already exists",
            )
        return AccountRecommendationValidation(decision, suggested_new_account, "create", True)

    return AccountRecommendationValidation(
        decision=decision,
        account=None,
        account_action=None,
        valid=False,
        error="recommendation is uncertain or uses an unknown decision",
    )


def validate_receipt_amounts(
    *,
    subtotal: str,
    tax: str,
    total: str,
    line_totals: Sequence[str],
) -> ReceiptAmountValidation:
    """Normalize extracted amounts to cents and verify both receipt equations."""
    return ReceiptAmountValidation(
        subtotal_cents=amount_to_cents(subtotal),
        tax_cents=amount_to_cents(tax),
        total_cents=amount_to_cents(total),
        line_items_total_cents=sum(amount_to_cents(value) for value in line_totals),
    )


def propose_expense(
    *,
    date: str,
    merchant: str,
    total: str,
    expense_account: str,
    payment_account: str,
) -> TransactionProposal:
    """Create the balanced double-entry proposal that Finfry will review."""
    cents = amount_to_cents(total)
    proposal = TransactionProposal(
        date=date,
        description=merchant.strip(),
        postings=(
            Posting(account=expense_account, amount=cents),
            Posting(account=payment_account, amount=-cents),
        ),
    )
    if not proposal.description:
        raise ValueError("merchant must not be empty")
    assert proposal.balanced
    return proposal
