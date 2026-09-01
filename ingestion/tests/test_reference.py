"""Tests for reference data integrity.

Reference data errors are the worst class of bug in this pipeline: they do not crash,
they just quietly reject valid trades. These tests make the invariants explicit.
"""

from __future__ import annotations

import csv
import io

from trade_sim import reference as ref


class TestCounterparties:
    def test_ids_are_unique(self) -> None:
        ids = [c.counterparty_id for c in ref.COUNTERPARTIES]
        assert len(ids) == len(set(ids))

    def test_leis_are_unique_and_correctly_sized(self) -> None:
        """An LEI is exactly 20 characters. A wrong-length LEI fails regulatory reporting."""
        leis = [c.lei for c in ref.COUNTERPARTIES]
        assert len(leis) == len(set(leis))
        assert all(len(lei) == 20 for lei in leis)

    def test_there_are_both_active_and_inactive_counterparties(self) -> None:
        """RJ016 needs inactive counterparties to exist in reference data."""
        assert ref.ACTIVE_COUNTERPARTY_IDS
        assert ref.INACTIVE_COUNTERPARTY_IDS

    def test_unknown_counterparty_range_is_genuinely_unused(self) -> None:
        """The generator injects CP9000-CP9999 as unknown; none may exist for real."""
        for cp_id in ref.COUNTERPARTY_IDS:
            numeric = int(cp_id.removeprefix("CP"))
            assert numeric < 9000


class TestCurrencies:
    def test_codes_are_unique_and_three_letters(self) -> None:
        codes = [c.currency_code for c in ref.CURRENCIES]
        assert len(codes) == len(set(codes))
        assert all(len(code) == 3 and code.isupper() for code in codes)

    def test_both_deliverable_and_non_deliverable_exist(self) -> None:
        """RJ017 needs at least one of each."""
        assert ref.DELIVERABLE_CURRENCY_CODES
        assert ref.NON_DELIVERABLE_CURRENCY_CODES

    def test_invalid_tokens_are_not_real_currencies(self) -> None:
        """If a corruption token were a real code, the fault would never be detected.

        Compared after normalisation, because the rules only ever see the trimmed and
        upper-cased value. Comparing the raw token is how "usd" survived in this list:
        it is not a currency code as written, it is USD by the time any rule runs, and
        the manifest was left expecting a rejection that could not happen.
        """
        for token in ref.INVALID_CURRENCY_TOKENS:
            assert token.strip().upper() not in ref.CURRENCY_CODES

    def test_zero_decimal_currencies_are_modelled(self) -> None:
        """JPY and KRW have no minor units; assuming 2 everywhere is a real bug class."""
        zero_decimal = {c.currency_code for c in ref.CURRENCIES if c.minor_units == 0}
        assert "JPY" in zero_decimal


class TestProducts:
    def test_types_are_unique(self) -> None:
        types = [p.product_type for p in ref.PRODUCTS]
        assert len(types) == len(set(types))

    def test_tenor_ranges_are_coherent(self) -> None:
        for product in ref.PRODUCTS:
            assert product.tenor_days_min <= product.tenor_days_max
            assert product.tenor_days_min >= 1

    def test_notional_ranges_are_coherent(self) -> None:
        for product in ref.PRODUCTS:
            assert 0 < product.notional_min < product.notional_max

    def test_weights_are_positive(self) -> None:
        assert all(p.weight >= 1 for p in ref.PRODUCTS)

    def test_unsupported_tokens_are_not_real_products(self) -> None:
        for token in ref.UNSUPPORTED_PRODUCT_TOKENS:
            assert token not in ref.PRODUCT_TYPES

    def test_physically_settled_products_all_exist(self) -> None:
        assert ref.PHYSICALLY_SETTLED_PRODUCTS.issubset(set(ref.PRODUCT_TYPES))


class TestBooks:
    def test_ids_are_unique(self) -> None:
        ids = [b.book_id for b in ref.BOOKS]
        assert len(ids) == len(set(ids))

    def test_limits_are_positive(self) -> None:
        assert all(b.notional_limit > 0 for b in ref.BOOKS)

    def test_invalid_book_tokens_are_not_real(self) -> None:
        for token in ref.INVALID_BOOK_TOKENS:
            assert token.strip().upper() not in ref.BOOK_IDS


class TestDirections:
    def test_invalid_direction_tokens_survive_normalisation(self) -> None:
        """Same trap as the currency tokens: "buy" is BUY once the typed layer runs."""
        for token in ref.INVALID_DIRECTION_TOKENS:
            assert token.strip().upper() not in {"BUY", "SELL"}


class TestRejectionReasons:
    def test_codes_are_unique(self) -> None:
        codes = [r.rule_code for r in ref.REJECTION_REASONS]
        assert len(codes) == len(set(codes))

    def test_codes_follow_the_naming_convention(self) -> None:
        for reason in ref.REJECTION_REASONS:
            assert reason.rule_code.startswith("RJ")
            assert reason.rule_code[2:].isdigit()

    def test_severities_are_from_the_allowed_set(self) -> None:
        allowed = {"REJECT", "WARN", "SUPERSEDE"}
        for reason in ref.REJECTION_REASONS:
            assert reason.severity in allowed, f"{reason.rule_code} has severity {reason.severity}"

    def test_every_reason_has_actionable_remediation(self) -> None:
        """A reason code without remediation guidance just moves the problem."""
        for reason in ref.REJECTION_REASONS:
            assert len(reason.remediation) > 30, f"{reason.rule_code} remediation is too thin"

    def test_all_case_study_requirements_are_covered(self) -> None:
        """The four explicitly-stated requirements must each map to a rule."""
        refs = {r.requirement_ref for r in ref.REJECTION_REASONS}
        # R1 = reject lower version, R2 = replace same version, R3 = reject past maturity.
        # R4 (mark expired) is a state transition, not a rejection, so it has no code here.
        assert {"R1", "R2", "R3"}.issubset(refs)


class TestSeedEmission:
    def test_every_seed_row_matches_its_header_width(self) -> None:
        """A ragged CSV makes dbt seed fail with an unhelpful message."""
        for spec in ref.build_seed_specs():
            for row in spec.rows:
                assert len(row) == len(spec.header), f"{spec.filename} has a ragged row: {row}"

    def test_seeds_are_non_empty(self) -> None:
        for spec in ref.build_seed_specs():
            assert spec.rows, f"{spec.filename} would be emitted empty"

    def test_seeds_survive_a_csv_round_trip(self) -> None:
        """Descriptions contain commas; this proves quoting works."""
        for spec in ref.build_seed_specs():
            buffer = io.StringIO()
            writer = csv.writer(buffer, lineterminator="\n")
            writer.writerow(spec.header)
            writer.writerows(spec.rows)

            buffer.seek(0)
            rows = list(csv.reader(buffer))
            assert rows[0] == list(spec.header)
            assert len(rows) == len(spec.rows) + 1
            for original, parsed in zip(spec.rows, rows[1:], strict=True):
                assert [str(v) for v in original] == parsed

    def test_seed_filenames_are_unique(self) -> None:
        names = [s.filename for s in ref.build_seed_specs()]
        assert len(names) == len(set(names))
