"""Shared fixtures.

Every fixture is offline. The simulator's whole value is that it can be tested without
a warehouse, so nothing here touches the network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from trade_sim.config import SimulatorSettings
from trade_sim.generator import TradeBook, TradeGenerator


@pytest.fixture
def sim_settings(tmp_path: Path) -> SimulatorSettings:
    """Deterministic settings pointed at a throwaway directory."""
    return SimulatorSettings(
        seed=1234,
        output_dir=tmp_path / "landing",
        state_dir=tmp_path / "state",
        error_rate=0.0,
        amend_rate=0.0,
        cancel_rate=0.0,
        replace_rate=0.0,
        stale_version_rate=0.0,
        near_maturity_rate=0.0,
        compress=False,
    )


@pytest.fixture
def clean_generator(sim_settings: SimulatorSettings) -> TradeGenerator:
    """Generator that only ever produces valid, brand-new trades."""
    return TradeGenerator(sim_settings, book=TradeBook(path=sim_settings.state_dir / "book.json"))


@pytest.fixture
def faulty_generator(sim_settings: SimulatorSettings) -> TradeGenerator:
    """Generator where every single event carries an injected fault."""
    settings = sim_settings.model_copy(update={"error_rate": 1.0})
    return TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
