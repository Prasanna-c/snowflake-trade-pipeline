"""Tests for the batch writer.

The interesting properties are about file *handling*, not content: a half-written file
that a consumer can see, or a double-compressed file, are both production incidents that
unit tests can prevent cheaply.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

from trade_sim.config import SimulatorSettings
from trade_sim.generator import TradeGenerator
from trade_sim.writer import BatchWriter, summarise_events


class TestFileNaming:
    def test_name_encodes_env_timestamp_ref_and_count(self, tmp_path: Path) -> None:
        writer = BatchWriter(tmp_path, env="dev")
        name = writer.file_name(datetime(2026, 6, 1, 14, 30, 5, tzinfo=UTC), "babc123", 500)
        assert name == "trades_dev_20260601T143005Z_babc123_500.ndjson.gz"

    def test_names_sort_chronologically(self, tmp_path: Path) -> None:
        """Lexicographic order must equal chronological order for stage listing to work."""
        writer = BatchWriter(tmp_path, env="dev")
        early = writer.file_name(datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC), "a", 1)
        late = writer.file_name(datetime(2026, 6, 1, 17, 0, 0, tzinfo=UTC), "b", 1)
        assert early < late

    def test_output_is_date_partitioned(self, tmp_path: Path) -> None:
        writer = BatchWriter(tmp_path, env="dev", compress=False)
        settings = SimulatorSettings(
            seed=1, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=0.0
        )
        generator = TradeGenerator(settings)

        path, _ = writer.write(
            generator.generate(5, as_of=date(2026, 6, 1)),
            batch_ref="test",
            when=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        )
        assert path.parent.name == "ingest_date=2026-06-01"


class TestFileIntegrity:
    def test_no_partial_file_is_left_behind(self, tmp_path: Path) -> None:
        """A `.partial` file surviving the write means consumers can read torn data."""
        settings = SimulatorSettings(
            seed=1, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=0.0
        )
        generator = TradeGenerator(settings)
        writer = BatchWriter(tmp_path, env="dev")

        writer.write(generator.generate(20, as_of=date(2026, 6, 1)), batch_ref="test")
        assert not list(tmp_path.rglob("*.partial"))

    def test_gzip_output_is_single_compressed_and_readable(self, tmp_path: Path) -> None:
        """Double compression is the classic PUT AUTO_COMPRESS mistake."""
        settings = SimulatorSettings(
            seed=1, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=0.0
        )
        generator = TradeGenerator(settings)
        writer = BatchWriter(tmp_path, env="dev", compress=True)

        path, events = writer.write(
            generator.generate(10, as_of=date(2026, 6, 1)), batch_ref="test"
        )

        with gzip.open(path, "rt", encoding="utf-8") as handle:
            lines = [line for line in handle.read().splitlines() if line]
        assert len(lines) == len(events)
        for line in lines:
            json.loads(line)

    def test_gzip_output_is_byte_deterministic(self, tmp_path: Path) -> None:
        """Reproducibility: the same input must produce the same bytes."""
        settings = SimulatorSettings(
            seed=7, output_dir=tmp_path, state_dir=tmp_path / "s1", error_rate=0.0
        )
        writer = BatchWriter(tmp_path / "a", env="dev", compress=True)
        path_a, _ = writer.write(
            TradeGenerator(settings).generate(10, as_of=date(2026, 6, 1)),
            batch_ref="test",
            when=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        )

        settings_b = settings.model_copy(update={"state_dir": tmp_path / "s2"})
        writer_b = BatchWriter(tmp_path / "b", env="dev", compress=True)
        path_b, _ = writer_b.write(
            TradeGenerator(settings_b).generate(10, as_of=date(2026, 6, 1)),
            batch_ref="test",
            when=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        )

        assert path_a.read_bytes() == path_b.read_bytes()

    def test_one_json_object_per_line(self, tmp_path: Path) -> None:
        """NDJSON, not a JSON array -- STRIP_OUTER_ARRAY is FALSE in the file format."""
        settings = SimulatorSettings(
            seed=1, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=0.0
        )
        writer = BatchWriter(tmp_path, env="dev", compress=False)
        path, events = writer.write(
            TradeGenerator(settings).generate(15, as_of=date(2026, 6, 1)), batch_ref="test"
        )

        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
        assert len(lines) == len(events)
        assert not path.read_text(encoding="utf-8").lstrip().startswith("[")

    def test_unparseable_payloads_are_written_verbatim(self, tmp_path: Path) -> None:
        """The corruption must survive to Snowflake, not be re-serialised into validity."""
        settings = SimulatorSettings(
            seed=3, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=1.0, compress=False
        )
        generator = TradeGenerator(settings)
        writer = BatchWriter(tmp_path, env="dev", compress=False)

        path, events = writer.write(
            generator.generate(400, as_of=date(2026, 6, 1)), batch_ref="test"
        )
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]

        bad_lines = 0
        for line in lines:
            try:
                json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1

        expected_bad = sum(1 for e in events if isinstance(e.payload, str))
        assert bad_lines == expected_bad
        assert bad_lines > 0, (
            "seed produced no unparseable payloads; the test is not exercising anything"
        )


class TestManifestWriting:
    def test_manifest_is_written_outside_the_ingest_partition(self, tmp_path: Path) -> None:
        """A manifest inside the ingest tree could be picked up by COPY as trade data."""
        settings = SimulatorSettings(
            seed=1, output_dir=tmp_path / "landing", state_dir=tmp_path / "s", error_rate=0.0
        )
        generator = TradeGenerator(settings)
        writer = BatchWriter(settings.output_dir, env="dev", compress=False)

        path, events = writer.write(generator.generate(5, as_of=date(2026, 6, 1)), batch_ref="test")
        manifest = generator.build_manifest(events, batch_ref="test", file_name=path.name)
        manifest_path = writer.write_manifest(manifest, path)

        assert "ingest_date=" not in str(manifest_path)
        assert manifest_path.suffixes[-2:] == [".manifest", ".json"]
        assert json.loads(manifest_path.read_text(encoding="utf-8"))["batch_ref"] == "test"


class TestSummary:
    def test_summary_counts_add_up(self, tmp_path: Path) -> None:
        settings = SimulatorSettings(
            seed=5, output_dir=tmp_path, state_dir=tmp_path / "s", error_rate=0.3
        )
        events = list(TradeGenerator(settings).generate(300, as_of=date(2026, 6, 1)))
        summary = summarise_events(events)

        assert summary["total"] == 300
        assert summary["expected_accepted"] + summary["expected_rejected"] == 300
        assert sum(summary["by_fault"].values()) == 300  # type: ignore[union-attr]
