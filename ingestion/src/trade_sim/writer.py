"""NDJSON batch writer.

File naming carries meaning, deliberately:

    trades_<env>_<YYYYMMDDTHHMMSSZ>_<batch_ref>_<count>.ndjson.gz

The timestamp makes lexicographic order equal chronological order, which is what lets
`ls` on the stage and a `PATTERN` in COPY both work sensibly. The batch reference is
the join key back to the manifest. The count lets an operator spot a truncated file
without opening it. None of that is available if files are named with a UUID.

Files are written to a temporary name and renamed on completion, so a consumer
watching the directory can never pick up a half-written file. That failure mode is the
single most common cause of "phantom" data quality incidents in file-based pipelines.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trade_sim.generator import GeneratedEvent
from trade_sim.schema import BatchManifest

log = logging.getLogger(__name__)


class BatchWriter:
    """Writes a batch of generated events to a date-partitioned NDJSON file."""

    def __init__(self, output_dir: Path, env: str = "dev", *, compress: bool = True) -> None:
        self.output_dir = output_dir
        self.env = env
        self.compress = compress

    def _partition_dir(self, when: datetime) -> Path:
        # Date-partitioned prefixes keep file listing cheap at scale: a COPY scoped to
        # one day lists thousands of files rather than millions. Matching the
        # production S3 layout here means the PATTERN in COPY never has to change.
        return self.output_dir / f"ingest_date={when.date().isoformat()}"

    def file_name(self, when: datetime, batch_ref: str, count: int) -> str:
        stamp = when.strftime("%Y%m%dT%H%M%SZ")
        suffix = ".ndjson.gz" if self.compress else ".ndjson"
        return f"trades_{self.env}_{stamp}_{batch_ref}_{count}{suffix}"

    def write(
        self,
        events: Iterable[GeneratedEvent],
        *,
        batch_ref: str,
        when: datetime | None = None,
    ) -> tuple[Path, list[GeneratedEvent]]:
        """Write events to a file. Returns the path and the materialised event list."""
        when = when or datetime.now(UTC)
        materialised = list(events)

        target_dir = self._partition_dir(when)
        target_dir.mkdir(parents=True, exist_ok=True)

        final_path = target_dir / self.file_name(when, batch_ref, len(materialised))
        temp_path = final_path.with_name(final_path.name + ".partial")

        payload = self._serialise(materialised)

        if self.compress:
            # mtime=0 so the gzip header is deterministic: identical input yields a
            # byte-identical file, which is what makes the whole simulator reproducible.
            with gzip.GzipFile(filename="", mode="wb", fileobj=temp_path.open("wb"), mtime=0) as gz:
                gz.write(payload)
        else:
            temp_path.write_bytes(payload)

        temp_path.replace(final_path)
        log.info(
            "wrote %d events to %s (%.1f KiB)",
            len(materialised),
            final_path.name,
            final_path.stat().st_size / 1024,
        )
        return final_path, materialised

    @staticmethod
    def _serialise(events: list[GeneratedEvent]) -> bytes:
        buffer = io.BytesIO()
        for event in events:
            if isinstance(event.payload, str):
                # A deliberately unparseable payload: write the bytes verbatim so
                # Snowflake's JSON parser sees exactly what the producer emitted.
                line = event.payload.encode("utf-8")
            else:
                line = json.dumps(event.payload, separators=(",", ":")).encode("utf-8")
            buffer.write(line)
            buffer.write(b"\n")
        return buffer.getvalue()

    def write_manifest(self, manifest: BatchManifest, data_path: Path) -> Path:
        """Write the manifest beside the data file, with a .manifest.json suffix.

        Kept out of the ingest partition tree so that COPY's PATTERN can never pick it
        up as trade data -- a manifest loaded as a trade is a confusing failure.
        """
        manifest_dir = self.output_dir.parent / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        path = manifest_dir / (data_path.name.split(".")[0] + ".manifest.json")
        path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return path


def summarise_events(events: list[GeneratedEvent]) -> dict[str, Any]:
    """Human-readable rollup for CLI output."""
    faults: dict[str, int] = {}
    for event in events:
        key = event.injected_fault or "clean"
        faults[key] = faults.get(key, 0) + 1
    return {
        "total": len(events),
        "expected_accepted": sum(1 for e in events if e.expected_verdict == "ACCEPTED"),
        "expected_rejected": sum(1 for e in events if e.expected_verdict == "REJECTED"),
        "by_fault": dict(sorted(faults.items(), key=lambda kv: (-kv[1], kv[0]))),
    }
