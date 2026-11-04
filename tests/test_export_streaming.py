"""Tests for bounded streaming export (no full-file materialization)."""
import asyncio
from unittest.mock import MagicMock, patch

import pytest

from target_queue_storage import export_records, _collect_export_files


@pytest.mark.asyncio
async def test_export_records_bounded_concurrency():
    """All records are sent without loading more tasks than max_concurrency."""
    client = MagicMock()
    max_in_flight = 0
    current = 0
    sent_payloads = []

    async def fake_send_one(record, msg_key, file_name, client, retry_count=0):
        nonlocal max_in_flight, current
        current += 1
        max_in_flight = max(max_in_flight, current)
        sent_payloads.append(record["id"])
        await asyncio.sleep(0.01)
        current -= 1
        return True

    records = [{"id": i} for i in range(25)]
    stats = {"successes": 0, "failures": 0}
    with patch("target_queue_storage.send_one", side_effect=fake_send_one):
        successes, failures = await export_records(
            iter(records), "prefix", "test.part", client, max_concurrency=5, stats=stats
        )

    assert successes == 25
    assert failures == 0
    assert stats == {"successes": 25, "failures": 0}
    assert max_in_flight <= 5
    assert sent_payloads == list(range(25))


@pytest.mark.asyncio
async def test_export_records_counts_failures():
    client = MagicMock()

    async def fake_send_one(record, msg_key, file_name, client, retry_count=0):
        return record["id"] % 2 == 0

    records = [{"id": i} for i in range(4)]
    with patch("target_queue_storage.send_one", side_effect=fake_send_one):
        successes, failures = await export_records(
            iter(records), "prefix", "test.part", client, max_concurrency=2
        )

    assert successes == 2
    assert failures == 2


def test_collect_export_files_sorted(tmp_path):
    (tmp_path / "b.part").write_text("{}\n")
    (tmp_path / "a.part").write_text("{}\n")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.json").write_text("[]")

    collected = _collect_export_files(str(tmp_path))
    names = [name for _, name, kind in collected]
    assert names == ["a.part", "b.part", "c.json"]
    assert [kind for *_, kind in collected] == ["part", "part", "json"]
