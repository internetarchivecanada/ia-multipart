"""iamd's route-switch guard must be able to fire against a trickler.

The guard measures throughput and switches route when one falls below
SLOW_FRAC of the best. It runs BETWEEN reads. So the read size decides
whether it can run at all: HTTPResponse.read(n) blocks until it has n bytes,
and the socket timeout is per-recv, so a dribbling route keeps resetting the
timeout while read() sits there holding the loop.

At the old 256 KiB, a 1 KB/s route gave the guard one turn every 262
seconds. The identical defect in the ETD extractor's own download loop cost three
runs -- a datanode on 2026-09-22, kb.dk at 15 KB/s on 09-23, and a worker
wedged on web.archive.org for 41 minutes on 09-25 -- and was fixed there the
same day by dropping its read to 64 KiB.

This pins the property rather than the number: whatever the read size is, a
slow-but-alive route must let the guard run inside one socket timeout.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import iamd  # noqa: E402

SLOWEST_LIVE_BPS = 1024          # 1 KB/s: slower than this is a dead route


def test_a_trickling_route_lets_the_rate_check_run_inside_one_timeout():
    seconds_per_read = iamd.CHUNK / SLOWEST_LIVE_BPS
    assert seconds_per_read <= iamd.STALL, (
        f"a {SLOWEST_LIVE_BPS} B/s route needs {seconds_per_read:.0f}s to fill "
        f"a {iamd.CHUNK} B read, but the socket timeout is {iamd.STALL}s -- the "
        "read blocks and the between-reads guard never gets a turn")


def test_the_read_size_is_the_fixed_value_the_guard_depends_on():
    """64 KiB is a correctness setting, not tuning. Pinned so a future
    'let us read bigger chunks, it will be faster' cannot silently disable
    the route-switch guard again."""
    assert iamd.CHUNK == 64 << 10
