"""iamd: parallel, resumable, verified downloads (the twin of iamp).

Served by a tiny local HTTP server that honours Range, plus a metadata stub,
so the test exercises real sockets, pwrite at offsets, the journal, and the
route rotation -- not mocks of urlopen.
"""
import hashlib
import http.server
import json
import os
import random
import socketserver
import sys
import threading
import time

import pytest

_REAL_SLEEP = time.sleep      # tests no-op time.sleep for iamd's backoff; the
                              # throttled test server must still really sleep

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
import iamd                                                       # noqa: E402

random.seed(11)
BODY = bytes(random.getrandbits(8) for _ in range(3 * 1024 * 1024 + 12345))
MD5 = hashlib.md5(BODY).hexdigest()


class Srv(http.server.BaseHTTPRequestHandler):
    fail_first_n = {"n": 0}          # route "/bad/..." fails this many times
    served = {}                      # route prefix -> bytes served
    SLOW_SLEEP = 0.02                # per 16KB on "/slow/..." (~800KB/s)

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path.startswith("/bad/") and Srv.fail_first_n["n"] > 0:
            Srv.fail_first_n["n"] -= 1
            self.send_error(500); return
        rng = self.headers.get("Range")
        if not rng:
            self.send_response(200); self.send_header("Content-Length", str(len(BODY)))
            self.end_headers(); self.wfile.write(BODY); return
        a, b = rng.split("=")[1].split("-"); a, b = int(a), int(b)
        chunk = BODY[a:b + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {a}-{b}/{len(BODY)}")
        self.send_header("Content-Length", str(len(chunk))); self.end_headers()
        pfx = self.path.split("/")[1]
        try:
            if pfx == "slow":
                for i in range(0, len(chunk), 16384):
                    self.wfile.write(chunk[i:i + 16384]); self.wfile.flush()
                    Srv.served[pfx] = Srv.served.get(pfx, 0) + len(chunk[i:i + 16384])
                    _REAL_SLEEP(Srv.SLOW_SLEEP)
            else:
                self.wfile.write(chunk)
                Srv.served[pfx] = Srv.served.get(pfx, 0) + len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass


@pytest.fixture(scope="module")
def server():
    httpd = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Srv)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def _stub_manifest(monkeypatch, routes):
    monkeypatch.setattr(iamd, "manifest", lambda item, name, hdr=None: (len(BODY), MD5, routes))
    monkeypatch.setattr(iamd.time, "sleep", lambda s: None)


def test_parallel_download_is_byte_exact(server, monkeypatch, tmp_path):
    _stub_manifest(monkeypatch, [f"{server}/good/f"])
    dest = str(tmp_path / "f")
    got = iamd.download("i", "f", dest, jobs=4, part_mb=1, progress=lambda s: None)
    assert got == MD5 and open(dest, "rb").read() == BODY
    assert not os.path.exists(dest + ".part") and not os.path.exists(dest + ".parts.json")


def test_resume_fetches_only_the_missing_parts(server, monkeypatch, tmp_path):
    _stub_manifest(monkeypatch, [f"{server}/good/f"])
    dest = str(tmp_path / "f")
    parts = iamd.plan_parts(len(BODY), 1)
    # simulate a death after parts 0 and 2 landed
    with open(dest + ".part", "wb") as f:
        f.truncate(len(BODY))
        for i, s, e in parts:
            if i in (0, 2):
                f.seek(s); f.write(BODY[s:e + 1])
    json.dump({"size": len(BODY), "md5": MD5, "part_mb": 1, "done": [0, 2]},
              open(dest + ".parts.json", "w"))
    fetched = []
    real = iamd._fetch_part
    def spy(router, s, e, fd, hdr):
        fetched.append(s); return real(router, s, e, fd, hdr)
    monkeypatch.setattr(iamd, "_fetch_part", spy)
    iamd.download("i", "f", dest, jobs=3, part_mb=1, progress=lambda s: None)
    assert open(dest, "rb").read() == BODY
    assert sorted(fetched) == [parts[i][1] for i in (1, 3)], fetched


def test_a_failing_route_is_bypassed_by_the_next_one(server, monkeypatch, tmp_path):
    """Two routes, like a datanode pair: the first 500s a few times."""
    Srv.fail_first_n["n"] = 3
    _stub_manifest(monkeypatch, [f"{server}/bad/f", f"{server}/good/f"])
    dest = str(tmp_path / "f")
    iamd.download("i", "f", dest, jobs=2, part_mb=1, progress=lambda s: None)
    assert open(dest, "rb").read() == BODY


def test_md5_mismatch_keeps_the_partial_and_raises(server, monkeypatch, tmp_path):
    monkeypatch.setattr(iamd, "manifest", lambda item, name, hdr=None: (len(BODY), "0" * 32, [f"{server}/good/f"]))
    monkeypatch.setattr(iamd.time, "sleep", lambda s: None)
    dest = str(tmp_path / "f")
    with pytest.raises(IOError, match="md5 mismatch"):
        iamd.download("i", "f", dest, jobs=2, part_mb=1, progress=lambda s: None)
    assert os.path.exists(dest + ".part") and not os.path.exists(dest)


def test_streams_per_file_are_capped(server, monkeypatch, tmp_path):
    """Brewster: the cap is per item/file ('items generally live on different
    machines'), default 4, ceiling 8 -- asking for 16 gets 8."""
    _stub_manifest(monkeypatch, [f"{server}/good/f"])
    seen = []
    real = iamd.ThreadPoolExecutor
    class Spy(real):
        def __init__(self, max_workers=None, **kw):
            seen.append(max_workers); super().__init__(max_workers=max_workers, **kw)
    monkeypatch.setattr(iamd, "ThreadPoolExecutor", Spy)
    iamd.download("i", "f", str(tmp_path / "f"), jobs=16, part_mb=1, progress=lambda s: None)
    assert seen == [8]
    assert iamd.MAX_JOBS == 8 and iamd.DEFAULT_JOBS == 4


def test_a_slow_node_is_abandoned_for_the_fast_one(server, monkeypatch, tmp_path):
    """Brewster: 'not all servers serve at the same rate, so this has to be
    adaptable.' A datanode pair where one node crawls: after the one probe
    each route gets, streams switch mid-part and the bulk comes from the
    fast node."""
    Srv.served.clear()
    monkeypatch.setattr(iamd, "CHECK_EVERY", 0.25)
    monkeypatch.setattr(iamd, "CHUNK", 65536)
    _stub_manifest(monkeypatch, [f"{server}/slow/f", f"{server}/good/f"])
    dest = str(tmp_path / "f")
    iamd.download("i", "f", dest, jobs=4, part_mb=1, progress=lambda s: None)
    assert open(dest, "rb").read() == BODY
    slow, good = Srv.served.get("slow", 0), Srv.served.get("good", 0)
    assert good > 2 * slow, (slow, good)
    assert slow > 0                              # it WAS probed, not ignored


def test_manifest_collapses_a_solo_node(monkeypatch):
    """A solo-node item has d1 == d2: one direct route, plus the redirect."""
    import io, json as _json
    body = _json.dumps({"d1": "ia1.us.archive.org", "d2": "ia1.us.archive.org",
                        "dir": "/7/items/x", "files": [{"name": "f", "size": "10", "md5": "m"}]})
    class R(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): pass
    monkeypatch.setattr(iamd.urllib.request, "urlopen", lambda req, timeout=0: R(body.encode()))
    size, md5, routes = iamd.manifest("x", "f")
    assert (size, md5) == (10, "m")
    assert routes == ["https://ia1.us.archive.org/7/items/x/f", "https://archive.org/download/x/f"]


def test_router_probes_each_route_once_then_prefers_the_fastest():
    r = iamd.Router(["a", "b", "b"])
    assert r.routes == ["a", "b"]
    assert r.pick() == "a" and r.pick() == "b"                    # each probed once
    assert r.pick() == "a"                                         # both probing: first
    r.observe("a", 1_000_000, 1.0); assert r.best() == "a"        # b still probing
    r.failed("b");                  assert r.best() == "a"        # b probed: 0
    r.observe("b", 5_000_000, 1.0); assert r.best() == "b"        # EWMA 0.7*0+0.3*5 = 1.5MB/s
    r.failed("b"); r.failed("b");   assert r.best() == "a"        # failures halve it


def test_a_contiguous_prefix_seeds_the_journal(server, monkeypatch, tmp_path):
    """A curl -C - partial is moved in and its whole parts credited."""
    _stub_manifest(monkeypatch, [f"{server}/good/f"])
    dest = str(tmp_path / "f"); seed = str(tmp_path / "f.curl")
    have = 2 * 1024 * 1024 + 777
    open(seed, "wb").write(BODY[:have])
    fetched = []
    real = iamd._fetch_part
    def spy(router, s, e, fd, hdr):
        fetched.append(s); return real(router, s, e, fd, hdr)
    monkeypatch.setattr(iamd, "_fetch_part", spy)
    iamd.download("i", "f", dest, jobs=2, part_mb=1, seed=seed, progress=lambda s: None)
    assert open(dest, "rb").read() == BODY and not os.path.exists(seed)
    assert sorted(fetched) == [2 * 1024 * 1024, 3 * 1024 * 1024]   # parts 0,1 credited


def test_a_finished_file_is_not_refetched(server, monkeypatch, tmp_path):
    _stub_manifest(monkeypatch, [f"{server}/good/f"])
    dest = str(tmp_path / "f"); open(dest, "wb").write(BODY)
    monkeypatch.setattr(iamd, "_fetch_part", lambda *a, **k: pytest.fail("fetched"))
    assert iamd.download("i", "f", dest, progress=lambda s: None) == MD5


def test_plan_parts_covers_the_file_exactly():
    ps = iamd.plan_parts(len(BODY), 1)
    assert ps[0][1] == 0 and ps[-1][2] == len(BODY) - 1
    assert sum(e - s + 1 for _, s, e in ps) == len(BODY)
    assert all(ps[i][2] + 1 == ps[i + 1][1] for i in range(len(ps) - 1))
