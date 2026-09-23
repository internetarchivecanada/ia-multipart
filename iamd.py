#!/usr/bin/env python3
"""iamd -- parallel, resumable, verified downloads from archive.org.

The download twin of iamp.py. Brewster 2026-09-23: "I wonder if we
want a ia download multipart, kind of like ia upload multipart -- and then
we could download in parallel as well, which could speed things up."

WHY. One HTTP stream from far away is slow whatever the pipe: measured
2026-09-22/23 from Detroit, 0.1-1 MB/s per connection, with the same 2MB
range taking 20s from either node of a pair. tel-archives-ouvertes-fr
(6.9GB) took hours on one stream; the 11GB abstract corpus at 2.3MB/s is
75 minutes. Four ranges in flight are four streams.

HOW. The item's manifest gives size, md5 and the datanode pair (d1/d2/dir).
The file is split into fixed parts (--part-mb, default 64); N workers fetch
Range requests and pwrite() each part at its offset into <dest>.part; every
finished part is journaled in <dest>.parts.json, so a death costs at most
one part and a rerun resumes. When every part is in, the md5 of the whole
file is checked against the manifest and the file is renamed into place.
Nothing else writes <dest>.

ROUTES ARE ADAPTIVE. Brewster 2026-09-23: "not all items are on multiple
servers, and not all servers serve at the same rate, so this has to be
adaptable." Measured the same minute: ia600600 1.47 MB/s, its pair node
ia800600 0.43 MB/s, and archive.org/download redirected to the SLOW one.
So the Router keeps a per-route moving average of per-stream throughput,
probes every route once, then sends each new part to the fastest one (with
1-in-8 exploration so a recovered node is rediscovered). A stream that is
crawling at under SLOW_FRAC of the best route's rate SWITCHES route
mid-part and continues from the byte it reached (Range from start+got), so
a slow node costs seconds, not a whole part. A solo-node item simply has
one route (the pair collapses when d1 == d2), and then the only adaptation
is the retry/backoff; four streams to one node is still within the cap.

    iamd.py ITEM NAME DEST [--jobs 4] [--part-mb 64]
    (library: iamd.download(item, name, dest, jobs=4, part_mb=64) -> md5)

At most FOUR streams per file, ever (MAX_JOBS): we are a guest on
archive.org's datanodes, and four is already 4x one connection.

Neither `ia download` (python) nor `ia-cli` (-j = concurrent FILES) does this.
"""
import argparse
import hashlib
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = "iamd/0.1 (+github internetarchivecanada/ia-multipart)"
# Brewster 2026-09-23: "I hope we only do 4 parallel downloader max so we
# dont badger the internet archive."  A hard ceiling, not a default: the
# whole point of one tool is that nobody wires up 16 streams by accident.
# Downloads only -- "uploads is a different matter" (iamp.py is not bound by this).
MAX_JOBS = 4
STALL = int(os.environ.get("IAMD_STALL", "120"))        # per-read socket timeout
TRIES = int(os.environ.get("IAMD_PART_TRIES", "8"))
CHECK_EVERY = float(os.environ.get("IAMD_CHECK_EVERY", "5"))   # s between rate checks
SLOW_FRAC = float(os.environ.get("IAMD_SLOW_FRAC", "0.3"))    # switch below this x best
MAX_SWITCHES = 6                                               # per part, no ping-pong
CHUNK = 256 << 10            # read size; also the granularity of the rate check


class Router:
    """Per-route throughput bookkeeping; picks the route a new stream should use."""
    def __init__(self, routes):
        self.routes = list(dict.fromkeys(routes))          # dedupe, keep order
        self.rate = {r: None for r in self.routes}         # EWMA B/s per stream
        self.errors = {r: 0 for r in self.routes}
        self.probing = set()                               # handed out, no sample yet
        self.n = 0
        self.lock = threading.Lock()

    def observe(self, route, nbytes, secs):
        if secs <= 0:
            return
        with self.lock:
            v = nbytes / secs
            self.probing.discard(route)
            old = self.rate[route]
            self.rate[route] = v if old is None else 0.7 * old + 0.3 * v

    def failed(self, route):
        with self.lock:
            self.errors[route] += 1
            self.probing.discard(route)
            if self.rate[route]:
                self.rate[route] *= 0.5                    # a failure is slow, too
            elif self.rate[route] is None:
                self.rate[route] = 0.0                     # probed: it failed

    def best(self, exclude=(), reserve=False):
        with self.lock:
            cands = [r for r in self.routes if r not in exclude]
            if not cands:
                return None
            # probe every route once -- but hand each unprobed route to ONE
            # stream, or four streams all start on the first route and
            # nothing ever learns the second exists
            unprobed = [r for r in cands if self.rate[r] is None and r not in self.probing]
            if unprobed:
                if reserve:
                    self.probing.add(unprobed[0])
                return unprobed[0]
            return max(cands, key=lambda r: self.rate[r] or 0)

    def pick(self, exclude=()):
        """Fastest known route, with 1-in-8 exploration of another so a node
        that recovered (or one we only saw while it was overloaded) gets
        re-measured."""
        with self.lock:
            self.n += 1
            explore = self.n % 8 == 0
        b = self.best(exclude, reserve=True)
        if explore and len(self.routes) > 1:
            others = [r for r in self.routes if r != b and r not in exclude]
            if others:
                with self.lock:
                    return min(others, key=lambda r: self.n % 7 + self.routes.index(r))
        return b

    def best_rate(self, exclude=()):
        with self.lock:
            vs = [self.rate[r] for r in self.routes if r not in exclude and self.rate[r]]
            return max(vs) if vs else None

    def summary(self):
        with self.lock:
            return " ".join(f"{_short(r)}={(self.rate[r] or 0)/1e6:.2f}MB/s"
                            f"{'!' + str(self.errors[r]) if self.errors[r] else ''}"
                            for r in self.routes)


def _short(url):
    h = url.split("/")[2]
    return h.split(".")[0] if h.endswith("archive.org") and h != "archive.org" else "download"


class _Switch(Exception):
    pass


def manifest(item, name, hdr=None):
    """-> (size, md5, routes) for `name` in `item`."""
    req = urllib.request.Request(f"https://archive.org/metadata/{item}",
                                 headers=dict(hdr or {}, **{"User-Agent": UA}))
    with urllib.request.urlopen(req, timeout=120) as r:
        d = json.load(r)
    f = next((f for f in d.get("files", []) if f.get("name") == name), None)
    if not f:
        raise FileNotFoundError(f"{item}/{name} not in the item's manifest")
    # direct node routes first (they are what we can measure and choose
    # between); archive.org/download last -- its redirect picks a node for us,
    # and it stays useful when the manifest names no node at all.
    routes = []
    for nd in (d.get("d1"), d.get("d2")):          # a solo item has d1 == d2
        if nd and d.get("dir"):
            routes.append(f"https://{nd}{d['dir']}/{name}")
    routes.append(f"https://archive.org/download/{item}/{name}")
    return int(f["size"]), f.get("md5", ""), list(dict.fromkeys(routes))


def plan_parts(size, part_mb):
    """-> [(index, start, end_inclusive)]"""
    ps = part_mb << 20
    return [(i, s, min(s + ps, size) - 1) for i, s in enumerate(range(0, size, ps))]


def _fetch_part(router, start, end, fd, hdr):
    """Fetch bytes start..end into fd at offset start, continuing from where
    it got to whenever the route changes (error, or crawling below SLOW_FRAC
    of the best route). -> bytes written (== end-start+1)."""
    want = end - start + 1
    got, switches, fails, last = 0, 0, 0, None
    exclude = ()
    while got < want:
        url = router.pick(exclude)
        if url is None:
            exclude = (); url = router.pick()
        try:
            req = urllib.request.Request(
                url, headers=dict(hdr, Range=f"bytes={start + got}-{end}"))
            with urllib.request.urlopen(req, timeout=STALL) as r:
                if getattr(r, "status", 206) != 206:
                    raise IOError(f"expected 206, got {getattr(r, 'status', '?')}")
                tw, gw = time.time(), 0                     # rate window
                while got < want:
                    chunk = r.read(min(CHUNK, want - got))
                    if not chunk:
                        break
                    os.pwrite(fd, chunk, start + got)
                    got += len(chunk); gw += len(chunk)
                    el = time.time() - tw
                    if el >= CHECK_EVERY:
                        router.observe(url, gw, el)
                        best = router.best_rate(exclude=(url,))
                        if (best and gw / el < SLOW_FRAC * best
                                and switches < MAX_SWITCHES and got < want):
                            raise _Switch(url)
                        tw, gw = time.time(), 0
                if gw:
                    router.observe(url, gw, time.time() - tw)
            if got < want:
                raise IOError(f"short read at {got} of {want}")
            return got
        except _Switch:
            switches += 1
            exclude = (url,)                               # not this one, next time
        except Exception as e:
            last = e
            router.failed(url)
            fails += 1
            if fails >= TRIES:
                raise last
            exclude = (url,) if len(router.routes) > 1 else ()
            time.sleep(min(5 * fails, 30))
    return got


def download(item, name, dest, jobs=4, part_mb=64, hdr=None, progress=print,
             seed=None):
    """Parallel resumable download; returns the verified md5. Raises on a
    size or md5 mismatch (the partial and journal are kept for a retry).

    `seed`: a file holding a CONTIGUOUS PREFIX of the target (a curl -C -
    partial, say). It is moved into place and every part that lies wholly
    inside it is credited, so switching tools mid-download loses nothing."""
    jobs = max(1, min(int(jobs), MAX_JOBS))       # never more than 4 streams
    hdr = dict(hdr or {}, **{"User-Agent": UA})
    size, md5, routes = manifest(item, name, hdr)
    tmp, journal = dest + ".part", dest + ".parts.json"
    parts = plan_parts(size, part_mb)
    done = set()
    try:
        j = json.load(open(journal))
        if j.get("size") == size and j.get("md5") == md5 and j.get("part_mb") == part_mb:
            done = set(j.get("done", []))
    except Exception:
        pass
    if not (os.path.exists(tmp) and os.path.getsize(tmp) == size and done):
        done = set()
        if seed and os.path.exists(seed) and os.path.getsize(seed) <= size:
            have = os.path.getsize(seed)
            os.replace(seed, tmp)
            done = {i for i, s, e in parts if e < have}
            progress(f"{name}: seeded {len(done)} parts from the {have:,}-byte prefix at {seed}")
        with open(tmp, "ab") as f:               # preallocate (keeps a seed's bytes)
            f.truncate(size)
    todo = [p for p in parts if p[0] not in done]
    progress(f"{name}: {size:,} B in {len(parts)} parts of {part_mb}MB, "
             f"{len(done)} done, {len(todo)} to fetch over {jobs} streams via "
             f"{len(routes)} route(s)")
    router = Router(routes)
    lock = threading.Lock()
    fd = os.open(tmp, os.O_WRONLY)
    t0, got = time.time(), [0]
    try:
        def one(p):
            i, s, e = p
            _fetch_part(router, s, e, fd, hdr)
            with lock:
                done.add(i); got[0] += e - s + 1
                with open(journal, "w") as jf:
                    json.dump({"size": size, "md5": md5, "part_mb": part_mb,
                               "done": sorted(done)}, jf)
                n = len(done)
                if n % max(1, len(parts) // 20) == 0 or n == len(parts):
                    el = time.time() - t0
                    progress(f"  {n}/{len(parts)} parts, {got[0]/1e6:,.0f} MB "
                             f"({got[0]/1e6/max(el,1e-9):.1f} MB/s, {el:.0f}s) "
                             f"routes: {router.summary()}")
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            futs = [ex.submit(one, p) for p in todo]
            for f in as_completed(futs):
                f.result()                      # re-raise the first failure
    finally:
        os.close(fd)
    if os.path.getsize(tmp) != size:
        raise IOError(f"size mismatch: {os.path.getsize(tmp)} != {size}")
    h = hashlib.md5()
    with open(tmp, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    if md5 and h.hexdigest() != md5:
        raise IOError(f"md5 mismatch: {h.hexdigest()} != {md5} (partial kept)")
    os.replace(tmp, dest)
    try:
        os.remove(journal)
    except OSError:
        pass
    progress(f"{name}: VERIFIED md5 {h.hexdigest()} in {time.time()-t0:.0f}s; "
             f"routes: {router.summary()}")
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("item"); ap.add_argument("name"); ap.add_argument("dest")
    ap.add_argument("--jobs", type=int, default=int(os.environ.get("IAMD_JOBS", "4")),
                    help=f"parallel streams, capped at {MAX_JOBS}")
    ap.add_argument("--part-mb", type=int, default=int(os.environ.get("IAMD_PART_MB", "64")))
    a = ap.parse_args()
    try:
        download(a.item, a.name, a.dest, jobs=a.jobs, part_mb=a.part_mb,
                 progress=lambda s: print(s, flush=True))
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}", flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
