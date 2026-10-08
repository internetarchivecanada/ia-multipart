#!/usr/bin/env python3
"""Resumable large-file uploads to archive.org via S3 multipart.

Why this exists: `ia upload` sends each file as one HTTP PUT. When a 4.5 GB
upload dies at 18% -- a dropped connection, a laptop sleep, an S3 hiccup --
everything restarts from byte zero, because archive.org keeps no partials for
plain PUTs. But IA's S3 endpoint DOES honor AWS-style multipart uploads
(verified 2026-08-26 against s3.us.archive.org), which turns every part into a
checkpoint: a death costs at most one part.

Protocol notes learned by probing, since none of this is in IA's docs:
  * part PUTs return NO ETag header;
  * the ETag the completion step expects is simply the part's md5;
  * the completion XML must quote it: <ETag>"d41d8..."</ETag> -- IA compares
    the XML string to its stored quoted etag literally.
The md5-as-etag rule is a feature: integrity is verified per part, by the
server, at completion time.

Resume: state (uploadId + finished parts) is journaled next to the source
file as <file>.iamp.json. Rerunning the same command skips finished parts.
The journal is per (file, item, remote name); a changed file (size or mtime)
invalidates it and starts a fresh upload.

One part or less: a file no bigger than one part goes up as a single plain
PUT (with Content-MD5, so IA still verifies it). A one-part multipart upload
costs IA two catalog tasks (the part, then the assembly) and resumes no
better than a PUT, since a death costs one part either way. --multipart
forces the multipart path anyway.

--header k:v goes on the initiate, the single PUT, and the completion
request: IA queues the follow-up task (e.g. derive) when the upload
completes, so a header like x-archive-queue-derive:0 must be on that request.

Usage:
  iamp.py ITEM FILE [--remote-name NAME] [--part-mb 100] [--retries 6]
          [--metadata collection:opensource ...] [--header k:v ...] [--multipart]
Metadata headers only apply when the upload CREATES the item (same as ia).
"""
import base64
import argparse
import configparser
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

S3 = "https://s3.us.archive.org"


def auth_header():
    c = configparser.ConfigParser()
    c.read(os.path.expanduser("~/.config/internetarchive/ia.ini"))
    return f"LOW {c['s3']['access']}:{c['s3']['secret']}"


def request(method, url, data=None, headers=None, timeout=1800):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": auth_header(),
                                          **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def retrying(fn, tries, what):
    for i in range(tries):
        try:
            return fn()
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            code = getattr(e, "code", None)
            body = ""
            if hasattr(e, "read"):
                try:
                    body = e.read()[:200].decode("utf-8", "replace")
                except Exception:
                    pass
            if i == tries - 1:
                raise
            wait = min(300, 15 * (i + 1))
            print(f"  {what}: {code or type(e).__name__} {body[:120]} "
                  f"-- retry in {wait}s ({i+1}/{tries})", flush=True)
            time.sleep(wait)


def meta_headers(pairs):
    """--metadata K:V pairs as x-archive-meta headers (applied only when the
    upload creates the item).

    A key given more than once is a LIST, and IA wants one numbered header per
    value -- x-archive-meta01-collection, x-archive-meta02-collection -- as the
    ia CLI sends it. One header per key kept only the LAST value: items created
    with `--metadata collection:web --metadata collection:<other>` silently
    left `web` out.

    HTTP headers are latin-1; an em-dash in a title crashes urllib. IA's
    convention (same as the ia CLI): percent-encode non-ASCII values and wrap
    them as uri(...)."""
    keys = [kv.split(":", 1)[0] for kv in pairs]
    hdrs, nth = {}, {}
    for kv in pairs:
        k, v = kv.split(":", 1)
        try:
            v.encode("latin-1")
        except UnicodeEncodeError:
            v = "uri(" + urllib.parse.quote(v) + ")"
        if keys.count(k) > 1:
            nth[k] = nth.get(k, 0) + 1
            hdrs[f"x-archive-meta{nth[k]:02d}-{k}"] = v
        else:
            hdrs[f"x-archive-meta-{k}"] = v
    return hdrs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("item")
    ap.add_argument("file", type=Path)
    ap.add_argument("--remote-name")
    ap.add_argument("--part-mb", type=int, default=100,
                    help="part size MiB (min 5, AWS convention; default 100 "
                         "= a death costs at most ~100 MiB)")
    ap.add_argument("--retries", type=int, default=6)
    ap.add_argument("--parallel", type=int, default=1,
                    help="concurrent part uploads. Parts are independent by "
                         "protocol and IA accepts concurrent PUTs (probed "
                         "2026-08-26: 4 workers ran 1.7x serial). Worth >1 "
                         "only when a single stream does not fill the uplink.")
    ap.add_argument("--metadata", action="append", default=[],
                    metavar="K:V", help="x-archive-meta-* if item is created")
    ap.add_argument("--header", action="append", default=[], metavar="K:V")
    ap.add_argument("--multipart", action="store_true",
                    help="use multipart even for a file that fits in one part")
    a = ap.parse_args()

    src = a.file
    size = src.stat().st_size
    part_bytes = max(5, a.part_mb) * 1024 * 1024
    n_parts = (size + part_bytes - 1) // part_bytes
    remote = a.remote_name or src.name
    base = f"{S3}/{a.item}/{urllib.parse.quote(remote)}"
    journal = src.with_name(src.name + ".iamp.json")

    sig = {"size": size, "mtime": int(src.stat().st_mtime),
           "item": a.item, "remote": remote, "part_bytes": part_bytes}
    state = None
    if journal.exists():
        try:
            state = json.loads(journal.read_text())
        except Exception:
            state = None
        if state and state.get("sig") != sig:
            print("source or target changed since the journal; starting fresh")
            state = None

    user_hdrs = dict(kv.split(":", 1) for kv in a.header)
    create_hdrs = {"x-archive-auto-make-bucket": "1",
                   **meta_headers(a.metadata), **user_hdrs}

    if n_parts <= 1 and not a.multipart and not state:
        # fits in one part: one PUT, one IA task; resuming would cost the same
        blob = src.read_bytes()
        digest = hashlib.md5(blob)
        hdrs = {**create_hdrs,
                "Content-MD5": base64.b64encode(digest.digest()).decode()}
        retrying(lambda: request("PUT", base, data=blob, headers=hdrs),
                 a.retries, "put")
        journal.unlink(missing_ok=True)       # a stale journal from an older version of the file
        print(f"put {size} bytes (md5 {digest.hexdigest()})\n"
              f"complete: https://archive.org/download/{a.item}/{remote}")
        return 0

    if not state:
        hdrs = dict(create_hdrs)
        st, body = retrying(
            lambda: request("POST", base + "?uploads", headers=hdrs),
            a.retries, "initiate")
        upload_id = ET.fromstring(body).findtext("{*}UploadId") or \
            ET.fromstring(body).findtext("UploadId")
        state = {"sig": sig, "upload_id": upload_id, "parts": {}}
        journal.write_text(json.dumps(state))
        print(f"initiated {upload_id[:36]}... "
              f"({n_parts} parts x {a.part_mb} MiB)")
    else:
        print(f"resuming {state['upload_id'][:36]}... "
              f"({len(state['parts'])}/{n_parts} parts already up)")

    uid = state["upload_id"]
    t0 = time.time()
    lock = threading.Lock()   # journal writes and reads of the shared file
    src_f = open(src, "rb")

    def send(pn):
        with lock:                       # one reader position at a time
            src_f.seek((pn - 1) * part_bytes)
            blob = src_f.read(part_bytes)
        md5 = hashlib.md5(blob).hexdigest()
        url = f"{base}?partNumber={pn}&uploadId={uid}"
        retrying(lambda: request("PUT", url, data=blob),
                 a.retries, f"part {pn}")
        with lock:
            state["parts"][str(pn)] = md5
            journal.write_text(json.dumps(state))     # checkpoint
            done = len(state["parts"])
        sent = done * part_bytes / 1048576
        print(f"part {pn}/{n_parts} up "
              f"({100*done//n_parts}%, ~{sent/max(time.time()-t0,1):.1f} MiB/s)",
              flush=True)

    pending = [pn for pn in range(1, n_parts + 1)
               if str(pn) not in state["parts"]]
    if a.parallel > 1:
        with ThreadPoolExecutor(a.parallel) as ex:
            # list() so a failed part raises here rather than being dropped
            list(ex.map(send, pending))
    else:
        for pn in pending:
            send(pn)
    src_f.close()

    xml = "<CompleteMultipartUpload>" + "".join(
        f'<Part><PartNumber>{i}</PartNumber><ETag>"{state["parts"][str(i)]}"'
        f"</ETag></Part>" for i in range(1, n_parts + 1)
    ) + "</CompleteMultipartUpload>"
    retrying(lambda: request("POST", f"{base}?uploadId={uid}",
                             data=xml.encode(), headers=user_hdrs),
             a.retries, "complete")
    journal.unlink(missing_ok=True)
    print(f"complete: https://archive.org/download/{a.item}/{remote}\n"
          f"(assembly takes ~a minute; the item updates after derive)")


if __name__ == "__main__":
    sys.exit(main())
