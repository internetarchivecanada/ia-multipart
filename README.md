# ia-multipart

**Resumable large-file uploads to archive.org — and, since 2026-09-23,
parallel resumable downloads.** Two small tools, no dependencies beyond
Python 3 (and your existing `ia` configuration for uploads).

| | tool | what it does |
|---|---|---|
| up | `iamp.py` | S3 multipart upload, journaled parts, kill-and-resume |
| down | `iamd.py` | Range-part download over 4 (up to 8) streams per file, adaptive across the item's datanodes, journaled parts, md5-proved |

```
./iamp.py my-item /path/to/big-file.gz --metadata collection:opensource
# ... dies at 43% (laptop sleep, dropped wifi, S3 hiccup) ...
./iamp.py my-item /path/to/big-file.gz          # resumes at 43%
```

## Why

`ia upload` sends each file as a single HTTP PUT. archive.org keeps no
partials for plain PUTs, so a 4.5 GB upload that dies at 18% restarts from
byte zero — which on a residential uplink can mean losing an hour per
failure, repeatedly. (This tool exists because exactly that happened, twice
in one day, to the Internet Archive Europe ETD project.)

IA's S3 endpoint (`s3.us.archive.org`) honors AWS-style **multipart
uploads**, which turn every part into a server-acknowledged checkpoint: a
death costs at most one part (default 100 MiB). `iamp.py` journals progress
next to the source file (`<file>.iamp.json`) and rerunning the identical
command skips finished parts. When the source file changes (size or mtime),
the journal invalidates itself and the upload starts fresh.

## Protocol notes (learned by probing, 2026-08-26 — none of this is documented)

Verified against `s3.us.archive.org` with round-trip md5 checks, including a
kill-and-resume mid-upload:

1. `POST /{bucket}/{key}?uploads` initiates and returns an `UploadId`;
   `x-archive-auto-make-bucket` and `x-archive-meta-*` headers work here
   exactly as on plain PUTs.
2. `PUT /{bucket}/{key}?partNumber=N&uploadId=…` uploads a part — but IA
   returns **no ETag header**, unlike AWS.
3. The ETag the completion step expects is simply the **part's md5**,
   lowercase hex.
4. The completion XML must **quote** it — `<ETag>"d41d8…"</ETag>` — because
   IA compares the XML string to its stored quoted etag literally.
5. Completion **verifies every part's md5 server-side** and fails loudly on
   mismatch (`InvalidPart`, naming the expected value), so integrity checking
   is built into the protocol rather than bolted on.
6. Assembly is asynchronous: the object 404s (or serves an interim
   placeholder page, ~137 KB) for a minute or so after completion. Verify by
   size *and* checksum, never by "the URL returned 200".
7. Minimum part size follows the AWS convention (5 MiB for non-final parts).

## Usage

```
iamp.py ITEM FILE
  --remote-name NAME     name in the item (default: basename)
  --part-mb 100          part size; a death costs at most this much
  --retries 6            per-request retry budget, exponential backoff
  --parallel 1           concurrent part uploads (IA accepts them; probed at
                         1.7x serial with 4 workers — worth >1 only when one
                         stream can't fill your uplink)
  --metadata K:V         x-archive-meta-* (applies if the item is created);
                         repeat a key for a list: --metadata collection:a --metadata collection:b
  --header K:V           any extra header
```

Credentials come from `~/.config/internetarchive/ia.ini` (run `ia configure`
once).

## Caveats

- The journal trusts its own record of uploaded parts; it does not re-list
  parts server-side on resume. A journal deleted mid-upload means starting
  over (the orphaned upload id is eventually garbage-collected by IA).
- One file per invocation, by design. Loop for many files; the interesting
  problem was never the loop.

## Downloads: `iamd.py`

```
./iamd.py etd-work-files abstract-texts.jsonl.gz ./abstract-texts.jsonl.gz
  --jobs 4        parallel streams for THIS file (default 4, ceiling 8)
  --part-mb 64    part size; a death costs at most this much
```

**Why.** One HTTP stream from far away is slow whatever the pipe: from the
US Midwest we measured 0.4–1.5 MB/s per connection to archive.org datanodes,
so an 11 GB file was a 75-minute single stream on a good day and never
finished on a laptop that goes offline. Neither `ia download` nor `ia-cli`
splits a file into ranges (`ia-cli -j` parallelises across *files*).

**How.** The item's `/metadata` gives the file's size, md5 and datanodes
(`d1`, `d2`, `dir`). The file is split into fixed parts; workers fetch
`Range:` requests and `pwrite()` each part at its offset into `<dest>.part`;
every finished part is journaled in `<dest>.parts.json`, so a death costs at
most one part and rerunning the same command resumes. When all parts are in,
the whole file's md5 is checked against the manifest and it is renamed into
place. A `seed=` (library) lets a `curl -C -` partial be credited instead of
refetched.

**Routes are adaptive** because not every item is on two servers and the two
do not serve at the same rate (measured the same minute: one node of a pair
at 1.47 MB/s, the other at 0.43, and `archive.org/download` redirecting to
the slow one). Each route is probed once, then new parts go to the fastest by
a per-stream throughput average, with 1-in-8 exploration so a recovered node
is rediscovered; a stream crawling under 0.3x the best route switches route
mid-part and continues from the byte it reached. A solo-node item (`d1 ==
d2`) simply has one direct route plus the redirect.

**The cap is per file.** One file lives on one datanode pair, and that pair
is what we must not badger: default 4 streams, hard ceiling 8 (`MAX_JOBS`,
in code). Different items live on different machines, so fetching several
items at once is not counted against it. Measured on the 11 GB file above:
single curl 4.4 MB/s at best, four static streams 3.3 MB/s (two thirds of
them stuck on the slow node), four adaptive streams 4.4–4.8 MB/s sustained.

Library use: `iamd.download(item, name, dest, jobs=4, part_mb=64, seed=None)
-> md5`. Raises on size/md5 mismatch and keeps the partial for a retry.

Tests: `python3 -m pytest tests/` (needs `pytest`; a local Range-honouring
HTTP server stands in for the datanodes, including a throttled one).

Built by the Internet Archive Europe ETD project
(github.com/internetarchivecanada/etd). Tests ran against items in
`test_collection`, which archive.org expires automatically.
