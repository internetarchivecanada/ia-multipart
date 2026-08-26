# ia-multipart

**Resumable large-file uploads to archive.org.** One small tool, no
dependencies beyond Python 3 and your existing `ia` configuration.

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
  --metadata K:V         x-archive-meta-* (applies if the item is created)
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

Built by the Internet Archive Europe ETD project
(github.com/internetarchivecanada/etd). Tests ran against items in
`test_collection`, which archive.org expires automatically.
