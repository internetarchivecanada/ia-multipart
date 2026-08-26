"""Does IA's S3 endpoint really honor AWS-style multipart uploads?

Protocol under test (AWS S3 v2 multipart, unsigned-payload style):
  1. POST /{bucket}/{key}?uploads                 -> UploadId
  2. PUT  /{bucket}/{key}?partNumber=N&uploadId=  -> ETag per part
  3. POST /{bucket}/{key}?uploadId=  (XML list)   -> assembled object
  4. verify: downloaded bytes == what we sent

Uses a fresh bucket in test_collection (IA expires those automatically).
Parts: 5 MiB + 1 MiB — the AWS 5 MiB minimum for non-final parts is assumed
to apply here too; that assumption is part of what this tests.
"""
import configparser
import hashlib
import os
import time
import urllib.request
import xml.etree.ElementTree as ET

c = configparser.ConfigParser()
c.read(os.path.expanduser("~/.config/internetarchive/ia.ini"))
AUTH = f"LOW {c['s3']['access']}:{c['s3']['secret']}"
BUCKET = f"multipart-probe-{time.strftime('%Y%m%d-%H%M%S')}"
KEY = "assembled.bin"
BASE = f"https://s3.us.archive.org/{BUCKET}/{KEY}"


def req(method, url, data=None, headers=None):
    h = {"Authorization": AUTH, **(headers or {})}
    r = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=300) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# deterministic test bytes, 5 MiB + 1 MiB
part1 = (b"0123456789abcdef" * 65536) * 5          # 5 MiB
part2 = b"THE-FINAL-PART--" * 65536                # 1 MiB
whole_md5 = hashlib.md5(part1 + part2).hexdigest()
print(f"bucket {BUCKET}; sending {len(part1)+len(part2):,} bytes, md5 {whole_md5}")

# 1. initiate (auto-create the bucket, file it in test_collection)
st, hd, body = req("POST", BASE + "?uploads", headers={
    "x-archive-auto-make-bucket": "1",
    "x-archive-meta-collection": "test_collection",
    "x-archive-meta-title": "multipart protocol probe (auto-expires)",
})
print(f"1. initiate: HTTP {st}")
if st != 200:
    print(body[:400]); raise SystemExit("initiate refused")
upload_id = ET.fromstring(body).findtext(
    "{*}UploadId") or ET.fromstring(body).findtext("UploadId")
print(f"   UploadId: {upload_id[:40]}...")

# 2. parts
etags = []
for n, blob in ((1, part1), (2, part2)):
    st, hd, body = req("PUT", f"{BASE}?partNumber={n}&uploadId={upload_id}",
                       data=blob)
    # IA returns no ETag header on part PUTs; its completion check compares
    # against the part's md5 (the error message says so verbatim). Compute it
    # locally -- which also means integrity is verified per part.
    et = hashlib.md5(blob).hexdigest()
    print(f"2. part {n}: HTTP {st}, md5-as-ETag {et}")
    if st != 200:
        print(body[:300]); raise SystemExit("part refused")
    etags.append(et)

# 3. complete
xml = "<CompleteMultipartUpload>" + "".join(
    f'<Part><PartNumber>{i+1}</PartNumber><ETag>"{e}"</ETag></Part>'
    for i, e in enumerate(etags)) + "</CompleteMultipartUpload>"
st, hd, body = req("POST", f"{BASE}?uploadId={upload_id}",
                   data=xml.encode())
print(f"3. complete: HTTP {st}")
if st != 200:
    print(body[:400]); raise SystemExit("complete refused")

# 4. verify round-trip (poll: assembly can lag a little)
for i in range(12):
    time.sleep(10)
    st, hd, body = req("GET", f"https://archive.org/download/{BUCKET}/{KEY}")
    if st == 200 and len(body) == len(part1) + len(part2):
        got = hashlib.md5(body).hexdigest()
        print(f"4. round-trip: {len(body):,} bytes, md5 {got}")
        print("MULTIPART WORKS" if got == whole_md5 else "MD5 MISMATCH")
        break
    print(f"   waiting for assembly... (HTTP {st}, {len(body)} bytes)")
else:
    print("assembly never appeared -- inconclusive")
