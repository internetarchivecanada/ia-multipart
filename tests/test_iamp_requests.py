"""iamp: which requests go to IA, and with which headers (no network: iamp.request is replaced)."""
import base64
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import iamp  # noqa: E402

INIT = b"<InitiateMultipartUploadResult><UploadId>u-1</UploadId></InitiateMultipartUploadResult>"


def run(monkeypatch, tmp_path, size, *args):
    calls = []

    def fake(method, url, data=None, headers=None, timeout=1800):
        calls.append((method, url.split("s3.us.archive.org/", 1)[1], dict(headers or {}), data))
        return 200, INIT if url.endswith("?uploads") else b""
    monkeypatch.setattr(iamp, "request", fake)
    src = tmp_path / "f.bin"
    src.write_bytes(b"x" * size)
    monkeypatch.setattr(sys, "argv", ["iamp.py", "item", str(src), "--header", "x-archive-queue-derive:0", *args])
    iamp.main()
    return calls, src


def test_file_that_fits_in_one_part_is_one_put_with_md5_and_headers(monkeypatch, tmp_path):
    calls, src = run(monkeypatch, tmp_path, 1000, "--metadata", "collection:test_collection")
    assert [(m, u) for m, u, _, _ in calls] == [("PUT", "item/f.bin")]
    h = calls[0][2]
    assert h["x-archive-queue-derive"] == "0"
    assert h["x-archive-auto-make-bucket"] == "1" and h["x-archive-meta-collection"] == "test_collection"
    assert h["Content-MD5"] == base64.b64encode(hashlib.md5(b"x" * 1000).digest()).decode()
    assert not (tmp_path / "f.bin.iamp.json").exists()


def test_multipart_completion_carries_the_headers(monkeypatch, tmp_path):
    calls, _ = run(monkeypatch, tmp_path, 6 * 1024 * 1024, "--part-mb", "5")    # two parts
    steps = [(m, u.split("?", 1)[1]) for m, u, _, _ in calls]
    assert steps == [("POST", "uploads"), ("PUT", "partNumber=1&uploadId=u-1"), ("PUT", "partNumber=2&uploadId=u-1"),
                     ("POST", "uploadId=u-1")]
    assert calls[0][2]["x-archive-queue-derive"] == "0"           # initiate
    assert calls[-1][2] == {"x-archive-queue-derive": "0"}         # completion: the header, not the item metadata


def test_multipart_flag_keeps_the_old_path_for_a_small_file(monkeypatch, tmp_path):
    calls, _ = run(monkeypatch, tmp_path, 1000, "--multipart")
    assert [m for m, _, _, _ in calls] == ["POST", "PUT", "POST"]


def test_empty_file_is_one_put(monkeypatch, tmp_path):
    calls, _ = run(monkeypatch, tmp_path, 0)
    assert [(m, d) for m, _, _, d in calls] == [("PUT", b"")]
