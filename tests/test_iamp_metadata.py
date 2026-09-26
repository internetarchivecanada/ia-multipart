"""iamp: --metadata becomes the x-archive-meta headers IA applies at item creation."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import iamp  # noqa: E402


def test_repeated_key_is_a_list_of_numbered_headers():
    h = iamp.meta_headers(["mediatype:web", "collection:web",
                           "collection:theses-and-dissertations-web", "title:X"])
    assert h == {"x-archive-meta-mediatype": "web",
                 "x-archive-meta01-collection": "web",
                 "x-archive-meta02-collection": "theses-and-dissertations-web",
                 "x-archive-meta-title": "X"}


def test_single_key_keeps_the_plain_header():
    assert iamp.meta_headers(["collection:opensource"]) == {"x-archive-meta-collection": "opensource"}


def test_value_may_contain_colons_and_non_latin1_is_uri_wrapped():
    h = iamp.meta_headers(["description:see https://x.org/a:b", "title:Theses — part 1"])
    assert h["x-archive-meta-description"] == "see https://x.org/a:b"
    assert h["x-archive-meta-title"] == "uri(Theses%20%E2%80%94%20part%201)"
