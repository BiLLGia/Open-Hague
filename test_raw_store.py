import hashlib

from pipeline.raw_store import RawStore


def test_stores_content_addressed(tmp_path):
    store = RawStore(tmp_path)
    data = b'{"hello": "world"}\n'
    ref = store.put("openbesluitvorming", data, suffix=".ndjson")
    digest = hashlib.sha256(data).hexdigest()
    assert ref.sha256 == digest
    assert ref.path == tmp_path / "raw" / "openbesluitvorming" / digest[:2] / f"{digest}.ndjson"
    assert ref.path.read_bytes() == data
    assert ref.size == len(data)
    assert ref.created is True


def test_put_is_idempotent_and_never_overwrites(tmp_path):
    store = RawStore(tmp_path)
    first = store.put("src", b"same bytes")
    mtime = first.path.stat().st_mtime_ns
    second = store.put("src", b"same bytes")
    assert second.path == first.path
    assert second.created is False
    assert first.path.stat().st_mtime_ns == mtime


def test_files_are_read_only(tmp_path):
    ref = RawStore(tmp_path).put("src", b"x")
    assert not ref.path.stat().st_mode & 0o222


def test_rejects_unsafe_source_names(tmp_path):
    store = RawStore(tmp_path)
    for bad in ["../etc", "a/b", "", "UPPER"]:
        try:
            store.put(bad, b"x")
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")
