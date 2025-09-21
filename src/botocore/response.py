"""Response helpers for the simplified botocore stubs."""

from __future__ import annotations

from typing import BinaryIO, Iterable, Iterator


class StreamingBody:
    """A minimal, file-like wrapper that mimics botocore's ``StreamingBody``."""

    def __init__(self, raw_stream: BinaryIO, content_length: int):
        self._raw_stream = raw_stream
        self._remaining = content_length

    def read(self, amt: int | None = None) -> bytes:
        data = self._raw_stream.read() if amt is None else self._raw_stream.read(amt)
        if not isinstance(data, (bytes, bytearray)):
            data = data.encode("utf-8")
        self._remaining = max(0, self._remaining - len(data))
        return data

    def iter_chunks(self, chunk_size: int = 8192) -> Iterator[bytes]:
        while True:
            data = self.read(chunk_size)
            if not data:
                break
            yield data

    def __iter__(self) -> Iterable[bytes]:
        return self.iter_chunks()


__all__ = ["StreamingBody"]
