"""Generate the gRPC stubs for YouTube streamList (needs grpcio-tools).

    uv pip install grpcio grpcio-tools
    uv run python scripts/mila/build_youtube_proto.py

Writes stream_list_pb2.py and stream_list_pb2_grpc.py next to
src/mila_stream/proto/stream_list.proto and makes the import inside the _grpc file
relative, so it works inside the mila_stream package. Without these files the reader
falls back to polling (liveChatMessages.list).
"""

import re
import sys
from pathlib import Path

PROTO_DIR = Path(__file__).resolve().parents[2] / "src" / "mila_stream" / "proto"


def main() -> int:
    import grpc_tools
    from grpc_tools import protoc

    include = Path(grpc_tools.__file__).parent / "_proto"
    rc = protoc.main(
        [
            "protoc",
            f"-I{PROTO_DIR}",
            f"-I{include}",
            f"--python_out={PROTO_DIR}",
            f"--grpc_python_out={PROTO_DIR}",
            str(PROTO_DIR / "stream_list.proto"),
        ]
    )
    if rc != 0:
        print("protoc failed", rc)
        return rc
    g = PROTO_DIR / "stream_list_pb2_grpc.py"
    src = g.read_text(encoding="utf-8")
    src = re.sub(
        r"^import stream_list_pb2 as",
        "from . import stream_list_pb2 as",
        src,
        flags=re.MULTILINE,
    )
    g.write_text(src, encoding="utf-8")
    print(
        "generated:", ", ".join(p.name for p in PROTO_DIR.glob("stream_list_pb2*.py"))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
