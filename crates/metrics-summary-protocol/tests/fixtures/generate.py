"""Regenerate independent wire fixtures with Python msgpack (tested with 1.1.0).

This is a developer helper; the Rust tests use the committed bytes and do not
require Python or msgpack. Pass this script's path to `python3` to regenerate.
"""

from pathlib import Path

import msgpack


directory = Path(__file__).resolve().parent
identity = [bytes(range(1, 17)), 2**64 - 1]
smallest = float.fromhex("0x0.0000000000001p-1022")
request = [
    1,
    1,
    2,
    identity,
    ["svc", "worker", "node", [["region", "华东"]]],
    -(2**63),
    2**64 - 1,
    [
        [
            2**64 - 1,
            "latency",
            [["http.method", "GET"], ["route", "/read"], ["区域", "华东"]],
            "seconds",
            [0, [3, 0.6000000000000001, 0.1, 0.25, 0.3, 0.30000000000000004, -0.0, 0.29]],
        ],
        [2, "counter", [], None, [1, 2**63 - 1]],
        [3, "gauge", [], None, [2, -(2**63)]],
        [4, "smallest", [], None, [0, [1] + [smallest] * 7]],
    ],
]
ack = [1, identity, 2, 0, "完成"]
for name, value in [("request", request), ("ack", ack)]:
    payload = msgpack.packb(value, use_bin_type=True, use_single_float=False)
    (directory / f"{name}.msgpack").write_bytes(payload)
