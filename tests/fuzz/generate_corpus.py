#!/usr/bin/env python3
"""Generate seed corpus files for Cinder fuzz targets.

Run from the project root:
    python3 tests/fuzz/generate_corpus.py

Creates binary seed files in tests/fuzz/corpus/<target>/ directories.
Each file contains one sample input that libFuzzer mutates to explore code paths.
"""

import os
import struct

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "corpus")
K_MAGIC = 0xC1
K_VERSION = 3
K_FRAME_HEADER_SIZE = 7
K_MAX_MESSAGE_SIZE = 67_108_864

OP_GET = 1
OP_SET = 2
OP_DEL = 3
OP_PING = 4
OP_GOSSIP = 5
OP_REPLICATE = 6
OP_HINT = 7
OP_GET_VERSIONED = 8

# Response status codes (Errc), mirrored from cinder::net::Errc.
STATUS_OK = 0
STATUS_NOT_FOUND = 1
STATUS_CAPACITY_EXCEEDED = 2
STATUS_INVALID_ARGUMENT = 3
STATUS_TTL_EXPIRED = 4
STATUS_NOT_SUPPORTED = 5
STATUS_INTERNAL_ERROR = 6
STATUS_TIMEOUT = 7
STATUS_NOT_READY = 8

# Flags
FLAG_HAS_TTL = 0x01
FLAG_HAS_EXPIRES_AT = 0x02
FLAG_HAS_VERSION_META = 0x01

K_SNAPSHOT_MAGIC = 0x43534E50  # "CSNP"
K_SNAPSHOT_FORMAT_VERSION = 1


def write_file(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        _ = f.write(data)


def write_seeds(target_dir, seeds):
    """Write a dict of {name: bytes} into target_dir, one file per entry.

    Returns the number of files written, so callers don't need to
    re-list the directory (which would double-count leftover files
    such as a .dict alongside the seeds).
    """
    os.makedirs(target_dir, exist_ok=True)
    for name, data in seeds.items():
        write_file(os.path.join(target_dir, name), data)
    return len(seeds)


# ---------------------------------------------------------------------------
# Protocol seed generation
# ---------------------------------------------------------------------------

def encode_request(opcode, key=b"", value=b"", flags=0, ttl_ms=0, expires_at_ms=0,
                    version=1, writer_hash=0):
    """Encode a Cinder protocol request frame."""
    # Payload: flags + optional ttl + optional expires + version + writer_hash + key + value
    payload = bytearray()
    payload.append(flags)
    if flags & FLAG_HAS_TTL:
        payload += struct.pack(">I", ttl_ms)
    if flags & FLAG_HAS_EXPIRES_AT:
        payload += struct.pack(">Q", expires_at_ms)

    payload += struct.pack(">Q", version)
    payload += struct.pack(">Q", writer_hash)
    payload += struct.pack(">I", len(key))
    payload += key
    payload += struct.pack(">I", len(value))
    payload += value

    header = struct.pack(">BBBI", K_MAGIC, K_VERSION, opcode, len(payload))
    return bytes(header + payload)


def encode_response(status, value=b"", flags=0, version=0, writer_hash=0, expires_at_ms=0):
    """Encode a Cinder protocol response frame."""
    payload = bytearray()
    payload.append(status)
    payload.append(flags)
    if flags & FLAG_HAS_VERSION_META:
        payload += struct.pack(">Q", version)
        payload += struct.pack(">Q", writer_hash)
    if flags & FLAG_HAS_EXPIRES_AT:
        payload += struct.pack(">Q", expires_at_ms)

    payload += struct.pack(">I", len(value))
    payload += value
    header = struct.pack(">BBBI", K_MAGIC, K_VERSION, 0, len(payload))
    return bytes(header + payload)


def generate_protocol_corpus():
    d = os.path.join(CORPUS_DIR, "protocol_decode")

    seeds = {
        # --- Valid requests, one per opcode ---
        "get_request": encode_request(OP_GET, key=b"hello"),
        "set_request": encode_request(OP_SET, key=b"k", value=b"v",
                                       flags=FLAG_HAS_TTL, ttl_ms=5000),
        "del_request": encode_request(OP_DEL, key=b"delete_me"),
        "ping_request": encode_request(OP_PING),
        "gossip_request": encode_request(OP_GOSSIP, value=b"node1@127.0.0.1:7000:alive:3"),
        "replicate_request": encode_request(OP_REPLICATE, key=b"k", value=b"v",
                                             version=42, writer_hash=99),
        "hint_request": encode_request(OP_HINT, key=b"k", value=b"v"),
        "get_versioned_request": encode_request(OP_GET_VERSIONED, key=b"key"),

        # --- Valid responses, one per status code so the fuzzer starts
        # with coverage of every Errc branch instead of finding them by
        # mutation alone ---
        "response_ok": encode_response(STATUS_OK, value=b"bar"),
        "response_not_found": encode_response(STATUS_NOT_FOUND),
        "response_capacity_exceeded": encode_response(STATUS_CAPACITY_EXCEEDED),
        "response_invalid_argument": encode_response(STATUS_INVALID_ARGUMENT),
        "response_ttl_expired": encode_response(STATUS_TTL_EXPIRED),
        "response_not_supported": encode_response(STATUS_NOT_SUPPORTED),
        "response_internal_error": encode_response(STATUS_INTERNAL_ERROR),
        "response_timeout": encode_response(STATUS_TIMEOUT),
        "response_not_ready": encode_response(STATUS_NOT_READY),
        "response_versioned": encode_response(STATUS_OK, value=b"val",
                                               flags=FLAG_HAS_VERSION_META,
                                               version=42, writer_hash=99),
        "response_with_expires": encode_response(
            STATUS_OK, value=b"val",
            flags=FLAG_HAS_VERSION_META | FLAG_HAS_EXPIRES_AT,
            version=1, writer_hash=1, expires_at_ms=1_700_000_000_000),
        "response_no_flags_byte": struct.pack(">BBBI", K_MAGIC, K_VERSION, 0, 1)
        + bytes([STATUS_OK]),  # pre-v3 style: flags byte absent (r.remaining() == 0 path)

        # --- Boundary / error cases ---
        "empty_frame": struct.pack(">BBBI", K_MAGIC, K_VERSION, OP_GET, 0),
        "bad_magic": struct.pack(">BBBI", 0x00, K_VERSION, OP_GET, 0),
        "bad_version": struct.pack(">BBBI", K_MAGIC, 0x99, OP_GET, 0),
        "truncated_header": bytes([K_MAGIC, K_VERSION]),
        "zero_length_header_only": b"",
        "oversized_payload": struct.pack(">BBBI", K_MAGIC, K_VERSION, OP_SET, 0xFFFFFFFF),
        "payload_len_at_max": struct.pack(">BBBI", K_MAGIC, K_VERSION, OP_SET,
                                           K_MAX_MESSAGE_SIZE),
        "payload_len_over_max_by_one": struct.pack(">BBBI", K_MAGIC, K_VERSION, OP_SET,
                                                     K_MAX_MESSAGE_SIZE + 1),
        "unknown_opcode_zero": encode_request(0, key=b"k"),
        "unknown_opcode_high": encode_request(0xFF, key=b"k"),
        "min_opcode": encode_request(OP_GET),
        "max_opcode": encode_request(OP_GET_VERSIONED),
        "empty_key": encode_request(OP_SET, key=b"", value=b"v"),
        "empty_key_and_value": encode_request(OP_SET, key=b"", value=b""),
        "large_key": encode_request(OP_SET, key=b"x" * 1000, value=b"v"),
        "large_value": encode_request(OP_SET, key=b"k", value=b"y" * 10_000),
        "both_ttl_expires": encode_request(
            OP_SET, key=b"k", value=b"v",
            flags=FLAG_HAS_TTL | FLAG_HAS_EXPIRES_AT,
            ttl_ms=5000, expires_at_ms=1_700_000_000_000),
        "key_len_exceeds_payload": encode_request(OP_SET, key=b"k")[:-1],
        "declared_len_shorter_than_actual": encode_request(OP_GET, key=b"hello")
        + b"\x00" * 8,  # trailing garbage past declared payload_len
        "truncated_mid_key": encode_request(OP_SET, key=b"longkeyhere", value=b"v")[:-6],
        "zero_ttl": encode_request(OP_SET, key=b"k", flags=FLAG_HAS_TTL, ttl_ms=0),
        "max_u32_ttl": encode_request(OP_SET, key=b"k", flags=FLAG_HAS_TTL,
                                       ttl_ms=0xFFFFFFFF),
    }

    n = write_seeds(d, seeds)
    print(f"  Generated {n} protocol seeds in {d}")


# ---------------------------------------------------------------------------
# Gossip seed generation
# ---------------------------------------------------------------------------

def generate_gossip_corpus():
    d = os.path.join(CORPUS_DIR, "gossip_parse")

    seeds = {
        "valid_single": b"node1@127.0.0.1:7000:alive:3",
        "valid_multi": b"node1@127.0.0.1:7000:alive:3;node2@10.0.0.1:8080:suspect:42",
        "valid_dead": b"n@192.168.1.1:9090:dead:0",
        "missing_at": b"node1:7000:alive:1",
        "missing_colons": b"node1@127.0.0.1",
        "invalid_port": b"node1@127.0.0.1:abc:alive:1",
        "port_overflow": b"node1@127.0.0.1:99999:alive:1",
        "invalid_state": b"node1@127.0.0.1:7000:unknown:1",
        "empty_string": b"",
        "empty_id": b"@127.0.0.1:7000:alive:1",
        "large_incarnation": b"n@127.0.0.1:7000:alive:18446744073709551615",
        "incarnation_overflow": b"n@127.0.0.1:7000:alive:18446744073709551616",
        "port_zero": b"n@127.0.0.1:0:alive:1",
        "port_max": b"n@127.0.0.1:65535:alive:1",
        "trailing_semi": b"node1@127.0.0.1:7000:alive:3;",
        "leading_semi": b";node1@127.0.0.1:7000:alive:3",
        "double_semi": b"node1@127.0.0.1:7000:alive:3;;node2@10.0.0.1:8080:alive:1",
        "ipv6_addr": b"node1@[::1]:7000:alive:3",
        "empty_entry_only_semi": b";",
        "negative_incarnation": b"n@127.0.0.1:7000:alive:-1",
        "extra_field": b"n@127.0.0.1:7000:alive:1:extra",
        "unicode_id": "café@127.0.0.1:7000:alive:1".encode(),
    }

    n = write_seeds(d, seeds)
    print(f"  Generated {n} gossip seeds in {d}")


# ---------------------------------------------------------------------------
# Store put seed generation
# ---------------------------------------------------------------------------

def generate_store_corpus():
    d = os.path.join(CORPUS_DIR, "store_put")

    seeds = {
        "short_kv": b"ab" + b"value",
        "empty_value": b"key" + b"",
        "single_byte": b"x",
        "empty_input": b"",
        "exact_4": b"abcd",
        "large_value": b"k" + b"x" * 1000,
        "null_bytes": b"\x00\x00\x00\x00" + b"\x00\x00\x00\x00",
        "unicode": b"key" + "café".encode(),
        "binary": bytes(range(256)),
        "all_zero": b"\x00" * 64,
        "all_ff": b"\xff" * 64,
    }

    n = write_seeds(d, seeds)
    print(f"  Generated {n} store seeds in {d}")


# ---------------------------------------------------------------------------
# Snapshot seed generation
# ---------------------------------------------------------------------------

def encode_snapshot(next_version, entries):
    """Encode a Cinder snapshot file."""
    buf = bytearray()
    buf += struct.pack("<I", K_SNAPSHOT_MAGIC)
    buf += struct.pack("<I", K_SNAPSHOT_FORMAT_VERSION)
    buf += struct.pack("<Q", next_version)
    buf += struct.pack("<I", len(entries))
    for key, value, version, writer_hash, expires_at_ms, has_ttl, freq in entries:
        key_bytes = key if isinstance(key, bytes) else key.encode()
        val_bytes = value if isinstance(value, bytes) else value.encode()
        buf += struct.pack("<I", len(key_bytes))
        buf += key_bytes
        buf += struct.pack("<I", len(val_bytes))
        buf += val_bytes
        buf += struct.pack("<Q", version)
        buf += struct.pack("<Q", writer_hash)
        buf += struct.pack("<Q", expires_at_ms)
        buf += struct.pack("<B", 1 if has_ttl else 0)
        buf += struct.pack("<Q", freq)
    return bytes(buf)


def generate_snapshot_corpus():
    d = os.path.join(CORPUS_DIR, "snapshot")

    seeds = {
        "valid_empty": encode_snapshot(0, []),
        "valid_one_entry": encode_snapshot(1, [("a", "v", 1, 10, 0, False, 0)]),
        "valid_two_entries": encode_snapshot(3, [
            ("key1", "val1", 1, 10, 0, False, 0),
            ("key2", "val2", 2, 20, 1_700_000_000_000, True, 0),
        ]),
        "bad_magic": struct.pack("<I", 0xDEADBEEF) + b"\x00" * 20,
        "bad_version": struct.pack("<I", K_SNAPSHOT_MAGIC) + struct.pack("<I", 99)
        + b"\x00" * 20,
        "truncated_header": struct.pack("<I", K_SNAPSHOT_MAGIC) + b"\x00" * 4,
        "empty_file": b"",
        "one_byte": b"\x00",
        "zero_entries_declared_nonzero": struct.pack("<I", K_SNAPSHOT_MAGIC)
        + struct.pack("<I", K_SNAPSHOT_FORMAT_VERSION)
        + struct.pack("<Q", 0)
        + struct.pack("<I", 0xFFFFFFFF),  # entry_count lies about what follows
        "large_key": encode_snapshot(1, [("k" * 1000, "v", 1, 0, 0, False, 0)]),
        "large_value": encode_snapshot(1, [("k", "v" * 10_000, 1, 0, 0, False, 0)]),
        "truncated_entry": encode_snapshot(1, [("a", "v", 1, 0, 0, False, 0)])[:-5],
        "truncated_mid_key": encode_snapshot(1, [("longkey", "v", 1, 0, 0, False, 0)])[:-10],
        "empty_key_entry": encode_snapshot(1, [("", "v", 1, 0, 0, False, 0)]),
        "empty_value_entry": encode_snapshot(1, [("k", "", 1, 0, 0, False, 0)]),
        "max_version": encode_snapshot(0xFFFFFFFFFFFFFFFF,
                                        [("k", "v", 0xFFFFFFFFFFFFFFFF, 0, 0, False, 0)]),
        "has_ttl_byte_invalid": encode_snapshot(1, [("k", "v", 1, 0, 0, False, 0)])[:-9]
        + b"\x02" + b"\x00" * 8,  # has_ttl byte outside {0,1}
    }

    n = write_seeds(d, seeds)
    print(f"  Generated {n} snapshot seeds in {d}")


# ---------------------------------------------------------------------------
# Protocol dictionary (for libFuzzer -dict flag)
# ---------------------------------------------------------------------------

def dict_token_value(data: bytes) -> str:
    """Format raw bytes as a libFuzzer dictionary token value.

    libFuzzer's dictionary parser expects to see the literal two-character
    escape sequence "\\xHH" in the *text* of the file and interprets it
    itself; it does not expect an already-decoded high-byte character.

    Writing `"\\xC1"` as a Python string literal does NOT produce that -
    Python resolves it to the single Unicode codepoint U+00C1 ('Á') at
    parse time, which then gets UTF-8 encoded to two bytes (0xC3 0x81)
    when written to a text file. libFuzzer sees the raw character 'Á',
    not the escape, and fails to parse it ("error in line 3").

    This function sidesteps the whole class of bug: it builds the escape
    sequence out of plain ASCII characters ('\\', 'x', hex digits) so
    nothing is ever silently re-encoded between "the byte we mean" and
    "the text libFuzzer reads".
    """
    out = []
    for b in data:
        # Printable ASCII, except the characters that would need their own
        # escaping inside the dictionary's quoted token, pass through as-is.
        if 0x20 <= b <= 0x7E and b not in (0x22, 0x5C):  # not '"' or '\'
            out.append(chr(b))
        else:
            out.append(f"\\x{b:02x}")
    return "".join(out)


def generate_protocol_dict():
    d = os.path.join(CORPUS_DIR, "protocol_decode")
    os.makedirs(d, exist_ok=True)
    dict_path = os.path.join(d, "protocol.dict")

    opcodes = [
        ("OpcodeGET", OP_GET), ("OpcodeSET", OP_SET), ("OpcodeDEL", OP_DEL),
        ("OpcodePING", OP_PING), ("OpcodeGOSSIP", OP_GOSSIP),
        ("OpcodeREPLICATE", OP_REPLICATE), ("OpcodeHINT", OP_HINT),
        ("OpcodeGET_VERSIONED", OP_GET_VERSIONED),
    ]
    statuses = [
        ("StatusOK", STATUS_OK), ("StatusNotFound", STATUS_NOT_FOUND),
        ("StatusCapacityExceeded", STATUS_CAPACITY_EXCEEDED),
        ("StatusInvalidArgument", STATUS_INVALID_ARGUMENT),
        ("StatusTtlExpired", STATUS_TTL_EXPIRED),
        ("StatusNotSupported", STATUS_NOT_SUPPORTED),
        ("StatusInternalError", STATUS_INTERNAL_ERROR),
        ("StatusTimeout", STATUS_TIMEOUT),
        ("StatusNotReady", STATUS_NOT_READY),
    ]
    flags = [
        ("FlagTTL", FLAG_HAS_TTL),
        ("FlagExpires", FLAG_HAS_EXPIRES_AT),
        ("FlagVersionMeta", FLAG_HAS_VERSION_META),
    ]
    byte_strings = [
        ("KeyHello", b"hello"), ("KeyMyKey", b"mykey"),
        ("ValBar", b"bar"), ("ValTest", b"test"),
        ("Alive", b"alive"), ("Suspect", b"suspect"), ("Dead", b"dead"),
    ]

    sections = [
        ("Magic and version", [
            ("MagicVersion", bytes([K_MAGIC, K_VERSION])),
        ]),
        ("Opcodes", [(name, bytes([val])) for name, val in opcodes]),
        ("Status codes", [(name, bytes([val])) for name, val in statuses]),
        ("Flags", [(name, bytes([val])) for name, val in flags]),
        ("Common strings", byte_strings),
    ]

    lines = ["# Cinder protocol fuzz dictionary\n"]
    for title, tokens in sections:
        lines.append(f"\n# {title}\n")
        for name, data in tokens:
            lines.append(f'{name}="{dict_token_value(data)}"\n')

    # encoding="ascii" is a deliberate regression guard: dict_token_value()
    # should only ever emit ASCII. If a future edit reintroduces a raw
    # non-ASCII byte here, this raises UnicodeEncodeError immediately at
    # generation time instead of failing cryptically inside libFuzzer later.
    with open(dict_path, "w", encoding="ascii", newline="\n") as f:
        f.writelines(lines)

    print(f"  Generated protocol dictionary at {dict_path} "
          f"({sum(len(tokens) for _, tokens in sections)} tokens)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating Cinder fuzz corpus...")
    generate_protocol_corpus()
    generate_gossip_corpus()
    generate_store_corpus()
    generate_snapshot_corpus()
    generate_protocol_dict()
    print("Done.")


if __name__ == "__main__":
    main()
