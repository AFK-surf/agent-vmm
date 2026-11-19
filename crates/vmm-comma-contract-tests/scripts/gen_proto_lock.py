#!/usr/bin/env python3
"""Generate golden_vectors/proto_lock.json from a directory of agent-vmm .proto files.

The lock records every wire-visible fact of the pinned protocol: message fields
(name, number, type, label, oneof), enum values (name, number) and service RPCs
(name, request, response, streaming).  vmm-comma-contract-tests compares another
proto tree against it: removals, renumbering and type changes fail; additions pass.

Usage: gen_proto_lock.py <proto_root> <source_revision> > proto_lock.json
"""
import json
import re
import sys
from pathlib import Path


def statements(text):
    """Yield (kind, text) tokens: statements ending in ';', block opens '{', closes '}'."""
    current = ""
    index, size = 0, len(text)
    while index < size:
        char = text[index]
        if char == '"':
            end = text.index('"', index + 1)
            current += text[index:end + 1]
            index = end + 1
            continue
        if text.startswith("//", index):
            newline = text.find("\n", index)
            index = size if newline < 0 else newline
            continue
        if text.startswith("/*", index):
            index = text.index("*/", index) + 2
            continue
        if char in ";{}":
            yield char, " ".join(current.split())
            current = ""
        else:
            current += char
        index += 1


FIELD = re.compile(r"^(?:(optional|repeated)\s+)?([A-Za-z0-9_.]+)\s+([A-Za-z0-9_]+)\s*=\s*(\d+)(?:\s*\[.*\])?$")
ENUM_VALUE = re.compile(r"^([A-Z0-9_]+)\s*=\s*(-?\d+)(?:\s*\[.*\])?$")
RPC = re.compile(r"^rpc\s+(\w+)\s*\(\s*(stream\s+)?([\w.]+)\s*\)\s*returns\s*\(\s*(stream\s+)?([\w.]+)\s*\)$")


def parse(path):
    package = None
    messages, enums, services = {}, {}, {}
    stack = []  # (kind, name)
    for end, body in statements(path.read_text()):
        if end == "}":
            if not stack:
                raise SystemExit(f"{path}: unbalanced }}")
            stack.pop()
            continue
        if end == "{":
            head = body.split()
            if head and head[0] == "extend":
                stack.append(("extend", head[1]))
            elif head and head[0] in ("message", "enum", "service", "oneof"):
                if head[0] in ("message", "enum") and stack:
                    raise SystemExit(f"{path}: nested {head[0]} {head[1]} is not supported")
                stack.append((head[0], head[1]))
                if head[0] == "message":
                    messages[head[1]] = {}
                elif head[0] == "enum":
                    enums[head[1]] = {}
                elif head[0] == "service":
                    services[head[1]] = {}
            elif head and head[0] == "rpc":
                match = RPC.match(body)
                if not match:
                    raise SystemExit(f"{path}: bad rpc: {body}")
                services[stack[-1][1]][match[1]] = {
                    "request": match[3], "client_streaming": bool(match[2]),
                    "response": match[5], "server_streaming": bool(match[4]),
                }
                stack.append(("rpcbody", match[1]))
            else:
                raise SystemExit(f"{path}: unknown block: {body}")
            continue
        if not body:
            continue
        if body.startswith("package "):
            package = body.split()[1]
            continue
        if body.split()[0] in ("syntax", "import", "option", "reserved"):
            continue
        if not stack:
            raise SystemExit(f"{path}: top-level statement: {body}")
        kind, name = stack[-1]
        if kind == "service":
            match = RPC.match(body)
            if not match:
                raise SystemExit(f"{path}: bad rpc: {body}")
            services[name][match[1]] = {
                "request": match[3], "client_streaming": bool(match[2]),
                "response": match[5], "server_streaming": bool(match[4]),
            }
        elif kind == "enum":
            match = ENUM_VALUE.match(body)
            if not match:
                raise SystemExit(f"{path}: bad enum value: {body}")
            enums[name][match[1]] = int(match[2])
        elif kind in ("message", "oneof"):
            owner = name if kind == "message" else stack[-2][1]
            match = FIELD.match(body)
            if not match:
                raise SystemExit(f"{path}: bad field: {body}")
            entry = {"number": int(match[4]), "type": match[2], "label": match[1] or ""}
            if kind == "oneof":
                entry["oneof"] = name
            messages[owner][match[3]] = entry
        elif kind in ("rpcbody", "extend"):
            continue
    if package is None:
        raise SystemExit(f"{path}: no package")
    return package, messages, enums, services


def main():
    root, revision = Path(sys.argv[1]), sys.argv[2]
    lock = {"source": "AFK-surf/agent-vmm", "revision": revision, "packages": {}}
    for path in sorted(root.rglob("*.proto")):
        package, messages, enums, services = parse(path)
        target = lock["packages"].setdefault(package, {"messages": {}, "enums": {}, "services": {}})
        for kind, values in (("messages", messages), ("enums", enums), ("services", services)):
            for name, value in values.items():
                if name in target[kind]:
                    raise SystemExit(f"duplicate {package}.{name}")
                target[kind][name] = value
    json.dump(lock, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
