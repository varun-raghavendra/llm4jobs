# tracker/diffing.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple


def stable_json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def diff_links(old_links: Iterable[str], new_links: Iterable[str]) -> Tuple[Set[str], Set[str]]:
    old_set = set(old_links)
    new_set = set(new_links)
    added = new_set - old_set
    removed = old_set - new_set
    return added, removed


@dataclass(frozen=True)
class DiffPayload:
    site: str
    added_urls: List[str]
    diff_hash: str


def build_diff_payload(site: str, added_urls: Set[str]) -> DiffPayload:
    payload_obj = {
        "site": site,
        "added_urls": sorted(added_urls),
    }
    diff_hash = sha256_hex(stable_json_dumps(payload_obj))
    return DiffPayload(
        site=site,
        added_urls=payload_obj["added_urls"],
        diff_hash=diff_hash,
    )
