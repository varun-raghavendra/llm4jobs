# tracker/run_common.py

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from tracker.diffing import dedupe_preserve_order, sha256_hex, stable_json_dumps
from tracker.node_client import fetch_links_via_node

LOG = logging.getLogger("tracker.run_common")


def setup_logging(verbosity: int = 0) -> None:
    """
    verbosity=0 -> INFO
    verbosity=1 -> DEBUG
    """
    level = logging.INFO if verbosity <= 0 else logging.DEBUG

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers[:] = [handler]

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def snapshot_hash_for_links(links: List[str]) -> str:
    return sha256_hex(stable_json_dumps(links))


@dataclass(frozen=True)
class NodeFetchLinksResult:
    links: List[str]
    raw_stdout: str
    raw_stderr: str
    node_ms: int


def fetch_links_or_raise(
    *,
    url: str,
    node_bin: str,
    node_workdir: str,
    timeout_seconds: int,
    log: Optional[logging.Logger] = None,
) -> NodeFetchLinksResult:
    """
    Calls Node extractor and returns deduped links.
    Raises RuntimeError on nonzero return code.
    """
    if log is None:
        log = LOG

    node_t0 = time.perf_counter()
    node_result = fetch_links_via_node(
        node_bin=node_bin,
        node_workdir=node_workdir,
        url=url,
        timeout_seconds=timeout_seconds,
    )
    node_ms = int((time.perf_counter() - node_t0) * 1000)

    log.debug(
        "node_result url=%s returncode=%s node_ms=%d stdout_bytes=%d stderr_bytes=%d",
        url,
        node_result.returncode,
        node_ms,
        len(node_result.raw_stdout or ""),
        len(node_result.raw_stderr or ""),
    )

    if node_result.returncode != 0:
        stderr_preview = (node_result.raw_stderr or "").strip().replace("\n", "\\n")[:1000]
        log.error(
            "node_failed url=%s returncode=%s node_ms=%d stderr_preview=%s",
            url,
            node_result.returncode,
            node_ms,
            stderr_preview,
        )
        raise RuntimeError(
            "Node extractor failed "
            + f"returncode={node_result.returncode} "
            + f"stderr={node_result.raw_stderr}"
        )

    links = dedupe_preserve_order(node_result.links)
    return NodeFetchLinksResult(
        links=links,
        raw_stdout=node_result.raw_stdout or "",
        raw_stderr=node_result.raw_stderr or "",
        node_ms=node_ms,
    )


def json_sample(values: Sequence[str], n: int = 10) -> str:
    # Accept any iterable (including sets) and take a stable slice.
    return json.dumps(list(values)[:n])
