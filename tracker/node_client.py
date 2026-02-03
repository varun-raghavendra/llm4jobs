# tracker/node_client.py
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class NodeCallResult:
    links: List[str]
    raw_stdout: str
    raw_stderr: str
    returncode: int


def fetch_links_via_node(
    *,
    node_bin: str,
    node_workdir: str,
    url: str,
    timeout_seconds: int = 120,
) -> NodeCallResult:
    """
    Calls your Node extractor CLI and returns links as a list.
    Expects index.js to print one link per line.
    """
    proc = subprocess.run(
        [node_bin, "index.js", url],
        cwd=node_workdir,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    links: List[str] = []
    for line in stdout.splitlines():
        s = line.strip()
        if s:
            links.append(s)

    return NodeCallResult(
        links=links,
        raw_stdout=stdout,
        raw_stderr=stderr,
        returncode=proc.returncode,
    )
