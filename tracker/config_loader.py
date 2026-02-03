# tracker/config_loader.py
from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class CompanyTarget:
    company: str
    url: str


def load_company_targets_csv(csv_path: str) -> List[CompanyTarget]:
    """
    CSV format
    - 2 columns
    - column 1: company name
    - column 2: URL

    Supports both of these forms
    - With header: company,url
    - Without header: NVIDIA,https://...
    """
    targets: List[CompanyTarget] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = [row for row in reader if row and any(cell.strip() for cell in row)]

    if not rows:
        return targets

    first = [c.strip().lower() for c in rows[0]]
    has_header = len(first) >= 2 and (
        (first[0] in ("company", "company_name", "name") and first[1] in ("url", "link"))
        or ("company" in first[0] and "url" in first[1])
    )

    data_rows = rows[1:] if has_header else rows

    for row in data_rows:
        if len(row) < 2:
            continue
        company = row[0].strip()
        url = row[1].strip()
        if not company or not url:
            continue
        targets.append(CompanyTarget(company=company, url=url))

    return targets
