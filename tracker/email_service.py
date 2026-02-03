# tracker/email_service.py
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import smtplib
import socket
import time
import datetime
from zoneinfo import ZoneInfo
import csv
from email.message import EmailMessage
from typing import Any, Dict, List

from dotenv import load_dotenv

from tracker.db import SQLiteState

LOG = logging.getLogger("tracker.email_service")


def setup_logging(verbose: int) -> None:
    level = logging.DEBUG if verbose > 0 else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def digest_id(owner: str) -> str:
    s = f"{owner}:{int(time.time())}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def format_markdown_digest(jobs: List[Dict[str, Any]]) -> str:
    """
    Group by site, sorted by created_ts_ms desc already.
    """
    lines: List[str] = []
    lines.append("# Job alerts")
    lines.append("")
    lines.append(f"Total new jobs: {len(jobs)}")
    lines.append("")
    lines.append("| Company | Job title | URL | Min years |")
    lines.append("|---|---|---|---|")

    for j in jobs:
        site = str(j.get("site", "")).strip() or "Unknown"
        title = str(j.get("job_title", "")).strip() or "Untitled"
        url = str(j.get("url", "")).strip()
        yrs = int(j.get("min_years", 0) or 0)
        link = f"[Link]({url})" if url else "Link"
        lines.append(f"| {site} | {title} | {link} | {yrs} |")

    return "\n".join(lines).strip() + "\n"

def format_plaintext_digest(jobs: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append("Job alerts")
    lines.append("")
    lines.append(f"Total new jobs: {len(jobs)}")
    lines.append("")
    for j in jobs:
        site = str(j.get("site", "")).strip() or "Unknown"
        title = str(j.get("job_title", "")).strip() or "Untitled"
        url = str(j.get("url", "")).strip()
        yrs = int(j.get("min_years", 0) or 0)
        lines.append(f"- {site} | {title} | min years: {yrs}")
        if url:
            lines.append(f"  {url}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def format_html_digest(jobs: List[Dict[str, Any]]) -> str:
    rows: List[str] = []
    for j in jobs:
        site = _html_escape(str(j.get("site", "")).strip() or "Unknown")
        title = _html_escape(str(j.get("job_title", "")).strip() or "Untitled")
        url = str(j.get("url", "")).strip()
        yrs = int(j.get("min_years", 0) or 0)
        link = f'<a href="{_html_escape(url)}">Link</a>' if url else "Link"
        rows.append(
            f"<tr>"
            f"<td>{site}</td>"
            f"<td>{title}</td>"
            f"<td>{link}</td>"
            f"<td>{yrs}</td>"
            f"</tr>"
        )

    rows_html = "\n".join(rows)
    return (
        "<html><body>"
        "<h1>Job alerts</h1>"
        f"<p>Total new jobs: {len(jobs)}</p>"
        "<table border=\"1\" cellpadding=\"6\" cellspacing=\"0\" style=\"border-collapse:collapse;\">"
        "<thead>"
        "<tr><th>Company</th><th>Job title</th><th>URL</th><th>Min years</th></tr>"
        "</thead>"
        "<tbody>"
        f"{rows_html}"
        "</tbody>"
        "</table>"
        "</body></html>"
    )


def send_email_digest(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    email_from: str,
    email_to: str,
    subject: str,
    body_text: str,
    body_html: str,
    attach_path: str | None = None,
) -> None:
    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(body_text)
    msg.add_alternative(body_html, subtype="html")

    if attach_path and os.path.exists(attach_path):
        try:
            with open(attach_path, "r", encoding="utf-8") as fh:
                data = fh.read()
            filename = os.path.basename(attach_path)
            msg.add_attachment(data, subtype="csv", filename=filename)
        except Exception:
            LOG.exception("failed_to_attach_file path=%s", attach_path)

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./state/snapshots.sqlite3")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    setup_logging(1 if args.verbose else 0)
    load_dotenv()

    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    email_from = os.getenv("EMAIL_FROM", "")
    email_to = os.getenv("EMAIL_TO", "")

    missing = [k for k, v in [
        ("SMTP_HOST", smtp_host),
        ("SMTP_USER", smtp_user),
        ("SMTP_PASS", smtp_pass),
        ("EMAIL_FROM", email_from),
        ("EMAIL_TO", email_to),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    owner = f"{socket.gethostname()}:{os.getpid()}"
    did = digest_id(owner)

    state = SQLiteState(args.db)
    jobs = state.list_jobs_ready_for_email(limit=args.limit)
    state.close()

    if not jobs:
        LOG.info("no_jobs_ready")
        return

    # Safety filter: only include jobs with < 4 years of experience.
    jobs = [j for j in jobs if int(j.get("min_years", 0) or 0) < 4]
    LOG.info(jobs)
    if not jobs:
        LOG.info("no_jobs_ready_after_filter")
        return

    # Prepare message bodies and CSV path
    body_text = format_plaintext_digest(jobs)
    body_html = format_html_digest(jobs)
    subject = f"Job alerts ({len(jobs)} new)"

    # Record every emailed job to a CSV for auditing/backup BEFORE sending so the
    # attached CSV contains all jobs up to and including this digest.
    # Provide separate date and time columns in Mountain Time (AM/PM).
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    try:
        mt = now_utc.astimezone(ZoneInfo("America/Denver"))
    except Exception:
        # fallback to UTC if zoneinfo unavailable
        mt = now_utc
    emailed_date = mt.strftime("%Y-%m-%d")
    emailed_time = mt.strftime("%I:%M:%S %p").lstrip("0")
    csv_path = os.getenv("EMAILED_JOBS_CSV", "./state/emailed_jobs.csv")
    try:
        os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
        write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
        with open(csv_path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if write_header:
                # Columns: emailed_date (YYYY-MM-DD), emailed_time (AM/PM Mountain Time), site, url, job_title, min_years
                writer.writerow(["emailed_date", "emailed_time", "site", "url", "job_title", "min_years"])
            for j in jobs:
                try:
                    site = j.get("site", "")
                    url = j.get("url", "")
                    title = j.get("job_title", "")
                    yrs = int(j.get("min_years", 0) or 0)
                    writer.writerow([emailed_date, emailed_time, site, url, title, yrs])
                except Exception:
                    LOG.exception("failed_to_write_emailed_job row=%s", j)
    except Exception:
        LOG.exception("failed_to_append_emailed_jobs_csv")

    # Send email and attach the CSV
    send_email_digest(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        email_from=email_from,
        email_to=email_to,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        attach_path=csv_path,
    )

    # After successful send, mark jobs as emailed in the DB
    urls = [j["url"] for j in jobs]
    state = SQLiteState(args.db)
    marked = state.mark_jobs_emailed(urls=urls, digest_id=did)
    state.close()

    LOG.info("email_sent count=%d digest_id=%s", marked, did)


if __name__ == "__main__":
    main()
