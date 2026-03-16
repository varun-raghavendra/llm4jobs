import sys
import json
import os
from pathlib import Path
from dotenv import load_dotenv

SECRETS_FILE = Path(__file__).resolve().parents[1] / "state" / "secrets.env"
load_dotenv(override=False)
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
if not OPENAI_API_KEY and SECRETS_FILE.exists():
    # If container env sets OPENAI_API_KEY to an empty string, allow secrets
    # file to override that empty value.
    load_dotenv(dotenv_path=SECRETS_FILE, override=True)

from openai import OpenAI

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
if not OPENAI_API_KEY:
    raise RuntimeError(
        "Missing OPENAI_API_KEY. Set it via container env (.env / docker-compose) "
        f"or put it in {SECRETS_FILE}"
    )

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
You are an information extraction system.

Your tasks:
1) Extract the MINIMUM required years of professional experience from the job description.
2) Extract the human-readable job title for this role.

Rules for MINIMUM YEARS:
- Ignore degrees entirely (BS, MS, PhD).
- Ignore preferred qualifications unless explicitly required.
- If multiple experience numbers appear, choose the LOWEST.
- Do NOT infer years of experience from the job title alone.
- If entry-level or new grad, return 0.
- If unclear, return 0.
- Never guess.

Rules for JOB TITLE:
- Use the main job posting content, not cookie banners, privacy notices, or generic site headers/footers.
- Ignore phrases like "Do Not Sell or Share My Personal Data", "Cookie Preferences", "Privacy Policy", etc.
- Prefer the heading that clearly describes the role (for example, an <h1> like "Senior Software Engineer, Machine Learning").
- The title should be concise and suitable for an email digest.

Return ONLY valid JSON:

{
  "job_title": string,
  "min_years": number
}
"""

KEYWORDS = [
    "experience",
    "years",
    "qualification",
    "requirement",
    "responsibil",
    "minimum",
    "preferred"
]

def trim_text(text, max_chars=8000):
    lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 20]
    keep = []

    for line in lines:
        lower = line.lower()
        if any(k in lower for k in KEYWORDS):
            keep.append(line)

    trimmed = "\n".join(keep)
    if len(trimmed) < 500:
        trimmed = text[:max_chars]

    return trimmed[:max_chars]

def extract_min_years(job_text, scraped_title=""):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"SCRAPED_TITLE: {scraped_title}\n\n"
                    f"JOB_DESCRIPTION:\n{job_text}"
                ),
            },
        ],
    )

    raw = response.choices[0].message.content

    try:
        data = json.loads(raw)
    except Exception:
        return {"job_title": scraped_title, "min_years": 0}

    years = data.get("min_years", 0)
    if not isinstance(years, (int, float)) or years < 0:
        years = 0

    job_title = str(data.get("job_title") or scraped_title or "").strip()

    return {
        "job_title": job_title,
        "min_years": int(years),
    }

def main():
    raw_input = sys.stdin.read()
    data = json.loads(raw_input)

    title = data.get("job_title", "")
    text = data.get("text", "") or data.get("page_text", "")

    trimmed = trim_text(text)
    result = extract_min_years(trimmed, scraped_title=title)

    output = {
        "job_title": result.get("job_title", title),
        "min_years": int(result.get("min_years", 0) or 0),
    }

    print(json.dumps(output))

if __name__ == "__main__":
    main()
