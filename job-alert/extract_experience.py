import sys
import json
import os
from pathlib import Path
from dotenv import load_dotenv

SECRETS_FILE = Path(__file__).resolve().parents[1] / "state" / "secrets.env"
load_dotenv(dotenv_path=SECRETS_FILE, override=False)

from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError(f"Missing OPENAI_API_KEY in secrets file: {SECRETS_FILE}")

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
You are an information extraction system.

Your task:
Extract the MINIMUM required years of professional experience from the job description.

Rules:
- Ignore degrees entirely (BS, MS, PhD).
- Ignore preferred qualifications unless explicitly required.
- If multiple experience numbers appear, choose the LOWEST.
- Do NOT infer from job title.
- If entry-level or new grad, return 0.
- If unclear, return 0.
- Never guess.

Return ONLY valid JSON:

{
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

def extract_min_years(job_text):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": job_text},
        ],
    )

    raw = response.choices[0].message.content

    try:
        data = json.loads(raw)
        years = data.get("min_years", 0)
        if not isinstance(years, (int, float)) or years < 0:
            return 0
        return int(years)
    except:
        return 0

def main():
    raw_input = sys.stdin.read()
    data = json.loads(raw_input)

    title = data.get("job_title", "")
    text = data.get("text", "") or data.get("page_text", "")

    trimmed = trim_text(text)
    min_years = extract_min_years(trimmed)

    output = {
        "job_title": title,
        "min_years": min_years
    }

    print(json.dumps(output))

if __name__ == "__main__":
    main()
