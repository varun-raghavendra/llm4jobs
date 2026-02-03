# Job Alert System (Puppeteer + LLM)

This repo tracks job postings and extracts:
- Job title
- Minimum required years of experience

The system is designed for **early-career roles** and is intentionally conservative.
If experience requirements are unclear, it returns `0`.

---

## Architecture (Single Reliable Path)

We use **ONLY a Puppeteer-based scraper**.

### Why Puppeteer Only

Many enterprise job sites use:
- Heavy JavaScript rendering
- Cookie banners (TrustArc / OneTrust)
- iframes and shadow DOM
- 403 / 400 blocking of non-browser requests

Static scrapers were unreliable and dropped valid jobs.

**Puppeteer is slower but correct**, and correctness matters more than speed.

---

## Pipeline Overview

```
Job URL
  ↓
Puppeteer (real browser)
  - handles cookies
  - renders JavaScript
  - extracts job title + full page text
  ↓
Python (LLM extraction)
  - trims noisy text
  - extracts minimum required experience
  - never infers seniority
  ↓
JSON output
```

---

## Folder Structure

```
JOB-ALERT/
├── puppeteer_scraper/
│   ├── puppeteer_scrapper.js   # main scraper (Node + Puppeteer)
│   ├── scrape_batch.js         # batch runner (WIP)
│   ├── node_modules/           # ignored
│   └── results/                # ignored (screenshots/logs)
├── extract_experience.py       # LLM experience extraction (Python)
├── .env                        # OpenAI key (NOT committed)
├── venv/                       # Python virtualenv (NOT committed)
└── README.md
```

---

## Setup Instructions

### 1. Python setup

```bash
python -m venv venv
source venv/bin/activate        # Windows: .\venv\Scripts\activate
pip install openai python-dotenv
```

### 2. Node setup

```bash
cd puppeteer_scraper
npm install
```

### 3. OpenAI key

Create a `.env` file at the project root:

```
OPENAI_API_KEY=sk-...
```

---

## Running the Pipeline

From the project root:

```bash
node puppeteer_scraper/puppeteer_scrapper.js "JOB_URL" | python extract_experience.py
```

### Example output

```json
{
  "job_title": "Software Engineer",
  "min_years": 2
}
```

If experience is not explicitly required, `min_years` will be `0`.

---

## Design Rules (Important)

- Do **NOT** infer seniority from job title
- Ignore degrees completely (BS / MS / PhD)
- If multiple experience numbers exist, choose the **lowest**
- If unclear → return `0`
- Puppeteer output is intentionally noisy; Python trims it

---

## Notes for Contributors

- Do **NOT** commit `.env`
- Puppeteer is the single source of truth
- Avoid adding static scrapers unless reliability is proven
- Batch processing and scheduling will be added later
