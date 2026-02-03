const puppeteer = require("puppeteer");

// --------- cookie helpers ---------

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isHttpUrl(value) {
  if (typeof value !== "string") return false;
  try {
    const u = new URL(value.trim());
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

// Runs inside the page context (DOM). Finds a clickable element by text,
// including inside shadow DOM.
function findClickableByTextInDom(textRegexSource) {
  const re = new RegExp(textRegexSource, "i");

  function isVisible(el) {
    const style = window.getComputedStyle(el);
    if (!style) return false;
    if (style.visibility === "hidden" || style.display === "none") return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function matches(el) {
    const t = (el.innerText || el.textContent || "").trim();
    return t && re.test(t);
  }

  function candidates(root) {
    const arr = [];
    const clickables = root.querySelectorAll(
      'button, [role="button"], input[type="button"], input[type="submit"], a'
    );
    for (const el of clickables) {
      if (matches(el) && isVisible(el)) arr.push(el);
    }
    return arr;
  }

  function traverse(root) {
    const direct = candidates(root);
    if (direct.length) return direct[0];

    const all = root.querySelectorAll("*");
    for (const el of all) {
      if (el.shadowRoot) {
        const hit = traverse(el.shadowRoot);
        if (hit) return hit;
      }
    }
    return null;
  }

  return traverse(document);
}

async function clickCookieAcceptInFrame(frame) {
  const knownSelectors = [
    "#onetrust-accept-btn-handler",
    "#truste-consent-button",
    '[data-testid*="accept"]',
    '[aria-label*="accept"]',
  ];

  for (const sel of knownSelectors) {
    try {
      const el = await frame.$(sel);
      if (el) {
        await el.click({ delay: 30 });
        return true;
      }
    } catch {}
  }

  try {
    const clicked = await frame.evaluate(() => {
      const el = findClickableByTextInDom("^(accept|accept all|i accept|agree)$");
      if (el) {
        el.click();
        return true;
      }
      return false;
    });
    return !!clicked;
  } catch {
    return false;
  }
}

async function acceptCookies(page) {
  try {
    await page.waitForFunction(() => {
      const el = findClickableByTextInDom("(accept|accept all|agree|i accept)");
      return !!el;
    }, { timeout: 8000 });
  } catch {}

  if (await clickCookieAcceptInFrame(page.mainFrame())) {
    await delay(1500);
    return true;
  }

  for (const frame of page.frames()) {
    if (frame === page.mainFrame()) continue;
    if (await clickCookieAcceptInFrame(frame)) {
      await delay(1500);
      return true;
    }
  }

  return false;
}

// --------- main scrape ---------

async function scrapeJob(url) {
  if (!isHttpUrl(url)) {
    throw new Error(`Invalid URL protocol: ${url}`);
  }

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });

  const page = await browser.newPage();

  // Speed optimizations
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const type = req.resourceType();
    if (["image", "stylesheet", "font", "media"].includes(type)) {
      req.abort();
    } else {
      req.continue();
    }
  });

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) " +
    "Chrome/121.0.0.0 Safari/537.36"
  );

  await page.goto(url, {
    waitUntil: "networkidle2",
    timeout: 60000,
  });

  await acceptCookies(page);
  await delay(2000);

  // ✅ Extract job title (DOM-first)
  const jobTitle = await page.evaluate(() => {
    const h1 = document.querySelector("h1");
    if (h1 && h1.innerText.trim().length > 3) {
      return h1.innerText.trim();
    }

    const h2 = document.querySelector("h2");
    if (h2 && h2.innerText.trim().length > 3) {
      return h2.innerText.trim();
    }

    return document.title || "";
  });

  // ✅ Extract rendered text
  const text = await page.evaluate(() => document.body.innerText || "");

  await browser.close();

  return {
    job_title: jobTitle,
    text: text
  };
}

// --------- CLI runner ---------

(async () => {
  const url = process.argv[2];
  if (!url) {
    console.error("❌ Please provide a job URL");
    process.exit(1);
  }
  if (!isHttpUrl(url)) {
    console.error("❌ URL must start with http:// or https://");
    process.exit(1);
  }

  try {
    const result = await scrapeJob(url);

    // 🚨 IMPORTANT: print JSON ONLY
    console.log(JSON.stringify(result));
  } catch (err) {
    console.error("❌ Puppeteer failed:", err);
    process.exit(1);
  }
})();
