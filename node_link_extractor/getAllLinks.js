import puppeteer from "puppeteer";

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function shouldDisableSandbox() {
  const raw = (process.env.PUPPETEER_DISABLE_SANDBOX || process.env.PPT_DISABLE_SANDBOX || "")
    .trim()
    .toLowerCase();
  const envWantsDisable = raw === "1" || raw === "true" || raw === "yes";
  const isRoot = typeof process.getuid === "function" && process.getuid() === 0;
  // In Docker this often runs as root; Chromium will refuse to start unless sandbox is disabled.
  return envWantsDisable || isRoot;
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

async function launchBrowser() {
  const args = [
    "--window-size=1280,800",
    "--disable-blink-features=AutomationControlled"
  ];

  if (shouldDisableSandbox()) {
    args.unshift("--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage");
  }

  return puppeteer.launch({
    headless: "new",
    args
  });
}

async function createPage(browser) {
  const page = await browser.newPage();

  await page.setViewport({ width: 1280, height: 800 });

  await page.setUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
  );

  await page.setExtraHTTPHeaders({
    "Accept-Language": "en-US,en;q=0.9"
  });

  return page;
}

async function renderPage(page, url) {
  const resp = await page.goto(url, {
    waitUntil: ["domcontentloaded", "networkidle2"],
    timeout: 60000
  });

  const status = resp ? resp.status() : null;
  const finalUrl = page.url();
  const title = await page.title();

  return { status, finalUrl, title };
}

async function waitForStableDom(page) {
  await page.waitForFunction(() => document.readyState === "complete", { timeout: 30000 });

  try {
    await page.waitForSelector("main", { timeout: 30000 });
  } catch {
  }

  await sleep(1000);

  for (let i = 0; i < 6; i++) {
    await page.evaluate(() => window.scrollBy(0, Math.floor(window.innerHeight * 0.9)));
    await sleep(400);
  }

  try {
    await page.waitForFunction(
      () => document.querySelectorAll("a[href]").length > 0,
      { timeout: 15000 }
    );
  } catch {
  }

  await sleep(800);
}

async function extractLinks(page) {
  return page.evaluate(() => {
    const anchors = Array.from(document.querySelectorAll("a[href]"));
    return anchors
      .map(a => a.href)
      .filter(href => href && href.trim().length > 0)
      .filter(href => !href.startsWith("https://www.amazon.jobs/en/search"));
  });
}

export async function getAllLinks(url) {
  if (!isHttpUrl(url)) {
    throw new Error(`Invalid URL protocol: ${url}`);
  }

  const browser = await launchBrowser();

  try {
    const page = await createPage(browser);

    const nav = await renderPage(page, url);
    await waitForStableDom(page);

    const links = (await extractLinks(page)).filter(isHttpUrl);

    if (links.length === 0) {
      const bodyText = await page.evaluate(() => (document.body && document.body.innerText) ? document.body.innerText.slice(0, 800) : "");
      console.log("Headless debug");
      console.log({ ...nav, bodyText });
      await page.screenshot({ path: "headless_debug.png", fullPage: true });
    }

    return links;
  } finally {
    await browser.close();
  }
}
