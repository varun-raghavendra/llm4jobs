const puppeteer = require("puppeteer");
const fs = require("fs");
const path = require("path");

function isHttpUrl(value) {
  if (typeof value !== "string") return false;
  try {
    const u = new URL(value.trim());
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

// ---------------- CONFIG ----------------

// OPTION A: hardcode URLs here
const JOB_URLS = [
  "https://www.mathworks.com/company/jobs/opportunities/36810-software-engineer-in-test?job_type_id%5B%5D=1756&job_type_id%5B%5D=1754&keywords=&location%5B%5D=US&posting_team_id%5B%5D=6&posting_team_id%5B%5D=5&posting_team_id%5B%5D=32&posting_team_id%5B%5D=12&posting_team_id%5B%5D=12&posting_team_id%5B%5D=3&posting_team_id%5B%5D=13&posting_team_id%5B%5D=1&posting_team_id%5B%5D=11&posting_team_id%5B%5D=8&posting_team_id%5B%5D=4&posting_team_id%5B%5D=7&posting_team_id%5B%5D=10&posting_team_id%5B%5D=9&posting_team_id%5B%5D=2&sort_order=DATE+DESC&sort_origin=user",
  "https://www.metacareers.com/profile/job_details/677160418622314",
  "https://nvidia.eightfold.ai/careers?start=0&location=US&pid=893392492016&sort_by=distance&filter_include_remote=1",
  "https://www.tesla.com/careers/search/job/ai-engineer-manipulation-optimus-224501",
  "https://intel.wd1.myworkdayjobs.com/en-US/External/job/US-Arizona-Phoenix/Senior-Analog---Mixed-Signal-Application-Engineer_JR0279353",
  "https://careers.adobe.com/us/en/job/R160035/ML-focused-Site-Reliability-Engineer-Developer-Platforms",
  "https://jobs.apple.com/en-us/details/200619215-3760/sr-machine-learning-engineer-ml-efficiency-ml-platform-technology?team=MLAI",
  "https://www.capgemini.com/jobs/393803-en_US+sap_btp/",
  "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/210625213",
  "https://careers.hcltech.com/job/Track-Manager-Windows-Azure-IaaS%2C-Terraform/12295-en_US",
  "https://jobs.intuit.com/job/mountain-view/staff-machine-learning-engineer/27595/87369450000",
  "https://lifeattiktok.com/search/7533745453742000402",
  "https://careers.walmart.com/us/en/jobs/CP-5263-9076",
  "https://careers.snap.com/job?id=Q126SWEI6",
  "https://www.databricks.com/company/careers/engineering---pipeline/software-engineer---genai-inference--8202670002",
  "https://careers.qualcomm.com/careers?query=software&location=united%20states&pid=446715647661&domain=qualcomm.com&sort_by=relevance",
  "https://careers.salesforce.com/en/jobs/jr326192/solution-engineer-agentforce-data-360-specialist/",
  "https://www.accenture.com/us-en/careers/jobdetails?id=R00302607_en&title=Workday+Certified+Advanced+Comp+Lead",
  "https://www.kpmguscareers.com/jobdetail/?jobId=131025"
];

// OPTION B (recommended): read from urls.txt
// const JOB_URLS = fs.readFileSync("urls.txt", "utf-8")
//   .split("\n")
//   .map(l => l.trim())
//   .filter(Boolean);

// text length threshold to decide "usable"
const MIN_CHARS = 500;

// output folder
const OUTPUT_DIR = path.join(__dirname, "results");

// ----------------------------------------

async function scrapeJob(page, url) {
  if (!isHttpUrl(url)) {
    return "";
  }

  try {
    await page.goto(url, {
      waitUntil: "networkidle2",
      timeout: 60000,
    });

    await new Promise(r => setTimeout(r, 3000));

    const text = await page.evaluate(() => document.body.innerText || "");
    return text;

  } catch (err) {
    return "";
  }
}

(async () => {
  // ensure results folder exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR);
  }

  const works = [];
  const failed = [];

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });

  const page = await browser.newPage();

  // speed optimizations
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    if (["image", "stylesheet", "font", "media"].includes(req.resourceType())) {
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

  for (const url of JOB_URLS) {
    if (!isHttpUrl(url)) {
      console.log(`❌ SKIP invalid protocol: ${url}`);
      failed.push(`${url} | invalid_protocol`);
      continue;
    }
    console.log(`Checking: ${url}`);

    const text = await scrapeJob(page, url);
    const len = text.trim().length;

    if (len >= MIN_CHARS) {
      console.log(`✅ OK (${len} chars)`);
      works.push(`${url} | ${len}`);
    } else {
      console.log(`❌ FAIL (${len} chars)`);
      failed.push(`${url} | ${len}`);
    }
  }

  await browser.close();

  // write files
  fs.writeFileSync(
    path.join(OUTPUT_DIR, "works.txt"),
    works.join("\n"),
    "utf-8"
  );

  fs.writeFileSync(
    path.join(OUTPUT_DIR, "failed.txt"),
    failed.join("\n"),
    "utf-8"
  );

  const summary = `
TOTAL URLS: ${JOB_URLS.length}
WORKS: ${works.length}
FAILED: ${failed.length}

=== WORKS ===
${works.join("\n")}

=== FAILED ===
${failed.join("\n")}
`;

  fs.writeFileSync(
    path.join(OUTPUT_DIR, "summary.txt"),
    summary.trim(),
    "utf-8"
  );

  console.log("\n📁 Results written to /results folder");
})();
