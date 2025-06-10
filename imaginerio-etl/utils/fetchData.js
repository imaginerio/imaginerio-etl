const puppeteer = require("puppeteer");
const fs = require("fs");
const path = require("path");

const downloadDir = "./data/jstor_download";
const dataDir = "./data/input";

// Check required environment variables
const requiredEnvVars = [
  "JSTOR_USER",
  "JSTOR_PASSWORD",
  "JSTOR_PROJECT",
  "JSTOR_VOCABULARY",
];
const missingEnvVars = requiredEnvVars.filter(
  (varName) => !process.env[varName]
);
if (missingEnvVars.length > 0) {
  console.warn(
    "⚠️ Missing required environment variables:",
    missingEnvVars.join(", ")
  );
  process.exit(1);
}

function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

function waitForDownloadCompletion(timeout = 30000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();

    // Initial count of downloaded (complete) files
    const initialFiles = fs
      .readdirSync(downloadDir)
      .filter((f) => !f.endsWith(".crdownload") && !f.endsWith(".tmp")).length;

    const interval = setInterval(() => {
      const currentFiles = fs.readdirSync(downloadDir);
      const downloading = currentFiles.filter(
        (f) => f.endsWith(".crdownload") || f.endsWith(".tmp")
      );
      const completedFiles = currentFiles.filter(
        (f) => !f.endsWith(".crdownload") && !f.endsWith(".tmp")
      );

      if (downloading.length === 0 && completedFiles.length > initialFiles) {
        clearInterval(interval);
        resolve(completedFiles); // You can filter/rename as needed afterwards
      }

      if (Date.now() - start > timeout) {
        clearInterval(interval);
        reject(new Error("Download timeout exceeded"));
      }
    }, 500);
  });
}

async function typeWithRetry(page, selector, value, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    await page.evaluate((sel) => {
      const input = document
        .querySelector(sel)
        .shadowRoot.querySelector("input");
      input.value = "";
      input.dispatchEvent(new Event("input", { bubbles: true }));
    }, selector);

    await page.type(`${selector} >>> input`, value, { delay: 100 });

    const typedValue = await page.evaluate((sel) => {
      return document.querySelector(sel).shadowRoot.querySelector("input")
        .value;
    }, selector);

    if (typedValue === value) return;

    console.warn(`Retrying typing into ${selector}, attempt ${attempt + 1}`);
    await delay(500);
  }

  throw new Error(
    `Failed to type value into ${selector} after ${maxRetries} attempts`
  );
}

async function login(page) {
  await page.goto("https://forum.jstor.org/#/login/by-email", {
    waitUntil: "networkidle2",
  });
  await page.waitForSelector("forum-ui-pharos-text-input#username");

  await page.evaluate(() => {
    document
      .querySelector("forum-ui-pharos-text-input#username")
      .shadowRoot.querySelector("input")
      .focus();
  });
  await typeWithRetry(
    page,
    "forum-ui-pharos-text-input#username",
    process.env.JSTOR_USER
  );

  await page.evaluate(() => {
    document
      .querySelector("forum-ui-pharos-text-input#password")
      .shadowRoot.querySelector("input")
      .focus();
  });
  await typeWithRetry(
    page,
    "forum-ui-pharos-text-input#password",
    process.env.JSTOR_PASSWORD
  );

  await page.evaluate(() => {
    document.querySelector("forum-ui-pharos-button#loginButton").click();
  });

  await page
    .waitForNavigation({ waitUntil: "networkidle2", timeout: 15000 })
    .catch(() => {
      console.warn("Login redirect timeout; checking manually...");
    });
}

async function exportFromPage(page) {
  console.log("Selecting all items...");
  
  // Wait for the grid and checkbox to be ready
  await page.waitForSelector('.ag-header-select-all.ag-checkbox', {
    timeout: 30000,
    visible: true
  });
  
  await delay(2000);

  // Click the checkbox using the more specific selector
  await page.evaluate(() => {
    const checkbox = document.querySelector('.ag-header-select-all.ag-checkbox');
    if (!checkbox) throw new Error('Select all checkbox not found');
    
    // Find and click the checkbox or its icon
    const icon = checkbox.querySelector('.ag-icon-checkbox-unchecked');
    if (icon) {
      icon.click();
    } else {
      checkbox.click();
    }
  });

  await delay(2000);
  
  console.log('Clicking "select all in project"...');
  await page.waitForFunction(() => {
    const panel = document.querySelector(".select-all-panel.visible");
    return (
      panel &&
      panel
        .querySelector("forum-ui-pharos-button")
        ?.shadowRoot?.querySelector("#button-element")
    );
  });
  
  await page.evaluate(() => {
    const button = document
      .querySelector(".select-all-panel.visible")
      .querySelector("forum-ui-pharos-button")
      .shadowRoot.querySelector("#button-element");
    button.click();
  });
  await delay(2000);
  console.log("Exporting...");
  await page.evaluate(() => {
    const exportBtn = Array.from(
      document
        .querySelector(".more-actions-panel, .bulk-actions-panel")
        .querySelectorAll("forum-ui-pharos-button")
    ).find((el) => el.textContent.trim().includes("Export"));
    exportBtn?.shadowRoot?.querySelector("button")?.click();
  });

  console.log("Waiting for download to complete...");
  await waitForDownloadCompletion();
}

function renameDownloadedFiles(downloadDir) {
  const files = fs.readdirSync(downloadDir);

  files.forEach((file) => {
    const oldPath = path.join(downloadDir, file);

    if (file.includes("assets")) {
      const newPath = path.join(downloadDir, "jstor.xls");
      fs.renameSync(oldPath, newPath);
    }

    if (file.includes("terms")) {
      const newPath = path.join(dataDir, "vocabulary.xls");
      fs.renameSync(oldPath, newPath);
    }
  });
}

(async () => {
  try {
    if (fs.existsSync(downloadDir)) {
      console.log(
        `Directory ${downloadDir} already exists, using existing directory`
      );
    } else {
      fs.mkdirSync(downloadDir, { recursive: true });
      console.log(`Created directory ${downloadDir}`);
    }

    console.log("Launching browser...");
    const browser = await puppeteer.launch({
      headless: true,
      slowMo: 100, // Increase from 50 to 100
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage', // Add this for CI environments
        '--disable-gpu', // Add this for CI environments
        '--window-size=1280,720'
      ],
      defaultViewport: {
        width: 1280,
        height: 720
      }
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 720 });

    const client = await page.target().createCDPSession();
    await client.send("Page.setDownloadBehavior", {
      behavior: "allow",
      downloadPath: downloadDir,
    });

    await login(page);

    console.log("Navigating to project...");
    await page.goto(process.env.JSTOR_PROJECT, { waitUntil: "networkidle2" });
    await exportFromPage(page);

    console.log("Navigating to vocabulary...");
    await page.goto(process.env.JSTOR_VOCABULARY, {
      waitUntil: "networkidle2",
    });
    await exportFromPage(page);

    renameDownloadedFiles(downloadDir);
    console.log("✅ All done!");
    await browser.close();
  } catch (error) {
    console.error("❌ Error during Puppeteer automation:", error);
  }
})();
