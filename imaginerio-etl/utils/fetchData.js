const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

(async () => {
    function delay(time) {
        return new Promise(function (resolve) {
            setTimeout(resolve, time)
        });
    }
    try {
        // Launch the browser
        console.log('launching browser')
        const browser = await puppeteer.launch({ headless: true });

        console.log('creating new page')
        const page = await browser.newPage();

        console.log('setting viewport')
        await page.setViewport({ width: 1280, height: 720 });

        console.log('setting download directory')
        const client = await page.target().createCDPSession()
        await client.send('Page.setDownloadBehavior', {
            behavior: 'allow',
            downloadPath: './jstor_download',
        })

        // Navigate to the login page
        console.log('logging in')
        await page.goto('https://forum.jstor.org/#/login/by-email');
        // Enter login information
        await page.type('#username', process.env.JSTOR_USER);
        await page.type('#password', process.env.JSTOR_PASSWORD);
        await page.click('#loginButton');

        // Go to project page
        console.log('navigating to project page')
        await page.waitForNavigation();
        await page.goto(process.env.JSTOR_PROJECT)

        // Select checkbox
        console.log('selecting items')
        const checkboxSelector = ".ag-header-select-all:nth-child(1)"
        await page.waitForSelector(checkboxSelector);
        await delay(2000)
        const checkboxHandle = await page.$(checkboxSelector)
        await checkboxHandle.click()

        // Select all
        const selectAllSelector = ".select-all-text .btn"
        await page.waitForSelector(selectAllSelector)
        await page.click(selectAllSelector)

        // Export data
        console.log('exporting data')
        const dropdownSelector = ".more-actions-panel .btn"
        await page.waitForSelector(dropdownSelector)
        await page.click(dropdownSelector)

        // Wait for download
        await delay(10000)

        // Rename downloaded file
        const file = fs.readdirSync('./jstor_download')[0];
        console.log(file)
        //fs.renameSync(`./jstor_download/${file}`, './input/jstor.xls');

        // End session
        await browser.close();

    } catch (error) {
        console.error(error)
    }
})();
