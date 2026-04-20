import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { config } from './config.js';
import { DEFAULT_CHROME_USER_AGENT } from './userAgents.js';

puppeteer.use(StealthPlugin());

/**
 * @param {{ userDataDir?: string } | undefined} [options]
 */
export async function launchBrowser(options = {}) {
  const incognito = Boolean(config.mlBrowserIncognito);
  const userDataDir = incognito ? undefined : options.userDataDir ?? config.userDataDir;
  const userAgent = config.browserUserAgent || DEFAULT_CHROME_USER_AGENT;

  /** @type {string[]} */
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--window-size=1366,768',
    `--user-agent=${userAgent}`,
    '--disable-dev-shm-usage',
  ];
  if (incognito) {
    args.push('--incognito');
    console.info(
      '[browser] ML_BROWSER_INCOGNITO=true — janela anónima, sem perfil persistente (cookies/sessão não são guardados entre execuções).'
    );
  }

  /** @type {import('puppeteer').LaunchOptions} */
  const launchOpts = {
    headless: config.headless,
    args,
  };
  if (userDataDir) {
    launchOpts.userDataDir = userDataDir;
  }

  const browser = await puppeteer.launch(launchOpts);

  return { browser, userAgent };
}
