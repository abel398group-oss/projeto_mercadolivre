import { launchBrowser } from '../browser.js';
import { sleep } from '../util.js';

const NODE_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  Accept: 'application/json',
  'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
};

/**
 * Cliente para api.mercadolibre.com: fetch em Node; em 403 usa Puppeteer com page.goto
 * (o site ML substitui window.fetch — evaluate+fetch quebra com "Failed to fetch").
 */
export class MlApiClient {
  /** @param {{ forceBrowser?: boolean; delayMs?: number }} opts */
  constructor(opts = {}) {
    this.forceBrowser = Boolean(opts.forceBrowser);
    this.delayMs = opts.delayMs ?? 150;
    /** @type {import('puppeteer').Browser | null} */
    this._browser = null;
    /** @type {import('puppeteer').Page | null} */
    this._page = null;
    this._useBrowser = Boolean(opts.forceBrowser);
  }

  async dispose() {
    if (this._browser) {
      await this._browser.close().catch(() => {});
      this._browser = null;
      this._page = null;
    }
  }

  async _ensureBrowser() {
    if (this._page) return;
    const { browser } = await launchBrowser();
    this._browser = browser;
    const page = await browser.newPage();
    await page.setExtraHTTPHeaders({
      Accept: 'application/json',
      'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
    });
    await page.goto('about:blank');
    this._page = page;
    this._useBrowser = true;
    console.info('[ml-catalog] API via browser (about:blank + fetch; fallback page.goto)');
  }

  /**
   * Pedidos à API no browser: evita página do ML (substitui fetch).
   * Se falhar (CORS, etc.), usa page.goto no URL da API.
   * @param {string} url
   */
  async _getJsonViaBrowser(url) {
    const page = this._page;
    if (!page) throw new Error('Puppeteer: página indisponível');

    try {
      return await page.evaluate(async (u) => {
        const r = await fetch(u, { method: 'GET', mode: 'cors', credentials: 'omit' });
        const text = await r.text();
        if (!r.ok) {
          throw new Error(`HTTP ${r.status} ${text.slice(0, 200)}`);
        }
        return JSON.parse(text);
      }, url);
    } catch (firstErr) {
      const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 90_000 });
      if (!resp) throw firstErr;
      const text = await resp.text();
      if (!resp.ok()) {
        throw new Error(`${resp.status()} ${text.slice(0, 240)}`);
      }
      return JSON.parse(text);
    }
  }

  /**
   * @param {string} url
   * @param {number} attempt
   * @returns {Promise<unknown>}
   */
  async getJson(url, attempt = 0) {
    if (this.delayMs > 0) await sleep(this.delayMs);

    if (this.forceBrowser || this._useBrowser) {
      await this._ensureBrowser();
      return this._getJsonViaBrowser(url);
    }

    const r = await fetch(url, { headers: NODE_HEADERS });
    if (r.status === 403 && attempt === 0) {
      console.warn('[ml-catalog] API 403 em Node — a mudar para browser (page.goto)…');
      this.forceBrowser = true;
      return this.getJson(url, 1);
    }
    if (r.status === 429 || r.status === 503) {
      const wait = Math.min(30_000, 2000 * 2 ** attempt);
      console.warn(`[ml-catalog] ${r.status} — a aguardar ${wait}ms e repetir…`);
      await sleep(wait);
      return this.getJson(url, attempt + 1);
    }
    const text = await r.text();
    if (!r.ok) {
      throw new Error(`GET ${url} → ${r.status} ${text.slice(0, 240)}`);
    }
    return JSON.parse(text);
  }
}
