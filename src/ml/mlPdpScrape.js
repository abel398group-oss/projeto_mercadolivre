import { launchBrowser } from '../browser.js';
import { config } from '../config.js';
import { emptyProduct } from '../productSchema.js';
import { sleep } from '../util.js';
import {
  fetchMlCatalogProduct,
  fetchMlItem,
  normalizeItemApiToPartial,
  normalizeProductApiToPartial,
} from './mlApiItem.js';
import { mergeWithFieldSources } from './mlCanonicalMerge.js';
import {
  assertUsableMlPdpUrl,
  buildJsonLdPartial,
  buildSyntheticJsonLdProductNode,
  extractMlProductIdFromUrl,
  jsonLdIsProductType,
} from './mlExtract.js';
import { parseIdentityFromUrl } from './mlIdentity.js';
import { extractMlPdpFromDom, normalizeMlPdpDomToPartial } from './mlPdpDomExtract.js';
import { finalizeAndValidate } from './mlProductFinalize.js';
import { buildEmbeddedPartialFromHtml } from './mlEmbeddedStateExtract.js';
import {
  browserDetectMlAccountChallenge,
  challengeUrlLooksBlocked,
  mlAccountVerificationErrorMessage,
} from './mlAccountChallenge.js';

/** @param {unknown} v */
function s(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * Lê e faz parse de todos os scripts JSON-LD; devolve o primeiro nó Product encontrado.
 * Executado no contexto da página (serializável).
 */
function collectProductJsonLdInPage() {
  function typeIsProduct(typeField) {
    if (typeField == null) return false;
    const list = Array.isArray(typeField) ? typeField : [typeField];
    return list.some((raw) => {
      const s = String(raw).toLowerCase().trim();
      if (!s) return false;
      if (s === 'product') return true;
      if (s.endsWith('/product')) return true;
      if (s.endsWith('#product')) return true;
      return false;
    });
  }

  /** @type {unknown[]} */
  const candidates = [];
  const scripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const el of scripts) {
    const raw = el.textContent?.trim();
    if (!raw) continue;
    try {
      candidates.push(JSON.parse(raw));
    } catch {
      /* ignorar bloco inválido */
    }
  }
  for (const parsed of candidates) {
    const nodes = [];
    const flatten = (data) => {
      if (data == null) return;
      if (Array.isArray(data)) {
        for (const x of data) flatten(x);
        return;
      }
      if (typeof data === 'object') {
        const g = /** @type {Record<string, unknown>} */ (data)['@graph'];
        if (g != null) {
          flatten(g);
          return;
        }
        nodes.push(data);
      }
    };
    flatten(parsed);
    for (const node of nodes) {
      if (!node || typeof node !== 'object') continue;
      const t = /** @type {Record<string, unknown>} */ (node)['@type'];
      if (typeIsProduct(t)) return node;
    }
  }
  return null;
}

/**
 * PDP Mercado Livre — API → embedded_json → JSON-LD → DOM + consolidação.
 *
 * @param {string} productUrl
 * @param {{
 *   browser?: import('puppeteer').Browser;
 *   page?: import('puppeteer').Page;
 *   keepBrowserOpen?: boolean;
 *   skipWarmup?: boolean;
 * } | undefined} [options]
 * Opção `page`: quando passada (ex.: bulk), reutiliza a mesma aba; não fecha página nem browser no `finally`.
 * @returns {Promise<import('../productSchema.js').CanonicalProduct & { price_currency: string }>}
 */
export async function scrapeMlPdp(productUrl, options = {}) {
  if (!productUrl || typeof productUrl !== 'string') {
    throw new TypeError('scrapeMlPdp: productUrl é obrigatório');
  }

  assertUsableMlPdpUrl(productUrl);

  console.info('[ml-pdp] API + embedded_json + JSON-LD + DOM');

  const keepBrowserOpen =
    options.keepBrowserOpen !== undefined ? Boolean(options.keepBrowserOpen) : config.keepBrowserOpen;

  const ownBrowser = !options.browser;
  let browser = options.browser;

  if (ownBrowser) {
    const launched = await launchBrowser();
    browser = launched.browser;
  }

  if (!browser) {
    throw new Error('scrapeMlPdp: browser indisponível');
  }

  /** Bulk: `page` injetada pelo runMlPdpBulk — uma aba para todo o lote (só `goto` muda). */
  const reuseExternalPage = options.page != null;

  const identity = parseIdentityFromUrl(productUrl);
  /** @type {import('../productSchema.js').CanonicalProduct} */
  let base = emptyProduct();

  const apiDelay = async () => {
    const d = Math.max(0, config.mlApiDelayMs);
    if (d > 0) await sleep(d);
  };

  if (identity.item_id) {
    const jItem = await fetchMlItem(identity.item_id);
    if (jItem) {
      base = mergeWithFieldSources(base, normalizeItemApiToPartial(jItem, productUrl), 'api_item');
    }
    await apiDelay();
  }

  if (identity.catalog_product_id) {
    const jProd = await fetchMlCatalogProduct(identity.catalog_product_id);
    if (jProd) {
      base = mergeWithFieldSources(
        base,
        normalizeProductApiToPartial(jProd, identity.catalog_product_id, productUrl),
        'api_product'
      );
    }
    await apiDelay();
  }

  const mergedItemId = String(base.item_id || '').trim();
  if (!identity.item_id && mergedItemId && /^MLB/i.test(mergedItemId)) {
    const jItem2 = await fetchMlItem(mergedItemId);
    if (jItem2) {
      base = mergeWithFieldSources(base, normalizeItemApiToPartial(jItem2, productUrl), 'api_item');
    }
    await apiDelay();
  }

  const page = reuseExternalPage ? /** @type {import('puppeteer').Page} */ (options.page) : await browser.newPage();
  /** @type {(import('../productSchema.js').CanonicalProduct & { price_currency: string }) | undefined} */
  let scrapeResult;
  try {
    const skipWarmup = Boolean(options.skipWarmup);
    const warmup = skipWarmup ? '' : String(config.mlPdpWarmupUrl || '').trim();
    if (warmup) {
      try {
        console.info(`[ml-pdp] visita inicial: ${warmup} (warm-up antes do produto)`);
        await page.goto(warmup, { waitUntil: 'domcontentloaded', timeout: 60_000 });
        const d = Math.max(0, config.mlPdpWarmupDelayMs);
        if (d > 0) await new Promise((r) => setTimeout(r, d));
      } catch (e) {
        console.warn('[ml-pdp] warm-up falhou (continuo para o produto):', e instanceof Error ? e.message : e);
      }
    }

    console.info('[ml-pdp] a abrir o anúncio…');
    await page.goto(productUrl, { waitUntil: 'domcontentloaded', timeout: 90_000 });

    let looksBlocked =
      challengeUrlLooksBlocked(page.url()) || (await page.evaluate(browserDetectMlAccountChallenge));

    if (looksBlocked && config.mlPdpFailFastOnAccountChallenge) {
      throw new Error(mlAccountVerificationErrorMessage('pdp'));
    }

    let extraWaitMs = Math.max(0, config.mlPdpWaitLoginMs);
    if (looksBlocked && !config.headless && config.mlPdpAutoWaitLoginWhenVisible) {
      extraWaitMs = Math.max(extraWaitMs, config.mlPdpAutoWaitLoginMaxMs);
    }
    if (looksBlocked && extraWaitMs > 0) {
      console.info(
        `[ml-pdp] O ML mostrou verificação ou pedido de login. Completa na janela do Chrome; a aguardar até ${Math.round(
          extraWaitMs / 1000
        )}s pelo carregamento do produto…`
      );
      console.info(
        '[ml-pdp] Dica: para o browser não fechar nunca após o scrape, usa ML_KEEP_BROWSER_OPEN=true ou npm run start:open'
      );
    }

    /** @type {unknown} */
    let productNode = null;
    let htmlSnapshot = '';
    /** @type {ReturnType<typeof buildEmbeddedPartialFromHtml> | null} */
    let embeddedBlock = null;

    const baseWaitMs = config.mlPdpJsonLdMaxWaitMs;
    const pollMs = config.mlPdpPollIntervalMs;
    const deadline = Date.now() + baseWaitMs + extraWaitMs;
    let lastHint = 0;
    while (Date.now() < deadline) {
      try {
        productNode = await page.evaluate(collectProductJsonLdInPage);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes('Execution context was destroyed') || msg.includes('Target closed')) {
          await new Promise((r) => setTimeout(r, pollMs));
          continue;
        }
        throw e;
      }
      if (productNode && typeof productNode === 'object') break;
      if (extraWaitMs > 0 && Date.now() - lastHint > 20_000) {
        console.info('[ml-pdp] ainda à espera do produto (login ou rede lenta)…');
        lastHint = Date.now();
      }
      await new Promise((r) => setTimeout(r, pollMs));
    }

    if (!productNode || typeof productNode !== 'object') {
      htmlSnapshot = await page.content();
      embeddedBlock = buildEmbeddedPartialFromHtml(htmlSnapshot);
      const ep = embeddedBlock.partial;
      const idFallback =
        s(ep.item_id) || s(ep.catalog_product_id) || extractMlProductIdFromUrl(productUrl);
      const hasName = s(ep.name).length > 2;
      const hasPrice = Number(ep.price_current) > 0;
      if (embeddedBlock.found && idFallback && (hasName || hasPrice)) {
        productNode = buildSyntheticJsonLdProductNode(ep, productUrl);
        console.info('[ml-pdp] JSON-LD ausente — nó mínimo a partir de embedded_json');
      }
    }

    if (!productNode || typeof productNode !== 'object') {
      looksBlocked =
        challengeUrlLooksBlocked(page.url()) || (await page.evaluate(browserDetectMlAccountChallenge));
      if (looksBlocked) {
        throw new Error(
          `${mlAccountVerificationErrorMessage('pdp (sem JSON-LD após espera)')}\n` +
            '  • Corre: npm run start:open -- "' +
            productUrl +
            '"\n' +
            '  • Ou: ML_PDP_FAIL_FAST_ON_ACCOUNT_CHALLENGE=false com Chrome visível para concluir login\n' +
            '  • USER_DATA_DIR / chrome-profile com sessão válida\n' +
            '  • ML_PDP_WAIT_LOGIN_MS / ML_PDP_AUTO_WAIT_LOGIN_MAX_MS'
        );
      }
      throw new Error(
        '[ml-pdp] Não encontrei schema.org/Product em application/ld+json nem estado embutido suficiente. Confirma que é um link de **produto** (não listagem) e tenta de novo.'
      );
    }

    if (!htmlSnapshot) {
      htmlSnapshot = await page.content();
    }
    if (!embeddedBlock) {
      embeddedBlock = buildEmbeddedPartialFromHtml(htmlSnapshot);
    }

    const blobsParsed = embeddedBlock.blobs.filter((b) => b.parsed != null).length;
    console.info(
      `[ml-pdp] embedded_json: found=${embeddedBlock.found} blobs_ok=${blobsParsed}/${embeddedBlock.blobs.length}`
    );
    const embKeys = Object.keys(embeddedBlock.partial).filter((k) => !k.startsWith('_'));
    if (embKeys.length) {
      const preview = embKeys.slice(0, 22).join(', ');
      console.info(`[ml-pdp] embedded_json campos (${embKeys.length}): ${preview}${embKeys.length > 22 ? '…' : ''}`);
    }

    const productIdFromUrl = extractMlProductIdFromUrl(productUrl);
    const asRecord = /** @type {Record<string, unknown>} */ (productNode);
    if (!jsonLdIsProductType(asRecord['@type'])) {
      throw new Error('[ml-pdp] nó JSON-LD não é schema.org/Product');
    }

    const { partial: ldPartial, price_currency: priceCyLd } = buildJsonLdPartial({
      productNode: asRecord,
      pageUrl: productUrl,
      productIdFromUrl,
    });
    let normalized = mergeWithFieldSources(
      base,
      /** @type {Record<string, unknown>} */ (embeddedBlock.partial),
      'embedded_json'
    );
    normalized = mergeWithFieldSources(
      /** @type {import('../productSchema.js').CanonicalProduct} */ (normalized),
      ldPartial,
      'json_ld'
    );
    const priceCurrencyLd = typeof priceCyLd === 'string' && priceCyLd ? priceCyLd : 'BRL';

    await page
      .waitForSelector('h1.ui-pdp-title, .ui-pdp-title, script[type="application/ld+json"]', {
        timeout: 20_000,
      })
      .catch(() => {});

    await page.evaluate(() => {
      window.scrollTo(0, Math.min(10_000, document.body?.scrollHeight || 0));
    });
    await new Promise((r) => setTimeout(r, Math.max(0, config.mlPdpDomScrollDelayMs)));

    /** @type {Record<string, unknown>} */
    const domRaw = await page.evaluate(extractMlPdpFromDom);
    const domPartial = normalizeMlPdpDomToPartial(domRaw, productUrl);
    normalized = mergeWithFieldSources(
      /** @type {import('../productSchema.js').CanonicalProduct} */ (normalized),
      /** @type {Record<string, unknown>} */ (domPartial),
      'dom'
    );

    const price_currency =
      typeof normalized.price_currency === 'string' && normalized.price_currency
        ? normalized.price_currency
        : priceCurrencyLd;

    if (keepBrowserOpen) {
      await page.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
    }

    console.info(
      `[ml-pdp] item_id=${normalized.item_id || '(vazio)'} catalog=${normalized.catalog_product_id || '(vazio)'}`
    );
    console.info('[ml-pdp] success');
    scrapeResult = finalizeAndValidate(
      /** @type {import('../productSchema.js').CanonicalProduct} */ ({ ...normalized, price_currency }),
      price_currency
    );
  } finally {
    if (reuseExternalPage) {
      await page.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
    } else if (!keepBrowserOpen) {
      let pauseMs = config.mlPdpPauseBeforeCloseMs;
      if (pauseMs == null && !config.headless) pauseMs = 8000;
      if (pauseMs == null) pauseMs = 0;
      if (pauseMs > 0) {
        console.info(
          `[ml-pdp] pausa ${pauseMs / 1000}s antes de fechar o browser (0 = imediato → ML_PDP_PAUSE_BEFORE_CLOSE_MS=0)`
        );
        await new Promise((r) => setTimeout(r, pauseMs));
      }
      await page.close().catch(() => {});
      if (ownBrowser) {
        await browser.close().catch(() => {});
      }
    }
  }
  return /** @type {import('../productSchema.js').CanonicalProduct & { price_currency: string }} */ (scrapeResult);
}
