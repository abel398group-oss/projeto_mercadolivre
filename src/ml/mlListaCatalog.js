import fs from 'node:fs/promises';
import path from 'node:path';
import { launchBrowser } from '../browser.js';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { emptyProduct, mergeProduct } from '../productSchema.js';
import { sleep } from '../util.js';
import {
  ML_ACCOUNT_VERIFICATION_PREFIX,
  isMlAccountChallengePage,
  mlAccountVerificationErrorMessage,
} from './mlAccountChallenge.js';
import {
  extractListingEnrichmentFromHtml,
  htmlLooksLikeSearchListing,
  isListaSuspiciousTrafficHtml,
} from './mlListaHtmlExtract.js';
import { writeCatalogLeanFile, writeCatalogLeanFileSync } from './mlCatalogLean.js';
import { getPipelineSharedBrowser, isPipelineSharedBrowser } from './mlPipelineBrowser.js';
import { notifyPipelineItemDiscovered, pipelineShutdownRequested } from './mlPipelineQueue.js';

const LISTA_HOST = 'https://lista.mercadolivre.com.br';
const CATEGORIAS_URL = 'https://www.mercadolivre.com.br/categorias';

const FETCH_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
};

/**
 * @param {string} mlbFromPath ex.: MLB123 do segmento /p/MLB123
 */
function mapListaIdToRecord(mlbFromPath, categoryPath) {
  const id = mlbFromPath.toUpperCase().startsWith('MLB')
    ? `MLB${mlbFromPath.replace(/^MLB/i, '').replace(/\D/g, '')}`
    : `MLB${String(mlbFromPath).replace(/\D/g, '')}`;
  return {
    product_id: id,
    listing_product_id: id,
    name: '',
    price_current: 0,
    price_currency: 'BRL',
    price_original: null,
    discount: 0,
    seller_id: '',
    shop_name: '',
    sales_count: null,
    sales_count_precision: 'unknown',
    rating: 0,
    rating_count: null,
    url: `https://www.mercadolivre.com.br/p/${id}`,
    url_primary: `https://www.mercadolivre.com.br/p/${id}`,
    image_main: '',
    images: [],
    category_source_id: categoryPath,
    source: 'ml_lista_html',
    collected_at: new Date().toISOString(),
  };
}

/**
 * @param {string} html
 * @returns {string[]}
 */
export function extractListaCategoryPaths(html) {
  if (!html || typeof html !== 'string') return [];
  const re = /https:\/\/lista\.mercadolivre\.com\.br\/([a-z0-9]+(?:\/[a-z0-9]+)*)/gi;
  const seen = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const p = m[1];
    if (p && p.length > 1) seen.add(p);
  }
  return [...seen].sort();
}

/**
 * @param {string} html
 * @returns {Set<string>}
 */
export function extractMlbIdsFromListaHtml(html) {
  const re = /\/p\/(MLB[0-9]+)/gi;
  const out = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    out.add(m[1]);
  }
  return out;
}

/**
 * @param {string} categoryPath
 * @param {number} offset
 */
function buildListaPageUrl(categoryPath, offset) {
  if (offset <= 0) return `${LISTA_HOST}/${categoryPath}`;
  return `${LISTA_HOST}/${categoryPath}_Desde_${offset}`;
}

/**
 * @returns {Promise<{ html: string; finalUrl: string }>}
 */
async function fetchHtml(url) {
  const r = await fetch(url, { headers: FETCH_HEADERS, redirect: 'follow' });
  if (!r.ok) throw new Error(`GET ${url} → ${r.status}`);
  const html = await r.text();
  return { html, finalUrl: r.url || url };
}

/** @type {import('puppeteer').Browser | null} */
let listaBrowser = null;
/** Uma aba reutilizada (evita abrir/fechar centenas de tabs e dá tempo ao DOM da listagem). */
/** @type {import('puppeteer').Page | null} */
let listaPage = null;
/** Depois do primeiro bloqueio com fetch, usa só Puppeteer até ao fim do run (evita GET inúteis). */
let listaPreferBrowser = false;

async function closeListaBrowserInstance() {
  if (listaPage) {
    await listaPage.close().catch(() => {});
    listaPage = null;
  }
  if (listaBrowser && !isPipelineSharedBrowser(listaBrowser)) {
    await listaBrowser.close().catch(() => {});
  }
  listaBrowser = null;
}

async function disposeListaBrowser() {
  listaPreferBrowser = false;
  await closeListaBrowserInstance();
}

/** @param {unknown} e */
function isTransientBrowserError(e) {
  const msg = e instanceof Error ? e.message : String(e);
  return /Target closed|TargetCloseError|Protocol error|Session closed|Execution context was destroyed|Browser disconnected|WebSocket is not open/i.test(
    msg
  );
}

/**
 * @param {string} url
 * @param {{ stats: Record<string, number> }} ctx
 */
async function fetchListaHtmlResolved(url, ctx) {
  const maxAttempts = 2;

  async function viaBrowser() {
    let lastErr = /** @type {unknown} */ (null);
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        if (listaBrowser && !listaBrowser.connected) {
          await closeListaBrowserInstance();
        }
        if (!listaBrowser) {
          const shared = getPipelineSharedBrowser();
          if (shared && shared.connected) {
            console.info('[ml-lista] a usar browser partilhado do pipeline (mesma sessão que o PDP; aba de listagem separada)');
            listaBrowser = shared;
          } else {
            console.info(
              `[ml-lista] a abrir Chrome (userDataDir=${path.resolve(process.cwd(), config.mlListaUserDataDir)}) — uma janela, mesma aba, vários URLs`
            );
            const { browser } = await launchBrowser({ userDataDir: config.mlListaUserDataDir });
            listaBrowser = browser;
          }
        }
        const browserRef = listaBrowser;
        if (!browserRef) throw new Error('browser indisponível');

        if (!listaPage || listaPage.isClosed()) {
          listaPage = await browserRef.newPage();
        }

        await listaPage.goto(url, { waitUntil: 'domcontentloaded', timeout: 90_000 });

        await Promise.race([
          listaPage.waitForSelector('.ui-search-result, .ui-search-layout, .poly-card, [class*="poly-card"]', {
            timeout: 14_000,
          }),
          listaPage.waitForSelector('a[href*="/p/MLB"]', { timeout: 14_000 }),
        ]).catch(() => {});

        const settle = Math.max(0, config.mlListaBrowserSettleMs);
        if (settle > 0) await sleep(settle);

        ctx.stats.lista_browser_fallbacks += 1;
        const finalUrl = listaPage.url();
        const html = await listaPage.content();
        if (isMlAccountChallengePage({ url: finalUrl, html })) {
          if (config.mlListaCloseBrowserOnAccountChallenge) await closeListaBrowserInstance();
          throw new Error(mlAccountVerificationErrorMessage('listagem'));
        }
        return { html, finalUrl };
      } catch (e) {
        lastErr = e;
        await closeListaBrowserInstance();
        const msg = e instanceof Error ? e.message : String(e);
        if (attempt < maxAttempts && isTransientBrowserError(e)) {
          console.warn(`[ml-lista] browser caiu (tentativa ${attempt}/${maxAttempts}): ${msg}`);
          await sleep(2000);
          continue;
        }
        throw e;
      }
    }
    throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
  }

  if (listaPreferBrowser) {
    return await viaBrowser();
  }

  const { html, finalUrl } = await fetchHtml(url);
  if (isMlAccountChallengePage({ url: finalUrl, html })) {
    if (config.mlListaFetchChallengeTryBrowser) {
      console.warn(
        '[ml-lista] bloqueio/verificação no GET (sem cookies típicos de browser) — a repetir com Puppeteer e perfil. ' +
          'No pipeline isto usa o mesmo Chrome que o PDP; faz login nessa janela se pedir.'
      );
      listaPreferBrowser = true;
      return await viaBrowser();
    }
    if (config.mlListaCloseBrowserOnAccountChallenge) await closeListaBrowserInstance();
    throw new Error(mlAccountVerificationErrorMessage('listagem'));
  }
  if (!isListaSuspiciousTrafficHtml(html)) {
    return { html, finalUrl };
  }

  ctx.stats.lista_blocked_pages += 1;
  if (!config.mlListaBrowserOnBlock) {
    return { html, finalUrl };
  }

  console.warn(
    '[ml-lista] página de bloqueio no fetch — a usar browser no restante desta execução (ML_LISTA_BROWSER_ON_BLOCK). ' +
      `Perfil (absoluto): ${path.resolve(process.cwd(), config.mlListaUserDataDir)} — por defeito igual a USER_DATA_DIR; evita dois Puppeteers no mesmo perfil em paralelo.`
  );
  listaPreferBrowser = true;
  return await viaBrowser();
}

/**
 * @param {string} categoryPath
 * @param {Map<string, Record<string, unknown>>} items
 * @param {{ stats: Record<string, number> }} stats
 * @param {(() => Promise<void>) | undefined} [onIncrementalFlush] gravar JSON no disco após cada página (lista)
 */
async function crawlListaCategory(categoryPath, items, stats, onIncrementalFlush) {
  const step = Math.max(1, config.mlListaPageStep);
  let offset = 0;
  let added = 0;
  let emptyStreak = 0;
  let page = 0;
  const ctx = { stats };

  for (;;) {
    if (config.mlListaDelayMs > 0) await sleep(config.mlListaDelayMs);
    const url = buildListaPageUrl(categoryPath, offset);
    let html = '';
    try {
      const got = await fetchListaHtmlResolved(url, ctx);
      html = got.html;
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes(ML_ACCOUNT_VERIFICATION_PREFIX)) {
        stats.lista_account_verification = (stats.lista_account_verification || 0) + 1;
        console.error(`[ml-lista] ${msg} | categoria=${categoryPath} | ${url}`);
        emptyStreak += 1;
        if (emptyStreak >= 2) break;
        continue;
      }
      throw e;
    }
    stats.lista_fetches += 1;
    const ids = extractMlbIdsFromListaHtml(html);
    if (
      ids.size === 0 &&
      listaPreferBrowser &&
      !isListaSuspiciousTrafficHtml(html) &&
      !htmlLooksLikeSearchListing(html) &&
      page === 0
    ) {
      console.warn(
        `[ml-lista] aviso (${categoryPath}): HTML via browser sem listagem nem /p/MLB — possível página ainda a carregar ou login/captcha. ` +
          'Aumenta ML_LISTA_BROWSER_SETTLE_MS (ex.: 2000) ou faz login no perfil lista.'
      );
    }
    if (ids.size === 0) {
      emptyStreak += 1;
      if (emptyStreak >= 2) break;
    } else {
      emptyStreak = 0;
      for (const rawId of ids) {
        const rec = mapListaIdToRecord(rawId, categoryPath);
        if (!items.has(rec.product_id)) {
          items.set(
            rec.product_id,
            mergeProduct(emptyProduct(), /** @type {*} */ (rec), 'listing_html')
          );
          added += 1;
          void notifyPipelineItemDiscovered(/** @type {*} */ (items.get(rec.product_id)));
        }
      }
    }

    const enrichment = extractListingEnrichmentFromHtml(html, categoryPath);
    for (const [pid, patch] of enrichment) {
      const prev = items.get(pid);
      if (prev) {
        items.set(pid, mergeProduct(/** @type {*} */ (prev), /** @type {*} */ (patch), 'listing_network'));
      } else if (/^MLB\d+$/.test(pid)) {
        const base = mapListaIdToRecord(pid, categoryPath);
        const seeded = mergeProduct(emptyProduct(), /** @type {*} */ (base), 'listing_html');
        items.set(pid, mergeProduct(seeded, /** @type {*} */ (patch), 'listing_network'));
        added += 1;
        void notifyPipelineItemDiscovered(/** @type {*} */ (items.get(pid)));
      }
    }

    page += 1;
    if (
      onIncrementalFlush &&
      config.mlListaFlushJsonEveryPages > 0 &&
      page % config.mlListaFlushJsonEveryPages === 0
    ) {
      await onIncrementalFlush();
    }
    if (config.mlListaMaxPagesPerCategory != null && page >= config.mlListaMaxPagesPerCategory) break;
    offset += step;
  }

  return { added };
}

export async function runCatalogViaLista() {
  const items = new Map();
  const meta = {
    site_id: 'MLB',
    mode: 'lista_html',
    started_at: new Date().toISOString(),
    note:
      'Coletado via HTML de lista.mercadolivre.com.br: IDs em /p/MLB; enriquecimento com JSON-LD (ItemList/Product), breadcrumbs, preço (andes-money-amount), título (poly/ui-search), imagem mlstatic, avaliações, vendidos, parcelas, frete grátis, badges. Campos vazios: completar com PDP se precisares. ML_LISTA_BROWSER_ON_BLOCK=true se o fetch for bloqueado.',
    stats: {
      categories_total: 0,
      categories_done: 0,
      items_unique: 0,
      lista_fetches: 0,
      lista_blocked_pages: 0,
      lista_account_verification: 0,
      lista_browser_fallbacks: 0,
      errors: 0,
    },
  };
  const stats = meta.stats;
  const outputPath = path.resolve(config.mlCatalogOutput);

  function buildCatalogPayload() {
    stats.items_unique = items.size;
    return { meta: { ...meta }, items: Object.fromEntries(items) };
  }

  function serializeCatalogPayload(payload) {
    return config.mlCatalogPretty ? JSON.stringify(payload, null, 2) : JSON.stringify(payload);
  }

  async function flush() {
    const payload = buildCatalogPayload();
    const json = serializeCatalogPayload(payload);
    await writeSnapshot({
      latestPath: outputPath,
      historySubdir: 'catalog',
      historyBaseName: 'catalogo_ml',
      content: json,
    });
    console.info(`[ml-lista] gravado ${items.size} itens → ${outputPath}`);
    await writeCatalogLeanFile(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-catalog-lean]', e instanceof Error ? e.message : e)
    );
  }

  /** Gravação síncrona no Ctrl+C (Windows/Git Bash por vezes corta o handler `async` antes do `writeFile`). */
  function flushSync() {
    const payload = buildCatalogPayload();
    const json = serializeCatalogPayload(payload);
    writeSnapshotSync({
      latestPath: outputPath,
      historySubdir: 'catalog',
      historyBaseName: 'catalogo_ml',
      content: json,
    });
    console.info(`[ml-lista] gravado ${items.size} itens → ${outputPath}`);
    try {
      writeCatalogLeanFileSync(/** @type {*} */ (payload));
    } catch (e) {
      console.error('[ml-catalog-lean]', e instanceof Error ? e.message : e);
    }
  }

  let stopping = false;
  if (process.env.ML_PIPELINE_ACTIVE !== '1') {
    process.on('SIGINT', () => {
      if (stopping) return;
      stopping = true;
      console.info('\n[ml-lista] interrompido — a gravar…');
      try {
        flushSync();
      } catch (e) {
        console.error('[ml-lista] falha ao gravar o JSON:', e instanceof Error ? e.message : e);
      }
      void disposeListaBrowser()
        .catch(() => {})
        .finally(() => process.exit(0));
    });
  }

  console.info('[ml-lista] a descobrir categorias em mercadolivre.com.br/categorias…');
  const catGot = await fetchListaHtmlResolved(CATEGORIAS_URL, { stats });
  stats.lista_fetches += 1;
  let paths = extractListaCategoryPaths(catGot.html);
  if (config.mlCatalogMaxCategories != null) {
    paths = paths.slice(0, config.mlCatalogMaxCategories);
  }
  stats.categories_total = paths.length;
  console.info(`[ml-lista] ${paths.length} URLs de listagem (Ctrl+C grava e sai)`);

  await flush();

  for (let i = 0; i < paths.length; i++) {
    if (pipelineShutdownRequested()) break;
    const p = paths[i];
    try {
      const { added } = await crawlListaCategory(p, items, stats, flush);
      stats.categories_done += 1;
      console.info(
        `[ml-lista] [${i + 1}/${paths.length}] ${p} +${added} novos | únicos ${items.size} | GET ${stats.lista_fetches}`
      );
      if (config.mlCatalogFlushEvery > 0 && (i + 1) % config.mlCatalogFlushEvery === 0) {
        await flush();
      }
      if (config.mlCatalogMaxItems != null && items.size >= config.mlCatalogMaxItems) {
        console.info(`[ml-lista] limite ML_CATALOG_MAX_ITEMS=${config.mlCatalogMaxItems}`);
        break;
      }
    } catch (e) {
      stats.errors += 1;
      console.warn(`[ml-lista] erro em ${p}:`, e instanceof Error ? e.message : e);
    }
  }

  await flush();
  if (items.size === 0 && stats.lista_blocked_pages >= 3) {
    console.warn(
      '[ml-lista] Nenhum item no JSON mas houve páginas de bloqueio/tráfego suspeito no fetch. ' +
        'Define no .env: ML_LISTA_BROWSER_ON_BLOCK=true (este projeto já documenta em .env.example). ' +
        'Usa o mesmo USER_DATA_DIR onde o ML já te reconhece (login) se precisares. Depois: npm run catalog'
    );
  }
  await disposeListaBrowser();
}
