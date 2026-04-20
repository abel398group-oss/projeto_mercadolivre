import fs from 'node:fs/promises';
import path from 'node:path';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { emptyProduct, mergeProduct } from '../productSchema.js';
import { MlApiClient } from './mlApiClient.js';
import { mapSearchItemToRecord } from './mlCatalogMap.js';
import { runCatalogViaLista } from './mlListaCatalog.js';
import { writeCatalogLeanFile, writeCatalogLeanFileSync } from './mlCatalogLean.js';
import { notifyPipelineItemDiscovered, pipelineShutdownRequested } from './mlPipelineQueue.js';

const API_BASE = 'https://api.mercadolibre.com';

const PROBE_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  Accept: 'application/json',
};

async function apiCategoriesReachable() {
  try {
    const r = await fetch(`${API_BASE}/sites/MLB/categories`, { headers: PROBE_HEADERS });
    return r.ok;
  } catch {
    return false;
  }
}

/**
 * Orquestração: `auto` usa API se responder; senão HTML lista.mercadolivre.com.br.
 */
export async function runFullCatalog() {
  const mode = (config.mlCatalogSource || 'auto').toLowerCase();
  if (mode === 'lista') return runCatalogViaLista();
  if (mode === 'api') return runCatalogViaApi();
  const ok = await apiCategoriesReachable();
  if (!ok) {
    console.warn('[ml-catalog] API inacessível (403/rede) — modo lista.mercadolivre.com.br');
    return runCatalogViaLista();
  }
  return runCatalogViaApi();
}

/** Limite documentado da busca por categoria (offset máximo efetivo ~1000). */
const SEARCH_RESULTS_HARD_CAP = 1000;

/**
 * @param {MlApiClient} client
 * @param {string} siteId
 * @param {number | null} maxLeafCategories null = todas
 */
async function collectLeafCategoryIds(client, siteId, maxLeafCategories) {
  const roots = /** @type {{ id: string }[]} */ (await client.getJson(`${API_BASE}/sites/${siteId}/categories`));
  /** @type {string[]} */
  const leaves = [];

  async function walk(id) {
    if (maxLeafCategories != null && leaves.length >= maxLeafCategories) return;
    const data = /** @type {{ children_categories?: { id: string }[] }} */ (
      await client.getJson(`${API_BASE}/categories/${id}`)
    );
    const ch = data.children_categories || [];
    if (ch.length === 0) {
      leaves.push(id);
      return;
    }
    for (const c of ch) {
      await walk(c.id);
      if (maxLeafCategories != null && leaves.length >= maxLeafCategories) return;
    }
  }

  for (const r of roots) {
    await walk(r.id);
    if (maxLeafCategories != null && leaves.length >= maxLeafCategories) break;
  }
  return leaves;
}

/**
 * @param {MlApiClient} client
 * @param {string} categoryId
 * @param {Map<string, Record<string, unknown>>} items
 * @param {typeof config} cfg
 * @param {{ search_calls: number }} stats
 * @param {(() => Promise<void>) | undefined} onIncrementalFlush
 */
async function searchCategoryIntoMap(client, categoryId, items, cfg, stats, onIncrementalFlush) {
  const limit = Math.min(50, Math.max(1, cfg.mlSearchPageSize));
  let offset = 0;
  let added = 0;
  let truncated = false;
  /** @type {number | null} */
  let reportedTotal = null;

  for (;;) {
    const url = `${API_BASE}/sites/${cfg.mlSiteId}/search?category=${encodeURIComponent(
      categoryId
    )}&limit=${limit}&offset=${offset}`;
    /** @type {{ results?: unknown[]; paging?: { total?: number } }} */
    const data = await client.getJson(url);
    stats.search_calls += 1;

    const paging = data.paging || {};
    if (reportedTotal == null && typeof paging.total === 'number') {
      reportedTotal = paging.total;
      if (reportedTotal > SEARCH_RESULTS_HARD_CAP) truncated = true;
    }

    const results = data.results || [];
    if (results.length === 0) break;

    for (const raw of results) {
      const item = /** @type {Record<string, unknown>} */ (raw);
      const rec = mapSearchItemToRecord(item, categoryId);
      if (!rec.product_id) continue;
      if (!items.has(rec.product_id)) {
        items.set(
          rec.product_id,
          mergeProduct(
            emptyProduct(),
            /** @type {Partial<import('../productSchema.js').CanonicalProduct>} */ (rec),
            'ml_search_api'
          )
        );
        added += 1;
        void notifyPipelineItemDiscovered(/** @type {*} */ (items.get(rec.product_id)));
      }
    }

    if (cfg.mlCatalogFlushApiEveryPage && onIncrementalFlush) {
      await onIncrementalFlush();
    }

    offset += limit;
    if (results.length < limit) break;

    const cap =
      reportedTotal != null
        ? Math.min(reportedTotal, SEARCH_RESULTS_HARD_CAP)
        : SEARCH_RESULTS_HARD_CAP;
    if (offset >= cap) break;
  }

  return { added, truncated };
}

export async function runCatalogViaApi() {
  const client = new MlApiClient({
    forceBrowser: config.mlUseBrowserForApi,
    delayMs: config.mlApiDelayMs,
  });

  const items = new Map();
  const meta = {
    site_id: config.mlSiteId,
    started_at: new Date().toISOString(),
    note:
      'Itens por categoria limitados a 1000 pela API pública. Categorias com mais anúncios ficam truncadas (ver stats.truncated_categories).',
    stats: {
      categories_total: 0,
      categories_done: 0,
      items_unique: 0,
      search_calls: 0,
      truncated_categories: 0,
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
    console.info(`[ml-catalog] gravado ${items.size} itens → ${outputPath}`);
    await writeCatalogLeanFile(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-catalog-lean]', e instanceof Error ? e.message : e)
    );
  }

  function flushSync() {
    const payload = buildCatalogPayload();
    const json = serializeCatalogPayload(payload);
    writeSnapshotSync({
      latestPath: outputPath,
      historySubdir: 'catalog',
      historyBaseName: 'catalogo_ml',
      content: json,
    });
    console.info(`[ml-catalog] gravado ${items.size} itens → ${outputPath}`);
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
      console.info('\n[ml-catalog] interrompido — a gravar…');
      try {
        flushSync();
      } catch (e) {
        console.error('[ml-catalog] falha ao gravar o JSON:', e instanceof Error ? e.message : e);
      }
      void client
        .dispose()
        .catch(() => {})
        .finally(() => process.exit(0));
    });
  }

  try {
    const leafIds = await collectLeafCategoryIds(
      client,
      config.mlSiteId,
      config.mlCatalogMaxCategories
    );
    stats.categories_total = leafIds.length;
    console.info(`[ml-catalog] ${leafIds.length} categorias-folha (sem limite de tempo; Ctrl+C grava e sai)`);

    await flush();

    for (let i = 0; i < leafIds.length; i++) {
      if (pipelineShutdownRequested()) break;
      const catId = leafIds[i];
      try {
        const { added, truncated } = await searchCategoryIntoMap(client, catId, items, config, stats, flush);
        stats.categories_done += 1;
        if (truncated) stats.truncated_categories += 1;
        console.info(
          `[ml-catalog] [${i + 1}/${leafIds.length}] ${catId} +${added} novos | únicos ${items.size} | buscas ${stats.search_calls}`
        );

        if (config.mlCatalogFlushEvery > 0 && (i + 1) % config.mlCatalogFlushEvery === 0) {
          await flush();
        }
        if (config.mlCatalogMaxItems != null && items.size >= config.mlCatalogMaxItems) {
          console.info(`[ml-catalog] limite ML_CATALOG_MAX_ITEMS=${config.mlCatalogMaxItems}`);
          break;
        }
      } catch (e) {
        stats.errors += 1;
        console.warn(`[ml-catalog] erro em ${catId}:`, e instanceof Error ? e.message : e);
      }
    }

    await flush();
  } finally {
    await client.dispose();
  }
}
