/**
 * Bulk PDP: um único browser + uma página reutilizados para todo o lote (`page.goto` por produto).
 *
 * Saída por defeito (pasta `output/` enxuta):
 * - **Agregado**: `ML_BULK_OUTPUT` → `./output/pdp_all.json`
 * - **Core (mínimo)**: `ML_PDP_CORE_OUTPUT` → `./output/pdp_all_core.json` (flush)
 * - **Lean (rico)**: `ML_PDP_LEAN_OUTPUT` → `./output/pdp_all_lean.json` (flush)
 * - **Debug lean**: `ML_PDP_DEBUG_LEAN_OUTPUT` → `./output/pdp_debug_lean.json` (só itens com conflitos/rejeições)
 * - **Métricas**: `ML_METRICS_OUTPUT` → `./output/metrics.json` (atualizado em cada flush e no fim; parcial em SIGINT)
 * - **Histórico opcional**: `SAVE_HISTORY_OUTPUTS=true` grava cópias com timestamp em `./output/history/{catalog,pdp,debug,metrics}/`
 *
 * Ficheiros JSONL auxiliares (discovered / enriched / invalid / duplicate) só com `ML_DEBUG_OUTPUTS=true`
 * ou `ML_BULK_JSONL=true`. Debug de sessão: `ML_BULK_SESSION_LOG`.
 *
 * Comportamento do browser (também registado em log no arranque):
 * - **Reutilização**: mesma instância do Chrome e mesma aba do primeiro ao último produto (salvo reciclagem).
 * - **Reciclagem periódica**: se `ML_BULK_BROWSER_RECYCLE_EVERY=N`, o browser fecha e volta a abrir a cada N produtos
 *   (antes de processar o item N+1, 2N+1, …) com o mesmo `USER_DATA_DIR`.
 * - **Limites**: `ML_BULK_MAX_ITEMS` reduz a fila no início; `ML_BULK_MAX_DURATION_MS` encerra após esse tempo
 *   (flush + métricas + fecho do browser), sem perder dados já processados.
 * - **Recuperação**: falha num produto não interrompe o lote; se a página ou a sessão parecerem inválidas,
 *   recria-se só a página ou o browser inteiro antes de novas tentativas (até 3 tentativas por URL).
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { launchBrowser, logChromePersistentProfileSummary } from './browser.js';
import { config } from './config.js';
import { buildBulkMetricsSnapshot } from './io/bulkMetrics.js';
import { writeRunManifest } from './io/runManifest.js';
import { writeSnapshot } from './io/writeSnapshot.js';
import { appendJsonlLine } from './io/jsonl.js';
import { dedupePrimaryKey } from './ml/mlDedupe.js';
import { resolveCatalogItemToPdpUrl } from './ml/mlExtract.js';
import { writePdpDebugLeanFromPayload } from './ml/mlPdpDebugLean.js';
import { warmPdpIdleTab } from './ml/mlPdpIdleTabWarmup.js';
import { writePdpLeanFromPayload } from './ml/mlPdpLean.js';
import { scrapeMlPdp } from './ml/mlPdpScrape.js';
import { sleep } from './util.js';

const catalogPath = path.resolve(String(config.mlBulkInput || '').trim());
const outFile = String(config.mlBulkOutput || '').trim();
const outPath = outFile ? path.resolve(outFile) : '';
const metricsOutFile = String(config.mlMetricsOutput || '').trim();
const metricsPath = metricsOutFile ? path.resolve(metricsOutFile) : '';

/**
 * @param {unknown} raw
 * @returns {{ meta: Record<string, unknown>; items: Record<string, Record<string, unknown>> }}
 */
function parseCatalogPayload(raw) {
  if (!raw || typeof raw !== 'object') {
    throw new Error('[ml-pdp-bulk] JSON inválido: esperava um objeto com `items`.');
  }
  const o = /** @type {Record<string, unknown>} */ (raw);
  const items = o.items;
  if (!items || typeof items !== 'object' || Array.isArray(items)) {
    throw new Error('[ml-pdp-bulk] JSON inválido: `items` tem de ser um objeto (product_id → registo).');
  }
  const meta = o.meta && typeof o.meta === 'object' && !Array.isArray(o.meta) ? /** @type {Record<string, unknown>} */ (o.meta) : {};
  return { meta, items: /** @type {Record<string, Record<string, unknown>>} */ (items) };
}

/** @param {Record<string, unknown>} entry */
async function appendSessionLog(entry) {
  const p = String(config.mlBulkSessionLog || '').trim();
  if (!p) return;
  const abs = path.resolve(p);
  try {
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.appendFile(abs, `${JSON.stringify({ t: new Date().toISOString(), ...entry })}\n`, 'utf8');
  } catch {
    /* não bloquear o lote */
  }
}

/** @param {string} msg */
function needsFullBrowserRecycle(msg) {
  return /Target closed|Session closed|Connection closed|Browser has been disconnected|WebSocket is not open/i.test(msg);
}

/** @param {string} msg */
function isLikelyBrokenPage(msg) {
  return (
    needsFullBrowserRecycle(msg) ||
    /Execution context was destroyed|Protocol error|Navigation failed|Frame was detached|net::ERR/i.test(msg)
  );
}

async function writeOutput(payload) {
  if (!outPath) return;
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  const json = config.mlBulkPretty ? JSON.stringify(payload, null, 2) : JSON.stringify(payload);
  await fs.writeFile(outPath, json, 'utf8');
}

async function main() {
  let rawText;
  try {
    rawText = await fs.readFile(catalogPath, 'utf8');
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`[ml-pdp-bulk] Não consegui ler o catálogo: ${catalogPath}`);
    console.error(msg);
    console.error('Gera antes: npm run catalog   ou define ML_BULK_INPUT=/caminho/catalogo_ml.json');
    process.exit(1);
    return;
  }

  let catalog;
  try {
    catalog = parseCatalogPayload(JSON.parse(rawText));
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error('[ml-pdp-bulk]', msg);
    process.exit(1);
    return;
  }

  /** @type {{ product_id: string; url: string }[]} */
  const queue = [];
  for (const [key, rec] of Object.entries(catalog.items)) {
    const url = resolveCatalogItemToPdpUrl(rec);
    const pid = String(rec?.product_id || key || '').trim() || key;
    if (!url) {
      console.warn(`[ml-pdp-bulk] sem URL para ${pid} — ignorado`);
      continue;
    }
    queue.push({ product_id: pid, url });
  }

  queue.sort((a, b) => a.product_id.localeCompare(b.product_id));

  const catalogItemCount = Object.keys(catalog.items).length;
  if (catalogItemCount > queue.length) {
    console.warn(
      `[ml-pdp-bulk] catálogo tem ${catalogItemCount} chaves mas só ${queue.length} com URL resolvível — os restantes ficam de fora.`
    );
  }

  const max = config.mlBulkMaxItems;
  const toRun = max != null ? queue.slice(0, max) : queue;
  if (max != null) {
    console.warn(`[ml-pdp-bulk] ML_BULK_MAX_ITEMS=${max} — a processar só os primeiros ${toRun.length}. Para todos, apaga ML_BULK_MAX_ITEMS do .env.`);
  }

  /** @type {Record<string, unknown>} */
  const itemsOut = {};
  /** @type {{ product_id: string; url: string; message: string }[]} */
  const errors = [];

  const useAuxiliaryJsonl = Boolean(config.mlDebugOutputs || config.mlBulkJsonl);
  /** @type {Map<string, string>} dedupe_key → primeira chave do catálogo */
  const dedupeFirstCatalogKey = new Map();

  const recycleEvery = config.mlBulkBrowserRecycleEvery;
  const maxDurationMs = config.mlBulkMaxDurationMs;

  const runStartedAtIso = new Date().toISOString();
  const runStartedMs = Date.now();

  /** @type {'complete' | 'max_duration' | 'sigint'} */
  let runStopReason = 'complete';

  function timeBudgetExceeded() {
    return maxDurationMs != null && Date.now() - runStartedMs >= maxDurationMs;
  }

  let processedCount = 0;
  let successScrapeCount = 0;
  let duplicateCount = 0;
  let browserRestartCount = 0;
  let pageReloadCount = 0;

  const payload = {
    meta: {
      ...catalog.meta,
      bulk_started_at: runStartedAtIso,
      bulk_catalog_path: catalogPath,
      bulk_total_queued: toRun.length,
      bulk_delay_ms: config.mlBulkDelayMs,
      bulk_max_items: max,
      bulk_queue_capped_by_max_items: max != null,
      bulk_max_duration_ms: maxDurationMs,
      bulk_jsonl: useAuxiliaryJsonl,
      bulk_browser_recycle_every: recycleEvery,
      bulk_session_mode: 'single_browser_single_page',
    },
    items: itemsOut,
    errors,
  };

  let flushCounter = 0;
  const flushEvery = Math.max(0, config.mlBulkFlushEvery);

  async function writeMetrics(reason, interruptedRun) {
    if (!metricsPath) return;
    const finishedIso = new Date().toISOString();
    const duration_seconds = Math.max(0, Math.round((Date.now() - runStartedMs) / 1000));
    const snap = buildBulkMetricsSnapshot({
      run_started_at: runStartedAtIso,
      run_finished_at: finishedIso,
      duration_seconds,
      total_input_items: toRun.length,
      processed_items: processedCount,
      success_items: successScrapeCount,
      failed_items: errors.length,
      unique_items_stored: Object.keys(itemsOut).length,
      duplicate_items: duplicateCount,
      browser_restarts: browserRestartCount,
      page_reloads: pageReloadCount,
      interrupted: interruptedRun,
      flush_reason: reason,
      run_stopped_reason: runStopReason,
      debug_outputs: useAuxiliaryJsonl,
      items: itemsOut,
      errors,
      source_meta: /** @type {Record<string, unknown>} */ (payload.meta),
    });
    const indent = config.mlBulkPretty ? 2 : 0;
    await writeSnapshot({
      latestPath: metricsPath,
      historySubdir: 'metrics',
      historyBaseName: 'metrics',
      content: JSON.stringify(snap, null, indent),
    });
    console.info(`[ml-pdp-bulk] métricas (${reason}): ${metricsPath}`);
  }

  async function flush(reason, interruptedRun = false) {
    payload.meta.bulk_stop_reason = runStopReason;
    if (outPath) {
      await writeOutput(payload);
      console.info(`[ml-pdp-bulk] gravado (${reason}): ${outPath}`);
    }
    await writePdpLeanFromPayload(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-pdp-lean]', e instanceof Error ? e.message : e)
    );
    await writePdpCoreFromPayload(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-pdp-core]', e instanceof Error ? e.message : e)
    );
    await writePdpDebugLeanFromPayload(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-pdp-debug-lean]', e instanceof Error ? e.message : e)
    );
    await writeMetrics(reason, interruptedRun);
  }

  let interrupted = false;
  /** @type {import('puppeteer').Browser | null} */
  let browserForSignal = null;
  const onSigInt = async () => {
    if (interrupted) return;
    interrupted = true;
    runStopReason = 'sigint';
    console.info('\n[ml-pdp-bulk] interrompido — a gravar…');
    await browserForSignal?.close().catch(() => {});
    await flush('SIGINT', true, true).catch((e) => console.error(e));
    process.exit(130);
  };
  process.on('SIGINT', () => {
    void onSigInt();
  });

  if (toRun.length === 0) {
    console.warn('[ml-pdp-bulk] Nenhum item com URL resolvível. Verifica o catálogo.');
    await flush('vazio', false, true);
    process.exit(0);
    return;
  }

  logChromePersistentProfileSummary('[ml-pdp-bulk]');
  console.info(`[ml-pdp-bulk] ${toRun.length} produto(s) — entrada: ${catalogPath}`);
  if (outPath) console.info(`[ml-pdp-bulk] saída (agregado): ${outPath}`);
  if (metricsPath) console.info(`[ml-pdp-bulk] métricas: ${metricsPath}`);
  if (useAuxiliaryJsonl) {
    console.info(
      `[ml-pdp-bulk] JSONL (debug): discovered=${config.mlDiscoveredJsonl} enriched=${config.mlEnrichedJsonl} invalid=${config.mlInvalidJsonl} duplicate=${config.mlDuplicateJsonl}`
    );
  }
  if (config.mlBulkSessionLog) {
    console.info(`[ml-pdp-bulk] log de sessão (debug): ${path.resolve(config.mlBulkSessionLog)}`);
  }
  if (maxDurationMs != null) {
    console.warn(
      `[ml-pdp-bulk] ML_BULK_MAX_DURATION_MS=${maxDurationMs} (~${Math.round(maxDurationMs / 1000)}s) — encerra com flush quando o tempo esgotar.`
    );
  }

  console.info(
    '[ml-pdp-bulk] Browser: **reutilizar** 1 instância + 1 página em todo o lote (cada produto = `page.goto` na mesma aba). Fecho global só no fim ou SIGINT.'
  );
  if (recycleEvery != null) {
    console.info(
      `[ml-pdp-bulk] Browser: **reciclar** a cada ${recycleEvery} produto(s) (ML_BULK_BROWSER_RECYCLE_EVERY) — novo launch, mesmo USER_DATA_DIR.`
    );
  } else {
    console.info('[ml-pdp-bulk] Browser: sem reciclagem periódica (ML_BULK_BROWSER_RECYCLE_EVERY não definido).');
  }
  console.info(
    '[ml-pdp-bulk] Em erro de página/protocolo: **recriar só a aba**; se a sessão cair, **reciclar o browser** (ver logs `[ml-pdp-bulk]` / ML_BULK_SESSION_LOG).'
  );

  let { browser } = await launchBrowser();
  browserForSignal = browser;
  /** @type {import('puppeteer').Page} */
  let page = await browser.newPage();
  await warmPdpIdleTab(page, '[ml-pdp-bulk]');
  /** Produtos já scrapeados nesta “vida” do browser (warm-up só no primeiro de cada sessão). */
  let itemsInSession = 0;

  async function recycleBrowser(reason, detail = {}) {
    browserRestartCount += 1;
    console.info(`[ml-pdp-bulk] reciclagem do browser (${reason})`);
    await appendSessionLog({ event: 'browser_recycle', reason, ...detail });
    await page.close().catch(() => {});
    await browser.close().catch(() => {});
    const launched = await launchBrowser();
    browser = launched.browser;
    browserForSignal = browser;
    page = await browser.newPage();
    await warmPdpIdleTab(page, '[ml-pdp-bulk]');
    itemsInSession = 0;
  }

  async function recreatePageOnly(reason, detail = {}) {
    pageReloadCount += 1;
    console.warn(`[ml-pdp-bulk] nova página (${reason})`);
    await appendSessionLog({ event: 'page_recreate', reason, ...detail });
    await page.close().catch(() => {});
    page = await browser.newPage();
    await warmPdpIdleTab(page, '[ml-pdp-bulk]');
    itemsInSession = 0;
  }

  try {
    if (useAuxiliaryJsonl) {
      for (const row of toRun) {
        await appendJsonlLine(config.mlDiscoveredJsonl, {
          catalog_key: row.product_id,
          url: row.url,
          discovered_at: new Date().toISOString(),
        });
      }
    }

    for (let i = 0; i < toRun.length; i++) {
      if (interrupted) break;

      if (timeBudgetExceeded()) {
        runStopReason = 'max_duration';
        console.warn(
          `[ml-pdp-bulk] limite de tempo atingido (${maxDurationMs} ms) — a gravar após ${processedCount} item(ns) processado(s).`
        );
        break;
      }

      if (recycleEvery != null && i > 0 && i % recycleEvery === 0) {
        await recycleBrowser('periódico', {
          ml_bulk_browser_recycle_every: recycleEvery,
          item_index_1based: i + 1,
        });
      }

      const { product_id: pid, url } = toRun[i];
      console.info(`[ml-pdp-bulk] (${i + 1}/${toRun.length}) ${pid}`);

      /** @type {import('./productSchema.js').CanonicalProduct & { price_currency?: string } | null} */
      let pdp = null;
      let lastMessage = '';
      const maxAttempts = 3;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          pdp = await scrapeMlPdp(url, {
            browser,
            page,
            keepBrowserOpen: true,
            skipWarmup: itemsInSession > 0,
            listing_product_id: pid,
          });
          lastMessage = '';
          break;
        } catch (e) {
          lastMessage = e instanceof Error ? e.message : String(e);
          console.error(`[ml-pdp-bulk] tentativa ${attempt}/${maxAttempts} falhou ${pid}:`, lastMessage);

          if (/account_verification/i.test(lastMessage)) {
            console.warn('[ml-pdp-bulk] account_verification — a reciclar browser (sem novas tentativas para este item).');
            try {
              await recycleBrowser('account_verification', {
                message_excerpt: lastMessage.slice(0, 240),
                catalog_key: pid,
                attempt,
              });
            } catch (re) {
              console.error('[ml-pdp-bulk] falha ao reciclar após account_verification:', re instanceof Error ? re.message : re);
            }
            pdp = null;
            break;
          }

          if (attempt === maxAttempts) {
            pdp = null;
            break;
          }

          if (needsFullBrowserRecycle(lastMessage)) {
            try {
              await recycleBrowser('recuperação (sessão inválida)', {
                message_excerpt: lastMessage.slice(0, 240),
                catalog_key: pid,
                attempt,
              });
            } catch (re) {
              console.error('[ml-pdp-bulk] falha ao relançar browser:', re instanceof Error ? re.message : re);
              pdp = null;
              break;
            }
            continue;
          }

          if (isLikelyBrokenPage(lastMessage)) {
            try {
              await recreatePageOnly('recuperação (página/contexto)', {
                message_excerpt: lastMessage.slice(0, 240),
                catalog_key: pid,
                attempt,
              });
            } catch (re) {
              console.error('[ml-pdp-bulk] falha ao recriar página:', re instanceof Error ? re.message : re);
              pdp = null;
              break;
            }
            continue;
          }

          pdp = null;
          break;
        }
      }

      if (pdp) {
        itemsInSession += 1;
        successScrapeCount += 1;
        const dedupeKey = dedupePrimaryKey(/** @type {Record<string, unknown>} */ (pdp));
        if (dedupeFirstCatalogKey.has(dedupeKey)) {
          duplicateCount += 1;
          const first = dedupeFirstCatalogKey.get(dedupeKey);
          console.warn(`[ml-pdp-bulk] duplicado ${dedupeKey} — mantido ${first}, ignorado ${pid}`);
          if (useAuxiliaryJsonl) {
            await appendJsonlLine(config.mlDuplicateJsonl, {
              dedupe_key: dedupeKey,
              catalog_key: pid,
              first_catalog_key: first,
              url,
              record: pdp,
              at: new Date().toISOString(),
            });
          }
        } else {
          dedupeFirstCatalogKey.set(dedupeKey, pid);
          itemsOut[pid] = pdp;
          if (useAuxiliaryJsonl) {
            await appendJsonlLine(config.mlEnrichedJsonl, pdp);
            const v = pdp.validation && typeof pdp.validation === 'object' ? pdp.validation : {};
            const bad =
              /** @type {Record<string, boolean>} */ (v).valid_identity === false ||
              /** @type {Record<string, boolean>} */ (v).valid_pricing === false;
            if (bad) {
              await appendJsonlLine(config.mlInvalidJsonl, pdp);
            }
          }
        }
      } else if (lastMessage) {
        errors.push({ product_id: pid, url, message: lastMessage });
      }

      processedCount += 1;
      flushCounter += 1;
      if (flushEvery > 0 && flushCounter >= flushEvery) {
        flushCounter = 0;
        await flush(`a cada ${flushEvery}`);
      }

      if (i < toRun.length - 1 && !interrupted) {
        const d = Math.max(0, config.mlBulkDelayMs);
        if (d > 0) await sleep(d);
      }
    }

    payload.meta.bulk_finished_at = new Date().toISOString();
    payload.meta.bulk_errors_count = errors.length;
    payload.meta.bulk_duplicates_skipped = duplicateCount;
    payload.meta.bulk_processed_in_run = processedCount;
    payload.meta.bulk_remaining_in_queue = Math.max(0, toRun.length - processedCount);

    const finalFlushReason = runStopReason === 'max_duration' ? 'limite_tempo' : 'final';
    await flush(finalFlushReason, false, true);
    if (runStopReason === 'max_duration') {
      console.info(
        `[ml-pdp-bulk] parado por tempo: ${Object.keys(itemsOut).length} únicos gravados, ${errors.length} erro(s), ${payload.meta.bulk_remaining_in_queue} item(ns) por processar na fila.`
      );
    } else {
      console.info(
        `[ml-pdp-bulk] concluído: ${Object.keys(itemsOut).length} únicos gravados, ${errors.length} erro(s)`
      );
    }
  } finally {
    browserForSignal = null;
    await page.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exitCode = 1;
});
