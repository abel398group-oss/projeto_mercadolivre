/**
 * Consumidor PDP do pipeline: **fonte operacional** = `pipeline_discovered.jsonl` (tail) + `pipeline_processed.jsonl`
 * (estado de itens já tratados) + ficheiro de offset. **Snapshots derivados** = `pdp_all.json`, lean, debug lean, métricas.
 * Arranque sem `pdp_all.json` pré-existente é suportado: o estado “já processado” vem do JSONL; o agregado repovoa-se nos flushes.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { launchBrowser } from '../browser.js';
import { config } from '../config.js';
import { buildBulkMetricsSnapshot } from '../io/bulkMetrics.js';
import { writeRunManifest } from '../io/runManifest.js';
import { writeSnapshot } from '../io/writeSnapshot.js';
import { appendJsonlLine } from '../io/jsonl.js';
import { sleep } from '../util.js';
import { dedupePrimaryKey } from './mlDedupe.js';
import { writePdpDebugLeanFromPayload } from './mlPdpDebugLean.js';
import { writePdpLeanFromPayload } from './mlPdpLean.js';
import { getPipelineSharedBrowser, isPipelineSharedBrowser } from './mlPipelineBrowser.js';
import { pipelineShutdownRequested } from './mlPipelineQueue.js';
import { warmPdpIdleTab } from './mlPdpIdleTabWarmup.js';
import { scrapeMlPdp } from './mlPdpScrape.js';

/** @typedef {{ product_id: string; url: string }} PipelineTask */

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
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

/**
 * IDs com PDP concluído com sucesso (persistente).
 * @param {string} processedPath
 */
async function loadProcessedSuccessIds(processedPath) {
  const set = new Set();
  try {
    const text = await fs.readFile(processedPath, 'utf8');
    for (const line of text.split('\n')) {
      if (!str(line)) continue;
      try {
        const o = JSON.parse(line);
        if (o && o.ok === true && o.product_id) set.add(str(o.product_id));
      } catch {
        /* ignore */
      }
    }
  } catch {
    /* sem ficheiro */
  }
  return set;
}

async function saveByteOffset(offsetPath, n) {
  const abs = path.resolve(offsetPath);
  await fs.mkdir(path.dirname(abs), { recursive: true });
  await fs.writeFile(abs, String(n), 'utf8');
}

/**
 * Lê novas linhas completas desde startByte (tail).
 * @param {string} discoveredPath
 * @param {number} startByte
 * @param {string} carry fragmento UTF-8 incompleto da leitura anterior
 */
async function tailDiscoveredLines(discoveredPath, startByte, carry) {
  let st;
  try {
    st = await fs.stat(discoveredPath);
  } catch {
    return { tasks: /** @type {PipelineTask[]} */ ([]), nextStartByte: 0, carry: '' };
  }
  if (st.size === 0) {
    return { tasks: [], nextStartByte: 0, carry: '' };
  }
  if (startByte > st.size) startByte = 0;
  const buf = await fs.readFile(discoveredPath);
  const slice = buf.subarray(startByte);
  const text = carry + slice.toString('utf8');
  const parts = text.split('\n');
  const nextCarry = parts.pop() ?? '';
  /** @type {PipelineTask[]} */
  const tasks = [];
  for (const p of parts) {
    if (!str(p)) continue;
    try {
      const o = JSON.parse(p);
      const product_id = str(o.product_id);
      const url = str(o.url);
      if (product_id && url) tasks.push({ product_id, url });
    } catch {
      /* ignore */
    }
  }
  const nextStartByte = st.size - Buffer.byteLength(nextCarry, 'utf8');
  return { tasks, nextStartByte, carry: nextCarry };
}

/**
 * Todas as tarefas no ficheiro (bootstrap / retoma).
 * @param {string} discoveredPath
 */
async function readAllDiscoveredTasks(discoveredPath) {
  /** @type {PipelineTask[]} */
  const tasks = [];
  let text;
  try {
    text = await fs.readFile(discoveredPath, 'utf8');
  } catch {
    return tasks;
  }
  for (const line of text.split('\n')) {
    if (!str(line)) continue;
    try {
      const o = JSON.parse(line);
      const product_id = str(o.product_id);
      const url = str(o.url);
      if (product_id && url) tasks.push({ product_id, url });
    } catch {
      /* ignore */
    }
  }
  return tasks;
}

/**
 * @param {{ producerDone: boolean; shutdown: boolean }} state
 */
export async function runPipelineConsumer(state) {
  const discoveredPath = path.resolve(config.mlPipelineDiscoveredJsonl);
  const processedPath = path.resolve(config.mlPipelineProcessedJsonl);
  const offsetPath = path.resolve(config.mlPipelineOffsetFile);
  const outFile = str(config.mlBulkOutput);
  const outPath = outFile && outFile !== '-' && String(outFile).toLowerCase() !== 'none' ? path.resolve(outFile) : '';
  const metricsFile = str(config.mlMetricsOutput);
  const metricsPath =
    metricsFile && metricsFile !== '-' && String(metricsFile).toLowerCase() !== 'none'
      ? path.resolve(metricsFile)
      : '';

  const processedSuccess = await loadProcessedSuccessIds(processedPath);
  /** @type {Record<string, unknown>} */
  const itemsOut = {};
  if (outPath) {
    try {
      const raw = JSON.parse(await fs.readFile(outPath, 'utf8'));
      const items = raw?.items && typeof raw.items === 'object' && !Array.isArray(raw.items) ? raw.items : {};
      for (const [k, v] of Object.entries(items)) {
        itemsOut[k] = v;
        processedSuccess.add(k);
      }
    } catch {
      /* novo */
    }
  }

  /** @type {Set<string>} */
  const doneIds = new Set(processedSuccess);
  /** @type {Set<string>} */
  const inQueue = new Set();
  /** Falhas definitivas nesta execução (evita re-enfileirar a mesma linha da fila várias vezes). */
  /** @type {Set<string>} */
  const gaveUpIds = new Set();
  /** @type {PipelineTask[]} */
  const queue = [];

  function enqueueIfNeeded(t) {
    if (doneIds.has(t.product_id)) return;
    if (gaveUpIds.has(t.product_id)) return;
    if (inQueue.has(t.product_id)) return;
    inQueue.add(t.product_id);
    queue.push(t);
  }

  const allDiscovered = await readAllDiscoveredTasks(discoveredPath);
  for (const t of allDiscovered) enqueueIfNeeded(t);

  let byteOffset = 0;
  let utf8Carry = '';
  try {
    byteOffset = (await fs.stat(discoveredPath)).size;
  } catch {
    byteOffset = 0;
  }
  await saveByteOffset(offsetPath, byteOffset);

  const recycleEvery = config.mlBulkBrowserRecycleEvery;
  const maxDurationMs = config.mlBulkMaxDurationMs;
  const pollMs = Math.max(200, config.mlPipelinePollMs);
  const flushEvery = Math.max(0, config.mlBulkFlushEvery);
  const runStartedMs = Date.now();
  const runStartedAtIso = new Date().toISOString();
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
  let flushCounter = 0;
  let scrapesDone = 0;

  /** @type {Map<string, string>} */
  const dedupeFirstCatalogKey = new Map();

  /** @type {{ product_id: string; url: string; message: string }[]} */
  const errors = [];

  const payload = {
    meta: {
      site_id: config.mlSiteId,
      pipeline: true,
      pipeline_discovered_path: discoveredPath,
      pipeline_processed_path: processedPath,
      bulk_started_at: runStartedAtIso,
      bulk_total_queued: queue.length,
      bulk_delay_ms: config.mlBulkDelayMs,
      bulk_max_items: config.mlBulkMaxItems,
      bulk_max_duration_ms: maxDurationMs,
      bulk_browser_recycle_every: recycleEvery,
      bulk_session_mode: 'pipeline_consumer',
    },
    items: itemsOut,
    errors,
  };

  async function writeOutput() {
    if (!outPath) return;
    await fs.mkdir(path.dirname(outPath), { recursive: true });
    const json = config.mlBulkPretty ? JSON.stringify(payload, null, 2) : JSON.stringify(payload);
    await fs.writeFile(outPath, json, 'utf8');
  }

  async function writeMetrics(reason, interruptedRun) {
    if (!metricsPath) return;
    const finishedIso = new Date().toISOString();
    const duration_seconds = Math.max(0, Math.round((Date.now() - runStartedMs) / 1000));
    const snap = buildBulkMetricsSnapshot({
      run_started_at: runStartedAtIso,
      run_finished_at: finishedIso,
      duration_seconds,
      total_input_items: Object.keys(itemsOut).length + queue.length + processedCount,
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
      debug_outputs: false,
      items: itemsOut,
      errors,
    });
    const indent = config.mlBulkPretty ? 2 : 0;
    await writeSnapshot({
      latestPath: metricsPath,
      historySubdir: 'metrics',
      historyBaseName: 'metrics',
      content: JSON.stringify(snap, null, indent),
    });
  }

  async function flush(reason, interruptedRun = false, withRunManifest = false) {
    payload.meta.bulk_stop_reason = runStopReason;
    await writeOutput();
    if (outPath) console.info(`[pipeline] gravado (${reason}): ${outPath}`);
    await writePdpLeanFromPayload(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-pdp-lean]', e instanceof Error ? e.message : e)
    );
    await writePdpDebugLeanFromPayload(/** @type {*} */ (payload)).catch((e) =>
      console.error('[ml-pdp-debug-lean]', e instanceof Error ? e.message : e)
    );
    await writeMetrics(reason, interruptedRun);
    if (metricsPath) console.info(`[pipeline] métricas (${reason}): ${metricsPath}`);
    if (withRunManifest) {
      await writeRunManifest({
        run_type: 'pipeline',
        run_started_at: runStartedAtIso,
        run_started_ms: runStartedMs,
      }).catch((e) => console.error('[run-manifest]', e instanceof Error ? e.message : e));
    }
  }

  /** Quando o `runPipeline.js` abre o Chrome antes, não fechar aqui (evita matar a listagem). */
  let ownsPipelineBrowser = false;
  /** @type {import('puppeteer').Browser} */
  let browser;
  const shared = getPipelineSharedBrowser();
  if (shared?.connected) {
    browser = shared;
    console.info('[pipeline] consumidor PDP: reutiliza o browser partilhado (listagem + PDP na mesma instância Chrome)');
  } else {
    const launched = await launchBrowser();
    browser = launched.browser;
    ownsPipelineBrowser = true;
  }
  /** @type {import('puppeteer').Page} */
  let page = await browser.newPage();
  await warmPdpIdleTab(page, '[pipeline]');
  let itemsInSession = 0;

  async function recycleBrowser(reason) {
    browserRestartCount += 1;
    console.info(`[pipeline] reciclagem do browser (${reason})`);
    await page.close().catch(() => {});
    if (isPipelineSharedBrowser(browser)) {
      console.warn('[pipeline] browser partilhado: não fechar a instância; só nova página PDP.');
      page = await browser.newPage();
      await warmPdpIdleTab(page, '[pipeline]');
      itemsInSession = 0;
      return;
    }
    await browser.close().catch(() => {});
    const launched = await launchBrowser();
    browser = launched.browser;
    ownsPipelineBrowser = true;
    page = await browser.newPage();
    await warmPdpIdleTab(page, '[pipeline]');
    itemsInSession = 0;
  }

  async function recreatePageOnly(reason) {
    pageReloadCount += 1;
    console.warn(`[pipeline] nova página (${reason})`);
    await page.close().catch(() => {});
    page = await browser.newPage();
    await warmPdpIdleTab(page, '[pipeline]');
    itemsInSession = 0;
  }

  let idleTicks = 0;
  const maxIdleTicks = 25;

  try {
    while (true) {
      if (pipelineShutdownRequested()) {
        runStopReason = 'sigint';
        break;
      }
      if (timeBudgetExceeded()) {
        runStopReason = 'max_duration';
        console.warn(`[pipeline] limite ML_BULK_MAX_DURATION_MS atingido — a gravar.`);
        break;
      }

      const tail = await tailDiscoveredLines(discoveredPath, byteOffset, utf8Carry);
      byteOffset = tail.nextStartByte;
      utf8Carry = tail.carry;
      await saveByteOffset(offsetPath, byteOffset);
      for (const t of tail.tasks) enqueueIfNeeded(t);

      if (queue.length === 0) {
        idleTicks += 1;
        if (state.producerDone && idleTicks >= maxIdleTicks) break;
        await sleep(pollMs);
        continue;
      }

      idleTicks = 0;

      if (recycleEvery != null && scrapesDone > 0 && scrapesDone % recycleEvery === 0) {
        await recycleBrowser('periódico');
      }

      const task = /** @type {PipelineTask} */ (queue.shift());
      inQueue.delete(task.product_id);
      const { product_id: pid, url } = task;

      console.info(`[pipeline] processed item ${pid} (fila ~${queue.length})`);

      /** @type {import('../productSchema.js').CanonicalProduct & { price_currency?: string } | null} */
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
          console.error(`[pipeline] tentativa ${attempt}/${maxAttempts} falhou ${pid}:`, lastMessage);

          if (/account_verification/i.test(lastMessage)) {
            console.warn('[pipeline] account_verification — a reciclar browser (sem novas tentativas para este item).');
            try {
              await recycleBrowser('account_verification');
            } catch (re) {
              console.error('[pipeline] falha ao reciclar após account_verification:', re instanceof Error ? re.message : re);
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
              await recycleBrowser('recuperação (sessão inválida)');
            } catch (re) {
              console.error('[pipeline] falha ao relançar browser:', re instanceof Error ? re.message : re);
              pdp = null;
              break;
            }
            continue;
          }
          if (isLikelyBrokenPage(lastMessage)) {
            try {
              await recreatePageOnly('recuperação (página/contexto)');
            } catch (re) {
              console.error('[pipeline] falha ao recriar página:', re instanceof Error ? re.message : re);
              pdp = null;
              break;
            }
            continue;
          }
          pdp = null;
          break;
        }
      }

      processedCount += 1;
      scrapesDone += 1;

      if (pdp) {
        itemsInSession += 1;
        successScrapeCount += 1;
        const dedupeKey = dedupePrimaryKey(/** @type {Record<string, unknown>} */ (pdp));
        if (dedupeFirstCatalogKey.has(dedupeKey)) {
          duplicateCount += 1;
          await appendJsonlLine(processedPath, {
            product_id: pid,
            processed_at: new Date().toISOString(),
            ok: true,
            note: 'duplicate_of_first',
          });
          doneIds.add(pid);
        } else {
          dedupeFirstCatalogKey.set(dedupeKey, pid);
          itemsOut[pid] = pdp;
          await appendJsonlLine(processedPath, {
            product_id: pid,
            processed_at: new Date().toISOString(),
            ok: true,
          });
        }
        doneIds.add(pid);
      } else if (lastMessage) {
        errors.push({ product_id: pid, url, message: lastMessage });
        await appendJsonlLine(processedPath, {
          product_id: pid,
          processed_at: new Date().toISOString(),
          ok: false,
          error: lastMessage.slice(0, 500),
        });
        gaveUpIds.add(pid);
      }

      flushCounter += 1;
      if (flushEvery > 0 && flushCounter >= flushEvery) {
        flushCounter = 0;
        await flush(`a cada ${flushEvery}`, false);
      }

      const d = Math.max(0, config.mlBulkDelayMs);
      if (d > 0) await sleep(d);
    }

    payload.meta.bulk_finished_at = new Date().toISOString();
    payload.meta.bulk_errors_count = errors.length;
    payload.meta.bulk_duplicates_skipped = duplicateCount;
    payload.meta.bulk_processed_in_run = processedCount;
    await flush(
      runStopReason === 'max_duration' ? 'limite_tempo' : 'pipeline_final',
      runStopReason === 'sigint',
      true
    );
  } finally {
    await page.close().catch(() => {});
    if (ownsPipelineBrowser) await browser.close().catch(() => {});
  }
}
