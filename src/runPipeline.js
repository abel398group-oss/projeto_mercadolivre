/**
 * Pipeline paralelo: descoberta (catálogo) + PDP em simultâneo.
 *
 * **Fonte operacional (append-only / tail):**
 * - `ML_PIPELINE_DISCOVERED_JSONL` → fila de itens descobertos (ex.: `./output/pipeline_discovered.jsonl`)
 * - `ML_PIPELINE_PROCESSED_JSONL` → registo de PDP processados (ex.: `./output/pipeline_processed.jsonl`)
 * - `ML_PIPELINE_OFFSET_FILE` → offset do tail no discovered
 *
 * **Snapshots derivados (JSON agregado / export):** `catalogo_ml.json`, `catalogo_ml_lean.json`, `pdp_all.json`,
 * `pdp_all_lean.json`, `pdp_debug_lean.json`, `metrics.json` — gerados a partir do estado em memória + flush;
 * não substituem a fila JSONL para retoma operacional.
 *
 * Variáveis úteis: ML_PIPELINE_FRESH=true (limpa só fila JSONL + offset no arranque), ML_BULK_MAX_DURATION_MS, ML_BULK_FLUSH_EVERY, etc.
 */

import 'dotenv/config';
import fs from 'node:fs/promises';
import path from 'node:path';
import { launchBrowser, logChromePersistentProfileSummary } from './browser.js';
import { config } from './config.js';
import { runFullCatalog } from './ml/mlCatalogRun.js';
import {
  clearPipelineSharedBrowser,
  setPipelineSharedBrowser,
} from './ml/mlPipelineBrowser.js';
import { runPipelineConsumer } from './ml/mlPipelineConsumer.js';

process.env.ML_PIPELINE_ACTIVE = '1';

/** @type {{ producerDone: boolean; shutdown: boolean }} */
const state = { producerDone: false, shutdown: false };

async function maybeTruncatePipelineFiles() {
  if (!config.mlPipelineFresh) return;
  const files = [
    config.mlPipelineDiscoveredJsonl,
    config.mlPipelineProcessedJsonl,
    config.mlPipelineOffsetFile,
  ];
  for (const f of files) {
    const p = path.resolve(String(f || '').trim());
    if (!p) continue;
    try {
      await fs.unlink(p);
      console.info(`[pipeline] fresh: removido ${p}`);
    } catch {
      /* não existia */
    }
  }
}

await maybeTruncatePipelineFiles();
logChromePersistentProfileSummary('[pipeline]');

/** Um único Chrome para listagem + PDP (evita lock de perfil e permite sessão comum). */
let pipelineBrowser = null;
try {
  console.info('[pipeline] a abrir Chrome partilhado (lista + consumidor PDP)…');
  const launched = await launchBrowser();
  pipelineBrowser = launched.browser;
  setPipelineSharedBrowser(pipelineBrowser);
} catch (e) {
  console.error('[pipeline] falha ao abrir browser partilhado:', e instanceof Error ? e.message : e);
  throw e;
}

process.on('SIGINT', () => {
  process.env.ML_PIPELINE_SHUTDOWN = '1';
  state.shutdown = true;
  console.info('[pipeline] graceful shutdown requested');
});

console.info('[pipeline] producer started');
console.info('[pipeline] consumer started');

const consumerPromise = runPipelineConsumer(state).catch((e) => {
  console.error('[pipeline] consumer erro:', e instanceof Error ? e.message : e);
});

const producerPromise = runFullCatalog()
  .then(() => {
    state.producerDone = true;
    console.info('[pipeline] producer finished');
  })
  .catch((e) => {
    console.error('[pipeline] producer erro:', e instanceof Error ? e.message : e);
    state.producerDone = true;
  });

try {
  await Promise.all([producerPromise, consumerPromise]);
} finally {
  clearPipelineSharedBrowser();
  if (pipelineBrowser) {
    await pipelineBrowser.close().catch(() => {});
    pipelineBrowser = null;
  }
}

delete process.env.ML_PIPELINE_ACTIVE;
delete process.env.ML_PIPELINE_SHUTDOWN;
console.info('[pipeline] graceful shutdown complete');
