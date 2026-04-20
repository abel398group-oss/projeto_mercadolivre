/**
 * Fila persistente do pipeline (descoberta → PDP). Sem analytics.
 */

import { appendJsonlLine } from '../io/jsonl.js';
import { config } from '../config.js';
import { resolveCatalogItemToPdpUrl } from './mlExtract.js';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

export function isPipelineMode() {
  return process.env.ML_PIPELINE_ACTIVE === '1';
}

export function pipelineShutdownRequested() {
  return process.env.ML_PIPELINE_SHUTDOWN === '1';
}

/**
 * Grava uma linha na fila de descoberta (produtor).
 * @param {Record<string, unknown>} record registo canónico após merge no catálogo
 */
export async function notifyPipelineItemDiscovered(record) {
  if (!isPipelineMode()) return;
  const outPath = str(config.mlPipelineDiscoveredJsonl);
  if (!outPath) return;
  const product_id = str(record?.product_id);
  if (!product_id) return;
  const url = resolveCatalogItemToPdpUrl(/** @type {*} */ (record));
  if (!url) return;
  /** @type {Record<string, string>} */
  const line = {
    product_id,
    url,
    discovered_at: new Date().toISOString(),
  };
  const cat = record?.category_source_id;
  if (cat != null && str(cat)) line.category_source_id = str(cat);
  await appendJsonlLine(outPath, line);
  console.info(`[pipeline] discovered item ${product_id}`);
}
