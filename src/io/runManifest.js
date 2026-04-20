/**
 * Resumo de execução (pipeline / bulk): só lê ficheiros já gravados e escreve run_manifest.json.
 *
 * Modelo de dados:
 * - `operational_sources`: JSONL do pipeline (fila de descoberta + registo de processamento) — fonte operacional.
 * - `derived_outputs`: JSON agregados e lean/métricas — snapshots exportados derivados dos fluxos de gravação.
 * Nos runs `bulk_pdp`, os caminhos em `operational_sources` são os mesmos da config (pipeline); esse run não os atualiza.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { config } from '../config.js';
import { LEAN_SCHEMA_VERSIONS } from '../ml/leanSchemaVersions.js';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {string} configPath
 */
function outputPathDisplay(configPath) {
  const s = str(configPath);
  if (!s || s === '-' || s.toLowerCase() === 'none') return '';
  const abs = path.resolve(s);
  const rel = path.relative(process.cwd(), abs);
  if (rel === '') return './';
  const norm = rel.split(path.sep).join('/');
  return norm.startsWith('.') ? norm : `./${norm}`;
}

/**
 * @param {string} startedAtIso
 */
export function formatRunId(startedAtIso) {
  const d = new Date(startedAtIso);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
}

/**
 * @param {{
 *   run_type: 'pipeline' | 'bulk_pdp';
 *   run_started_at: string;
 *   run_started_ms?: number;
 * }} opts
 */
export async function writeRunManifest(opts) {
  const outFile = str(config.mlRunManifestOutput);
  if (!outFile || outFile === '-' || outFile.toLowerCase() === 'none') return;

  const manifestPath = path.resolve(outFile);
  const metricsFile = str(config.mlMetricsOutput);
  const metricsPath =
    metricsFile && metricsFile !== '-' && metricsFile.toLowerCase() !== 'none' ? path.resolve(metricsFile) : '';

  /** @type {Record<string, unknown>} */
  let m = {};
  if (metricsPath) {
    try {
      const raw = JSON.parse(await fs.readFile(metricsPath, 'utf8'));
      if (raw && typeof raw === 'object') m = /** @type {Record<string, unknown>} */ (raw);
    } catch {
      /* métricas indisponíveis */
    }
  }

  const catalogLeanFile = str(config.mlCatalogLeanOutput);
  let catalog_items = 0;
  if (catalogLeanFile && catalogLeanFile !== '-' && catalogLeanFile.toLowerCase() !== 'none') {
    try {
      const raw = JSON.parse(await fs.readFile(path.resolve(catalogLeanFile), 'utf8'));
      const items = raw?.items && typeof raw.items === 'object' && !Array.isArray(raw.items) ? raw.items : {};
      catalog_items = Object.keys(items).length;
    } catch {
      catalog_items = 0;
    }
  }

  const started_at = typeof m.run_started_at === 'string' ? m.run_started_at : opts.run_started_at;
  const finished_at = typeof m.run_finished_at === 'string' ? m.run_finished_at : new Date().toISOString();
  let duration_seconds = typeof m.duration_seconds === 'number' ? m.duration_seconds : null;
  if (duration_seconds == null && opts.run_started_ms != null) {
    duration_seconds = Math.max(0, Math.round((Date.now() - opts.run_started_ms) / 1000));
  }

  const manifest = {
    run_id: formatRunId(opts.run_started_at),
    run_type: opts.run_type,
    started_at,
    finished_at,
    duration_seconds,
    schema_versions: { ...LEAN_SCHEMA_VERSIONS },
    counts: {
      catalog_items,
      processed_items: typeof m.processed_items === 'number' ? m.processed_items : 0,
      success_items: typeof m.success_items === 'number' ? m.success_items : 0,
      unique_items_stored: typeof m.unique_items_stored === 'number' ? m.unique_items_stored : 0,
    },
    operational_sources: {
      discovered_jsonl: outputPathDisplay(config.mlPipelineDiscoveredJsonl),
      processed_jsonl: outputPathDisplay(config.mlPipelineProcessedJsonl),
    },
    derived_outputs: {
      catalog: outputPathDisplay(config.mlCatalogOutput),
      catalog_lean: outputPathDisplay(config.mlCatalogLeanOutput),
      pdp: outputPathDisplay(config.mlBulkOutput),
      pdp_core: outputPathDisplay(config.mlPdpCoreOutput),
      pdp_lean: outputPathDisplay(config.mlPdpLeanOutput),
      pdp_debug: outputPathDisplay(config.mlPdpDebugLeanOutput),
      metrics: outputPathDisplay(config.mlMetricsOutput),
    },
  };

  const indent = config.mlBulkPretty ? 2 : 0;
  await fs.mkdir(path.dirname(manifestPath), { recursive: true });
  await fs.writeFile(manifestPath, JSON.stringify(manifest, null, indent || undefined), 'utf8');
  console.info(`[run-manifest] gravado → ${manifestPath}`);
}
