/**
 * Gera output/pdp_all_lean.json (ou ML_PDP_LEAN_OUTPUT) a partir de um pdp_all.json já existente.
 * Não altera scraping, merge nem validação.
 */
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { config } from './config.js';
import { writePdpDebugLeanFromPayload } from './ml/mlPdpDebugLean.js';
import { writePdpLeanFromPayload } from './ml/mlPdpLean.js';

/** @param {unknown} v */
function trimStr(v) {
  return v == null ? '' : String(v).trim();
}

const outCfg = trimStr(config.mlPdpLeanOutput);
if (!outCfg || outCfg === '-' || outCfg.toLowerCase() === 'none') {
  console.error('[ml-pdp-lean] ML_PDP_LEAN_OUTPUT está desligado; define um caminho (ex.: ./output/pdp_all_lean.json).');
  process.exit(1);
}

const inPath = path.resolve(
  process.argv[2] || process.env.ML_PDP_LEAN_SOURCE || './output/pdp_all.json'
);

const raw = await fsPromises.readFile(inPath, 'utf8');
const payload = JSON.parse(raw);
await writePdpLeanFromPayload(/** @type {*} */ (payload));
await writePdpDebugLeanFromPayload(/** @type {*} */ (payload)).catch((e) =>
  console.error('[ml-pdp-debug-lean]', e instanceof Error ? e.message : e)
);
console.info(`[ml-pdp-lean] origem: ${inPath} → ${path.resolve(outCfg)}`);
