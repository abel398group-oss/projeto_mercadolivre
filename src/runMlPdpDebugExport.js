/**
 * Gera output/pdp_debug_lean.json a partir de um pdp_all.json já existente.
 */
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { config } from './config.js';
import { writePdpDebugLeanFromPayload } from './ml/mlPdpDebugLean.js';

function trimStr(v) {
  return v == null ? '' : String(v).trim();
}

const outCfg = trimStr(config.mlPdpDebugLeanOutput);
if (!outCfg || outCfg === '-' || outCfg.toLowerCase() === 'none') {
  console.error(
    '[ml-pdp-debug-lean] ML_PDP_DEBUG_LEAN_OUTPUT está desligado; define um caminho (ex.: ./output/pdp_debug_lean.json).'
  );
  process.exit(1);
}

const inPath = path.resolve(
  process.argv[2] || process.env.ML_PDP_DEBUG_SOURCE || process.env.ML_PDP_LEAN_SOURCE || './output/pdp_all.json'
);

const raw = await fsPromises.readFile(inPath, 'utf8');
const payload = JSON.parse(raw);
await writePdpDebugLeanFromPayload(/** @type {*} */ (payload));
console.info(`[ml-pdp-debug-lean] origem: ${inPath} → ${path.resolve(outCfg)}`);
