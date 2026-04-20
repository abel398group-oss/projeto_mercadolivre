import fs from 'node:fs/promises';
import path from 'node:path';
import { config } from './config.js';
import {
  calculateCompletenessScore,
  calculateMlScore,
  getMlScoreSignals,
  isShippingInconsistent,
} from './ml/mlScore.js';

const inputPath = path.resolve(String(config.mlAnalyzeInput || '').trim());
const outputPath = path.resolve(String(config.mlAnalyzeOutput || '').trim());

/**
 * @param {number} score
 * @returns {'alta' | 'media' | 'baixa'}
 */
function opportunityLevel(score) {
  if (score >= 80) return 'alta';
  if (score >= 60) return 'media';
  return 'baixa';
}

/**
 * @param {Record<string, unknown>} product
 * @returns {string[]}
 */
function collectWarnings(product) {
  /** @type {string[]} */
  const w = [];

  const pid = String(product.product_id ?? '');
  if (/^MLBU/i.test(pid)) {
    w.push('product_id é catálogo (MLBU), não item MLB — URLs /p/ podem divergir do item API');
  }

  if (product.rating_count == null || product.rating_count === '') {
    w.push('rating_count ausente — confiança da avaliação limitada');
  }

  if (!String(product.seller_id ?? '').trim()) {
    w.push('seller_id vazio — rastreio do vendedor incompleto');
  }

  if (isShippingInconsistent(product)) {
    w.push('shipping inconsistente (is_free com preço sem contexto de promoção/primeira compra)');
  }

  return w;
}

/**
 * @param {unknown} raw
 * @returns {Record<string, Record<string, unknown>>}
 */
function extractProductsMap(raw) {
  if (!raw || typeof raw !== 'object') return {};
  const o = /** @type {Record<string, unknown>} */ (raw);

  if (o.items && typeof o.items === 'object' && !Array.isArray(o.items)) {
    return /** @type {Record<string, Record<string, unknown>>} */ (o.items);
  }

  if (Array.isArray(o.products)) {
    /** @type {Record<string, Record<string, unknown>>} */
    const out = {};
    for (const row of o.products) {
      if (!row || typeof row !== 'object') continue;
      const id = String(/** @type {Record<string, unknown>} */ (row).product_id ?? '').trim();
      if (id) out[id] = /** @type {Record<string, unknown>} */ (row);
    }
    return out;
  }

  return {};
}

async function main() {
  let rawText;
  try {
    rawText = await fs.readFile(inputPath, 'utf8');
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`[ml-analyze] Não consegui ler: ${inputPath}`);
    console.error(msg);
    console.error('Gera antes: npm run catalog:pdp   ou define ML_ANALYZE_INPUT=');
    process.exit(1);
    return;
  }

  /** @type {unknown} */
  let data;
  try {
    data = JSON.parse(rawText);
  } catch (e) {
    console.error('[ml-analyze] JSON inválido:', e instanceof Error ? e.message : e);
    process.exit(1);
    return;
  }

  const map = extractProductsMap(data);
  const ids = Object.keys(map).sort();

  /** @type {unknown[]} */
  const products = [];

  for (const id of ids) {
    const row = map[id];
    const score = calculateMlScore(row);
    const completeness_score = calculateCompletenessScore(row);
    const signals = getMlScoreSignals(row);
    const warnings = collectWarnings(/** @type {Record<string, unknown>} */ (row));

    products.push({
      product_id: String(/** @type {Record<string, unknown>} */ (row).product_id ?? id),
      name: String(/** @type {Record<string, unknown>} */ (row).name ?? ''),
      score,
      completeness_score,
      opportunity_level: opportunityLevel(score),
      signals,
      warnings,
    });
  }

  products.sort(
    (a, b) =>
      Number(/** @type {{ score: number }} */ (b).score) -
      Number(/** @type {{ score: number }} */ (a).score)
  );

  const payload = {
    meta: {
      generated_at: new Date().toISOString(),
      source_file: inputPath,
      products_count: products.length,
    },
    products,
  };

  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  const json = config.mlAnalyzePretty ? JSON.stringify(payload, null, 2) : JSON.stringify(payload);
  await fs.writeFile(outputPath, json, 'utf8');

  console.info(`[ml-analyze] ${products.length} produto(s) → ${outputPath}`);
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exitCode = 1;
});
