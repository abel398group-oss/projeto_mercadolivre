/**
 * Testes com HTML congelado em test/fixtures — sem browser.
 * Executar: npm run test:fixtures
 */

import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { challengeHtmlLooksBlocked } from '../src/ml/mlAccountChallenge.js';
import { buildEmbeddedPartialFromHtml } from '../src/ml/mlEmbeddedStateExtract.js';
import { htmlLooksLikeSearchListing, isListaSuspiciousTrafficHtml } from '../src/ml/mlListaHtmlExtract.js';
import { extractListaCategoryPaths, extractMlbIdsFromListaHtml } from '../src/ml/mlListaCatalog.js';
import { buildPdpCoreProduct } from '../src/ml/mlPdpCore.js';
import { buildPdpDebugLeanSnapshot, sanitizeConflict } from '../src/ml/mlPdpDebugLean.js';
import { buildPdpLean } from '../src/ml/mlPdpLean.js';
import { computeCanonicalProductId } from '../src/ml/mlProductFinalize.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** @param {...string} parts relative to test/fixtures */
function readFixture(...parts) {
  const p = path.join(__dirname, 'fixtures', ...parts);
  return fs.readFileSync(p, 'utf8');
}

function testListaNormal() {
  const html = readFixture('lista', 'lista_normal.html');
  const ids = extractMlbIdsFromListaHtml(html);
  assert.ok(ids.size >= 2, 'deve extrair pelo menos 2 MLB da listagem');
  assert.ok(ids.has('MLB2233445566'));
  assert.ok(ids.has('MLB9988776611'));
  assert.strictEqual(htmlLooksLikeSearchListing(html), true, 'deve parecer listagem de busca');
  assert.strictEqual(isListaSuspiciousTrafficHtml(html), false);
  assert.strictEqual(challengeHtmlLooksBlocked(html, 'https://lista.mercadolivre.com.br/foo'), false);
}

function testCategoriasNormal() {
  const html = readFixture('lista', 'categorias_normal.html');
  const paths = extractListaCategoryPaths(html);
  assert.ok(paths.length >= 3, 'deve extrair paths de categorias');
  assert.ok(paths.some((p) => p === 'eletrodomesticos'));
  assert.ok(paths.some((p) => p === 'informatica'));
  assert.ok(paths.some((p) => p === 'celulares'));
}

function testListaBloqueio() {
  const html = readFixture('lista', 'lista_account_verification.html');
  assert.strictEqual(challengeHtmlLooksBlocked(html, ''), true, 'marcador duro de verificação');
  assert.strictEqual(isListaSuspiciousTrafficHtml(html), true);
  assert.strictEqual(htmlLooksLikeSearchListing(html), false, 'não é listagem válida');
}

function testPdpNormal() {
  const html = readFixture('pdp', 'pdp_normal.html');
  const { found, partial } = buildEmbeddedPartialFromHtml(html);
  assert.strictEqual(found, true);
  assert.strictEqual(partial.item_id, 'MLB2233445566');
  assert.strictEqual(partial.name, 'Produto Fixture PDP Normal');
  assert.strictEqual(partial.price_current, 1299.99);
  assert.strictEqual(partial.seller_id, '5544332211');
  assert.strictEqual(partial.category_id, 'MLB5678');
  assert.strictEqual(partial.domain_id, 'MLB-CELLPHONES');
}

function testPdpIdDivergente() {
  const html = readFixture('pdp', 'pdp_id_divergente.html');
  const { partial } = buildEmbeddedPartialFromHtml(html);
  assert.strictEqual(partial.catalog_product_id, 'MLBU8877665544');
  assert.strictEqual(partial.item_id, 'MLB9988776655');

  /** Simula chave de listagem (fila) + corpo PDP embutido */
  const merged = {
    ...partial,
    listing_product_id: 'MLB1111111111',
    product_id: 'MLB1111111111',
  };
  assert.strictEqual(computeCanonicalProductId(merged), 'MLB9988776655', 'canonical = item_id quando presente');

  const lean = buildPdpLean(merged);
  assert.strictEqual(lean.listing_product_id, 'MLB1111111111');
  assert.strictEqual(lean.item_id, 'MLB9988776655');
  assert.strictEqual(lean.canonical_id || computeCanonicalProductId(merged), 'MLB9988776655');
}

function testExportsCoreLeanDebug() {
  const rich = {
    product_id: 'MLB1',
    listing_product_id: 'MLB1',
    item_id: 'MLB1',
    name: 'X',
    price_current: 10,
    description: 'Texto longo de descrição para o lean',
    pdp_attributes_table: { Marca: 'Teste' },
    review_summary_ai: { summary: 'não deve aparecer no core' },
    pdp_subtitle: 'sub',
    url: 'https://www.mercadolivre.com.br/p/MLB1',
    collected_at: new Date().toISOString(),
  };

  const core = buildPdpCoreProduct(rich);
  assert.strictEqual('description' in core, false);
  assert.strictEqual('pdp_attributes_table' in core, false);
  assert.strictEqual('review_summary_ai' in core, false);

  const lean = buildPdpLean(rich);
  assert.strictEqual(lean.description, rich.description);
  assert.deepStrictEqual(lean.pdp_attributes_table, rich.pdp_attributes_table);

  const conf = sanitizeConflict({
    field: 'url',
    rejected_source: 'a',
    kept_source: 'b',
    rejected_value: 'https://click1.mercadolivre.com.br/tracking?x=1',
    kept_value: 'https://www.mercadolivre.com.br/p/MLB1',
  });
  assert.strictEqual(conf.rejected_value, '[tracking_or_raw_url]');
  assert.strictEqual(conf.kept_value, '[derived_or_raw_url]');

  const debug = buildPdpDebugLeanSnapshot({
    meta: {},
    items: {
      k1: {
        ...rich,
        _source_conflicts: [
          {
            field: 'url_primary',
            rejected_source: 'x',
            kept_source: 'y',
            rejected_value: 'https://evil.example/track',
            kept_value: 'https://www.mercadolivre.com.br/p/MLB1',
          },
        ],
        _field_rejections: [],
      },
    },
  });
  const c = /** @type {Record<string, unknown>} */ (debug.items.k1)._source_conflicts[0];
  assert.strictEqual(c.rejected_value, '[tracking_or_raw_url]');
  assert.strictEqual(c.kept_value, '[derived_or_raw_url]');
}

function run() {
  testListaNormal();
  testCategoriasNormal();
  testListaBloqueio();
  testPdpNormal();
  testPdpIdDivergente();
  testExportsCoreLeanDebug();
  console.log('scraper.fixtures.spec.js: ok');
}

run();
