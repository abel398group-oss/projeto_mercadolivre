/**
 * Testes (sem browser): `npm run test:embedded` ou `node src/ml/mlEmbeddedStateExtract.spec.js`
 */

import assert from 'node:assert';
import crypto from 'node:crypto';
import {
  buildEmbeddedPartialFromHtml,
  extractEmbeddedStateFromHtml,
  normalizeEmbeddedStateToPartial,
  parseJsStringLiteral,
  safeDecodeURIComponent,
  tryConsumeJsonParseCall,
} from './mlEmbeddedStateExtract.js';

const htmlPreload = `<!doctype html><html><head></head><body>
<script>
window.__PRELOADED_STATE__ = {"page":{"product":{"id":"MLBU9998887776","name":"Item catálogo","price":149.9,"currency_id":"BRL","sold_quantity":42,"seller":{"id":"123456789","nickname":"LojaTeste"}}}};
</script>
</body></html>`;

const htmlNext = `<!doctype html><html><body>
<script id="__NEXT_DATA__" type="application/json">{"props":{"pageProps":{"item":{"id":"MLB1234567890","title":"Next item","price":55,"sold_quantity":10}}}}</script>
</body></html>`;

/** Escape string for use inside JSON.stringify for embedding in JS double-quoted string */
function escapeForDoubleQuotedJsJson(obj) {
  return JSON.stringify(JSON.stringify(obj)).slice(1, -1);
}

function run() {
  const a = extractEmbeddedStateFromHtml(htmlPreload);
  assert.strictEqual(a.found, true);
  const preloadBlob = a.blobs.find((b) => b.type === 'preloaded_state' && b.parsed);
  assert.ok(preloadBlob);
  assert.ok(preloadBlob?.subtype?.includes('embedded_preloaded') || preloadBlob?.subtype === 'embedded_preloaded_state');
  assert.strictEqual(preloadBlob?.extraction_method, 'direct_balanced_object');
  assert.strictEqual(preloadBlob?.confidence, 'high');

  const b = buildEmbeddedPartialFromHtml(htmlPreload);
  assert.ok(b.partial.catalog_product_id || b.partial.name);
  assert.strictEqual(b.field_sources.name, 'embedded_json');

  const n = normalizeEmbeddedStateToPartial({ nested: { id: 'MLB1112223334', plain_text: 'Nome longo', price: 10 } });
  assert.ok(n.partial.item_id || n.partial.name);

  const c = extractEmbeddedStateFromHtml(htmlNext);
  assert.ok(c.blobs.some((x) => x.type === 'next_data' && x.parsed));

  const d = buildEmbeddedPartialFromHtml('<html><body>sem estado</body></html>');
  assert.strictEqual(d.found, false);

  /* JSON.parse("...") com string escapada */
  const inner = { p: { id: 'MLB8888888888', price: 77, permalink: 'https://www.mercadolivre.com.br/x' } };
  const esc = escapeForDoubleQuotedJsJson(inner);
  const htmlJsonParse = `<script>window.__PRELOADED_STATE__ = JSON.parse("${esc}");</script>`;
  const jp = extractEmbeddedStateFromHtml(htmlJsonParse);
  assert.ok(jp.found, 'JSON.parse double quote');
  const jpBlob = jp.blobs.find((x) => x.subtype === 'embedded_json_parse' && x.parsed);
  assert.ok(jpBlob);
  assert.strictEqual(jpBlob?.extraction_method, 'json_parse_string');

  /* JSON.parse(decodeURIComponent("...")) */
  const uriPayload = '{"item":{"id":"MLB7777777777","sold_quantity":3,"price":12}}';
  const uriEnc = encodeURIComponent(uriPayload);
  const htmlUri = `<script>window.__INITIAL_STATE__ = JSON.parse(decodeURIComponent("${uriEnc}"));</script>`;
  const u = extractEmbeddedStateFromHtml(htmlUri);
  const ub = u.blobs.find((x) => x.subtype === 'embedded_json_parse_uri' && x.parsed);
  assert.ok(ub);
  assert.strictEqual(ub?.extraction_method, 'json_parse_decode_uri');

  /* aspas simples em JSON.parse */
  const escSingle = JSON.stringify(inner);
  const htmlSingle = `<script>window.__APP_STATE__ = JSON.parse('${escSingle.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}');</script>`;
  const sq = extractEmbeddedStateFromHtml(htmlSingle);
  assert.ok(sq.blobs.some((b) => b.parsed && b.subtype === 'embedded_json_parse'));

  /* alias: const state = JSON.parse(...) ; window.__PRELOADED_STATE__ = state */
  const esc2 = escapeForDoubleQuotedJsJson(inner);
  const htmlAlias = `<script>
const state = JSON.parse("${esc2}");
window.__PRELOADED_STATE__ = state;
</script>`;
  const al = extractEmbeddedStateFromHtml(htmlAlias);
  assert.ok(al.blobs.some((b) => b.subtype === 'embedded_alias_assignment' && b.type === 'preloaded_state'));
  assert.ok(
    al.blobs.some((b) => b.extraction_method === 'alias_to_global_window'),
    'alias_to_global_window'
  );

  /* Base64 JSON válido (com hint ML) dentro de aspas — run ≥120 chars para o regex */
  const b64Inner = JSON.stringify({
    catalog: { id: 'MLBU1234567890', seller_id: '999', price: 199 },
    permalink: 'https://www.mercadolivre.com.br/p/MLB1234567890',
    extra: 'x'.repeat(200),
  });
  const b64 = Buffer.from(b64Inner, 'utf8').toString('base64');
  assert.ok(b64.length >= 120, `base64 length ${b64.length}`);
  const htmlB64 = `<script>var x = "${b64}";</script>`;
  const bx = extractEmbeddedStateFromHtml(htmlB64);
  assert.ok(bx.blobs.some((b) => b.subtype === 'embedded_base64_json' && b.parsed));

  /* Payload JSON válido mas sem indícios ML — não deve dar found com esse blob sozinho */
  const noMl = extractEmbeddedStateFromHtml(
    '<script>window.__PRELOADED_STATE__ = {"foo":{"bar":1}};</script>'
  );
  assert.strictEqual(
    noMl.blobs.some((b) => b.type === 'preloaded_state' && b.parsed),
    false
  );

  /* Falso positivo aparente: base64 curto */
  const shortB64 = crypto.randomBytes(30).toString('base64');
  const fp = extractEmbeddedStateFromHtml(`<script>var y = "${shortB64}";</script>`);
  assert.strictEqual(fp.blobs.some((x) => x.subtype === 'embedded_base64_json'), false);

  /* parseJsStringLiteral com escape: corpo = a, aspa, b, barra, c */
  const lit = parseJsStringLiteral('"' + 'a' + '\\"' + 'b' + '\\\\' + 'c' + '"', 0);
  assert.strictEqual(lit?.value, 'a"b\\c');

  /* safeDecodeURIComponent */
  assert.strictEqual(safeDecodeURIComponent('%20x'), ' x');
  assert.strictEqual(safeDecodeURIComponent('%E0%A4%A'), null);

  const at = tryConsumeJsonParseCall(`JSON.parse("{}")`, 0);
  assert.strictEqual(at?.parsed ? Object.keys(at.parsed).length : -1, 0);

  console.log('mlEmbeddedStateExtract.spec.js: ok');
}

run();
