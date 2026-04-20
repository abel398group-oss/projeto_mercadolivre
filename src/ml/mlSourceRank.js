/**
 * Hierarquia de confiança das fontes (maior número vence em conflito).
 * 1. API oficial → api_item / api_product
 * 2. Estado embutido no HTML → embedded_json (__PRELOADED_STATE__, __NEXT_DATA__, etc.)
 * 3. JSON-LD
 * 4. DOM
 * 5. Heurística / listagem
 * 6. Regex em texto
 */

/** @typedef {keyof typeof SOURCE_RANK | string} SourceId */

export const SOURCE_RANK = /** @type {const} */ ({
  api_item: 60,
  api_product: 58,
  embedded_json: 50,
  json_ld: 40,
  dom: 30,
  listing_network: 25,
  heuristic: 22,
  regex_text: 15,
  category_ssr: 10,
  /** Chave de descoberta no catálogo/fila (só preenche listing_product_id; não compete com PDP). */
  catalog_discovery: 8,
  /** Legado mergeProduct */
  pdp: 35,
});

/**
 * @param {string} sourceId
 * @returns {number}
 */
export function sourceRank(sourceId) {
  const k = String(sourceId || '').trim();
  return /** @type {Record<string, number>} */ (SOURCE_RANK)[k] ?? 0;
}

/**
 * @param {string} prevSource
 * @param {string} incomingSource
 * @returns {boolean} true se incoming deve substituir valor existente
 */
export function incomingSourceWins(prevSource, incomingSource) {
  const pr = sourceRank(prevSource);
  const ir = sourceRank(incomingSource);
  if (ir > pr) return true;
  if (ir < pr) return false;
  return true;
}
