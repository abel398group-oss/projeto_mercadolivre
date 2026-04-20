/**
 * URL canónica no catálogo (listagem): /p/{product_id}, sem slug.
 * Campo derivado — não reflete extração HTML.
 */

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {string} provenance mergeProduct `provenance`
 * @returns {boolean}
 */
export function isCatalogListingMergeProvenance(provenance) {
  const p = str(provenance);
  if (!p) return false;
  return (
    p === 'listing_html' ||
    p === 'listing_network' ||
    p === 'ml_search_api' ||
    p === 'category_ssr' ||
    p.includes('listing')
  );
}

/**
 * @param {Record<string, unknown>} product
 * @returns {string | null}
 */
export function buildCanonicalUrlFromListing(product) {
  const id = str(product?.product_id).toUpperCase();
  if (id.startsWith('MLB')) {
    return `https://www.mercadolivre.com.br/p/${id}`;
  }
  return null;
}

/**
 * @param {Record<string, unknown>} product registo canónico (mutado)
 */
export function applyListingCanonicalUrlsToRecord(product) {
  const canonicalUrl = buildCanonicalUrlFromListing(product);
  if (!canonicalUrl) return;
  product.url = canonicalUrl;
  product.url_primary = canonicalUrl;
  if (!product._field_sources || typeof product._field_sources !== 'object') product._field_sources = {};
  /** @type {Record<string, string>} */ (product._field_sources).url = 'derived';
  /** @type {Record<string, string>} */ (product._field_sources).url_primary = 'derived';
}
