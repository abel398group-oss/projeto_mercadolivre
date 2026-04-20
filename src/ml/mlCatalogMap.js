import { normalizeImageList } from './fieldValidators.js';

/**
 * Normaliza um item devolvido pela busca pública (sites/MLB/search).
 * @param {Record<string, unknown>} item
 * @param {string} categoryId
 * @returns {Record<string, unknown>}
 */
export function mapSearchItemToRecord(item, categoryId) {
  const rawId = item.id;
  let product_id = '';
  if (rawId != null) {
    const s = String(rawId).trim();
    if (/^MLB\d+/i.test(s)) {
      product_id = `MLB${s.replace(/^MLB/i, '').replace(/\D/g, '')}`;
    } else {
      const d = s.replace(/\D/g, '');
      product_id = d ? `MLB${d}` : '';
    }
  }

  let price = 0;
  if (typeof item.price === 'number') {
    price = item.price;
  } else if (item.price && typeof item.price === 'object') {
    const pa = /** @type {{ amount?: number }} */ (item.price).amount;
    if (typeof pa === 'number') price = pa;
  } else if (item.price != null && item.price !== '') {
    price = Number(item.price);
  } else if (item.installments && typeof item.installments === 'object') {
    const ia = /** @type {{ amount?: number }} */ (item.installments).amount;
    if (typeof ia === 'number') price = ia;
  }

  const seller = item.seller && typeof item.seller === 'object' ? /** @type {Record<string, unknown>} */ (item.seller) : null;
  const sellerId = seller?.id != null ? String(seller.id) : '';

  const soldRaw = item.sold_quantity;
  let sold = NaN;
  if (soldRaw != null && soldRaw !== '') {
    const n = Number(soldRaw);
    if (Number.isFinite(n)) sold = n;
  }

  const thumbRaw = item.thumbnail != null ? String(item.thumbnail) : '';
  const imgs = normalizeImageList(thumbRaw ? [thumbRaw] : []);
  const main = imgs[0] || '';

  return {
    product_id,
    listing_product_id: product_id,
    name: String(item.title || '').trim(),
    price_current: Number.isFinite(price) ? price : 0,
    price_currency: String(item.currency_id || 'BRL'),
    price_original: null,
    discount: 0,
    seller_id: sellerId,
    shop_name: '',
    sales_count: Number.isFinite(sold) ? sold : null,
    rating: 0,
    rating_count: null,
    url: String(item.permalink || '').trim(),
    url_primary: String(item.permalink || '').trim(),
    image_main: main,
    images: imgs,
    condition: item.condition != null ? String(item.condition) : '',
    listing_type_id: item.listing_type_id != null ? String(item.listing_type_id) : '',
    category_source_id: categoryId,
    source: 'ml_search_api',
    collected_at: new Date().toISOString(),
  };
}
