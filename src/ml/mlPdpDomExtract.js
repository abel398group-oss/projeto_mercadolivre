import {
  cleanMlShopNameFromDom,
  parseSalesCountFromMlPdpText,
  shippingPartialFromMlDomSnippet,
} from './mlPdpHeuristics.js';

/**
 * Extrator DOM da PDP VPP (Mercado Livre BR).
 * Função pura para `page.evaluate(fn)` — não usar imports nem variáveis de fora.
 *
 * @returns {Record<string, unknown>}
 */
export function extractMlPdpFromDom() {
  const text = (el) => (el && el.textContent ? el.textContent.replace(/\s+/g, ' ').trim() : '');
  const allText = (sel) => {
    const nodes = document.querySelectorAll(sel);
    const out = [];
    for (let i = 0; i < nodes.length; i++) {
      const t = text(nodes[i]);
      if (t) out.push(t);
    }
    return out;
  };

  /** @type {Record<string, unknown>} */
  const out = {};

  const titleEl = document.querySelector('h1.ui-pdp-title');
  if (titleEl) out.name_dom = text(titleEl);

  /** Breadcrumb (categorias) — vários layouts VPP */
  (function collectBreadcrumb() {
    const selectors = [
      'nav[aria-label*="readcrumb" i] a',
      'nav[aria-label*="navega" i] a',
      '.andes-breadcrumb__item a',
      'ol.andes-breadcrumb a',
      '[class*="ui-vpp-breadcrumb"] a',
      '.ui-pdp-breadcrumb a',
    ];
    for (let s = 0; s < selectors.length; s++) {
      const nodes = document.querySelectorAll(selectors[s]);
      if (nodes.length < 1) continue;
      /** @type {string[]} */
      const parts = [];
      const seen = new Set();
      for (let i = 0; i < nodes.length; i++) {
        const x = text(nodes[i]);
        if (!x) continue;
        const low = x.toLowerCase();
        if (low === 'mercado livre' || low === 'mercadolivre' || low === 'início' || low === 'inicio' || low === 'home') {
          continue;
        }
        if (seen.has(low)) continue;
        seen.add(low);
        parts.push(x);
      }
      if (parts.length) {
        out.breadcrumb_labels = parts;
        return;
      }
    }
  })();

  const subEl = document.querySelector('.ui-pdp-header__subtitle .ui-pdp-subtitle, .ui-pdp-subtitle');
  if (subEl) {
    out.condition_sales_subtitle = text(subEl);
    const al = subEl.getAttribute('aria-label');
    if (al) out.condition_sales_aria = al.trim();
  }

  const ratingEl = document.querySelector(
    '.ui-pdp-review__rating, .ui-review-capability__rating__average, [data-testid="review-summary"] .ui-pdp-review__rating'
  );
  if (ratingEl) {
    const r = parseFloat(String(text(ratingEl)).replace(',', '.'));
    if (Number.isFinite(r)) out.rating_dom = r;
  }

  const reviewAmount = document.querySelector('.ui-pdp-review__amount, .ui-pdp-review__ratings');
  if (reviewAmount) {
    const m = text(reviewAmount).match(/\(?([\d\.]+)\)?/);
    if (m) {
      const n = parseInt(m[1].replace(/\./g, ''), 10);
      if (Number.isFinite(n)) out.rating_count_dom = n;
    }
  }

  const reviewLabel = document.querySelector('[data-testid="review-summary"]');
  if (reviewLabel && !out.rating_count_dom) {
    const full = text(reviewLabel);
    const m2 = full.match(/(\d[\d\.]*)\s*avalia/i);
    if (m2) out.rating_count_dom = parseInt(m2[1].replace(/\./g, ''), 10);
  }

  if (!out.rating_count_dom) {
    const cap = document.querySelector('[data-testid="reviews-desktop"], .ui-review-capability-main');
    if (cap) {
      const cm = text(cap).match(/([\d\.]+)\s*avalia/i);
      if (cm) out.rating_count_dom = parseInt(cm[1].replace(/\./g, ''), 10);
    }
  }

  /** Preço principal (fraction + cents) */
  function parseMoneyFromContainer(root) {
    if (!root) return { value: 0, label: '' };
    const frac = root.querySelector('.andes-money-amount__fraction');
    const cents = root.querySelector('.andes-money-amount__cents');
    if (!frac) return { value: 0, label: '' };
    const whole = parseInt(String(text(frac)).replace(/\./g, ''), 10) || 0;
    const c = cents ? parseInt(text(cents), 10) || 0 : 0;
    const val = whole + c / 100;
    const labEl = root.querySelector('[aria-label]');
    const label = labEl ? String(labEl.getAttribute('aria-label') || '') : '';
    return { value: val, label: label };
  }

  const priceMain = document.querySelector('.ui-pdp-price__second-line, .ui-pdp-price .ui-pdp-price__main-container');
  if (priceMain) {
    const containers = priceMain.querySelectorAll('.andes-money-amount');
    let best = 0;
    for (let i = 0; i < containers.length; i++) {
      const c = containers[i];
      if (c.closest('s')) continue;
      if (c.classList.contains('andes-money-amount--previous')) continue;
      const { value } = parseMoneyFromContainer(c.parentElement || c);
      if (value > best) best = value;
    }
    if (best > 0) out.price_current_dom = best;
    else {
      const { value } = parseMoneyFromContainer(priceMain);
      if (value > 0) out.price_current_dom = value;
    }
  }

  const prevS = document.querySelector('s.andes-money-amount--previous, .ui-pdp-price s.andes-money-amount');
  if (prevS) {
    const { value, label } = parseMoneyFromContainer(prevS.closest('.andes-money-amount') || prevS.parentElement);
    if (value > 0) out.price_original_dom = value;
    if (label && /antes/i.test(label)) out.price_original_aria_label = label;
  }

  const discEl = document.querySelector('.ui-pdp-price__discount, [class*="ui-pdp-price__discount"]');
  if (discEl) {
    const d = text(discEl);
    const m = d.match(/(\d+)\s*%/);
    if (m) out.discount_percent_dom = parseInt(m[1], 10);
  }
  if (!out.discount_percent_dom && out.price_original_dom && out.price_current_dom) {
    const o = Number(out.price_original_dom);
    const c = Number(out.price_current_dom);
    if (o > c && c > 0) out.discount_percent_dom = Math.round(100 * (1 - c / o));
  }

  const instEl = document.querySelector('.ui-pdp-price .ui-pdp-price__subtitles, .ui-pdp-price__subtitles');
  if (instEl) {
    const ist = text(instEl);
    if (/\d+\s*x/i.test(ist)) out.installments_text_dom = ist;
  }

  const unitEl = document.querySelector('[class*="price_per"], .ui-pdp-price .ui-pdp-price__subtitles ~ *');
  if (unitEl) {
    const ut = text(unitEl);
    if (/pre[cç]o\s+por|\/\s*kg|\/\s*litro/i.test(ut)) out.unit_price_text_dom = ut;
  }

  (function collectShippingSnippet() {
    const roots = [
      '.ui-pdp-container__shipping',
      '.ui-pdp-media .ui-pdp-shipping',
      '.ui-pdp-shipping__text',
      '[class*="ui-pdp-shipping"]',
      '[data-testid*="shipping" i]',
      '.ui-pdp-delivery',
    ];
    for (let r = 0; r < roots.length; r++) {
      const shipEl = document.querySelector(roots[r]);
      if (!shipEl) continue;
      const wrap = shipEl.closest('.ui-pdp-media') || shipEl.closest('[class*="shipping"]') || shipEl;
      const st = text(wrap);
      if (st.length > 3 && st.length < 1200) {
        out.shipping_summary_dom = st;
        return;
      }
    }
  })();

  const desc = document.querySelector('.ui-pdp-description__content');
  if (desc) out.description_dom = text(desc);

  const bullets = allText('.ui-pdp-highlights .ui-pdp-highlight, .ui-pdp-highlights li, [class*="ui-pdp-highlights"] li');
  if (bullets.length) out.product_highlights = bullets;

  const charRows = document.querySelectorAll('.andes-table__row, tr.andes-table__row');
  /** @type {Record<string, string>} */
  const attrs = {};
  for (let i = 0; i < charRows.length; i++) {
    const row = charRows[i];
    const th = row.querySelector('.andes-table__header, th');
    const td = row.querySelector('.andes-table__column, td');
    if (th && td) {
      const k = text(th);
      const v = text(td);
      if (k && v) attrs[k] = v;
    }
  }
  if (Object.keys(attrs).length) out.attributes_table = attrs;

  const sellerTitle =
    document.querySelector('.ui-pdp-seller__header__title--official') ||
    document.querySelector('.ui-pdp-seller__header__title') ||
    document.querySelector('.ui-pdp-seller__link');
  if (sellerTitle) out.shop_name_dom = text(sellerTitle);

  const sellerRep = document.querySelector('.ui-pdp-seller__header__subtitle, .ui-pdp-seller__status');
  if (sellerRep) {
    const rt = text(sellerRep);
    if (rt) out.seller_reputation_text_dom = rt;
  }

  const sellerCard = document.querySelector('.ui-pdp-seller');
  if (sellerCard) {
    const links = sellerCard.querySelectorAll(
      'a[href*="/perfil/"], a[href*="loja"], a[href*="tienda"], a[href*="noindex/catalog"], a[href*="mercadolivre.com"][href*="user"]'
    );
    for (let i = 0; i < links.length; i++) {
      const href = links[i].getAttribute('href') || '';
      if (!href || href === '#' || href.startsWith('javascript:')) continue;
      if (/login|registration|jms\/mlb\/lgz/i.test(href)) continue;
      out.shop_link_dom = href.startsWith('http') ? href : `https://www.mercadolivre.com.br${href}`;
      break;
    }
    const um = sellerCard.innerHTML.match(/(?:seller[_-]?id|user[_-]?id)["']?\s*[:=]\s*["']?(\d{5,})/i);
    if (um) out.seller_id_dom = um[1];
  }

  const qtyBtn = document.querySelector('#quantity-selector, .ui-pdp-buybox__quantity__trigger, [class*="ui-pdp-buybox__quantity"] button');
  if (qtyBtn) {
    const qal = qtyBtn.getAttribute('aria-label');
    if (qal) out.stock_quantity_aria = qal.trim();
    const qtxt = text(qtyBtn);
    if (qtxt) out.stock_quantity_text = qtxt;
  }
  const buybox = document.querySelector('.ui-pdp-buybox');
  if (buybox) {
    const fullBb = text(buybox);
    const stockM = fullBb.match(/(\+?\d+)\s*dispon[ií]veis/i);
    if (stockM) out.stock_available_phrase = stockM[0];
    if (/estoque dispon/i.test(fullBb)) out.stock_status_dom = 'available';
    if (/sem estoque|indispon/i.test(fullBb)) out.stock_status_dom = 'unavailable';
  }

  const reviewRoot = document.querySelector('[data-testid="reviews-desktop"], .ui-review-capability-main');
  if (reviewRoot) {
    const summaryAi = reviewRoot.querySelector(
      '[class*="summary"], [class*="Summary"], .ui-review-capability__summary, [data-testid*="summary"]'
    );
    if (summaryAi) {
      const st = text(summaryAi);
      if (st.length > 40) out.review_ai_summary_dom = st;
    }
    const histBars = reviewRoot.querySelectorAll('[class*="histogram"], [class*="level"], [role="meter"]');
    if (histBars.length >= 3) {
      /** @type {Record<string, number>} */
      const dist = {};
      for (let i = 0; i < histBars.length; i++) {
        const b = histBars[i];
        const lab = text(b) || String(b.getAttribute('aria-label') || '');
        const starM = lab.match(/([1-5])\s*(?:estrelas|\u2605|stars)/i);
        const pctM = lab.match(/(\d+)\s*%/);
        if (starM && pctM) dist[`${starM[1]}_star_pct`] = parseInt(pctM[1], 10);
      }
      if (Object.keys(dist).length) out.rating_histogram_dom = dist;
    }
  }

  const galImgs = document.querySelectorAll(
    '.ui-pdp-gallery img[src*="mlstatic"], .ui-pdp-gallery__figure img, figure.ui-pdp-gallery__figure img'
  );
  if (galImgs.length) {
    const urls = [];
    const seenU = new Set();
    for (let i = 0; i < galImgs.length; i++) {
      const src = galImgs[i].getAttribute('src') || '';
      if (src.includes('mlstatic') && !seenU.has(src)) {
        seenU.add(src);
        urls.push(src);
      }
    }
    if (urls.length) out.gallery_image_urls = urls;
  }

  const relCards = document.querySelectorAll('.ui-recommendations-carousel-wrapper-ref a[href*="/p/"], .ui-recommendations-wrapper a[href*="/p/"]');
  if (relCards.length) {
    /** @type {{ title: string; url: string }[]} */
    const rel = [];
    const seen = new Set();
    for (let i = 0; i < Math.min(relCards.length, 24); i++) {
      const a = relCards[i];
      const href = a.getAttribute('href') || '';
      if (!href || seen.has(href)) continue;
      seen.add(href);
      rel.push({ title: text(a).slice(0, 200), url: href.startsWith('http') ? href : `https://www.mercadolivre.com.br${href}` });
    }
    if (rel.length) out.related_products_sample = rel;
  }

  try {
    const w = /** @type {unknown} */ (window);
    const pre = /** @type {Record<string, unknown> | undefined} */ (
      /** @type {{ __PRELOADED_STATE__?: unknown }} */ (w).__PRELOADED_STATE__
    );
    if (pre && typeof pre === 'object') {
      out.has_window_preloaded_state = true;
    }
  } catch {
    /* ignore */
  }

  return out;
}

/**
 * Normaliza o payload do DOM para merge com productSchema (campos canónicos + extras).
 * @param {Record<string, unknown>} dom
 * @param {string} pageUrl
 */
export function normalizeMlPdpDomToPartial(dom, pageUrl) {
  if (!dom || typeof dom !== 'object') return {};

  /** @type {Record<string, unknown>} */
  const partial = {};

  const name = String(dom.name_dom || '').trim();
  if (name) partial.name = name;

  const pc = Number(dom.price_current_dom);
  if (Number.isFinite(pc) && pc > 0) partial.price_current = pc;

  const po = Number(dom.price_original_dom);
  if (Number.isFinite(po) && po > 0) partial.price_original = po;

  const disc = Number(dom.discount_percent_dom);
  if (Number.isFinite(disc) && disc > 0) partial.discount = disc;

  const rating = Number(dom.rating_dom);
  if (Number.isFinite(rating) && rating > 0) partial.rating = rating;

  const rc = dom.rating_count_dom;
  if (rc != null && rc !== '') partial.rating_count = Number(rc);

  const desc = String(dom.description_dom || '').trim();
  if (desc) partial.description = desc;

  const shop = String(dom.shop_name_dom || '').trim();
  if (shop) partial.shop_name = cleanMlShopNameFromDom(shop);

  const sl = String(dom.shop_link_dom || '').trim();
  if (sl) partial.shop_link = sl;

  const sid = String(dom.seller_id_dom || '').trim();
  if (sid) partial.seller_id = sid;

  if (Array.isArray(dom.breadcrumb_labels) && dom.breadcrumb_labels.length) {
    partial.categories = [.../** @type {string[]} */ (dom.breadcrumb_labels)];
    partial.taxonomy_path = /** @type {string[]} */ (dom.breadcrumb_labels).join(' > ');
    const last = /** @type {string[]} */ (dom.breadcrumb_labels);
    partial.product_category_from_breadcrumb = String(last[last.length - 1] || '').trim();
  }

  if (Array.isArray(dom.product_highlights) && dom.product_highlights.length) {
    partial.pdp_highlights = dom.product_highlights;
  }
  if (dom.attributes_table && typeof dom.attributes_table === 'object') {
    partial.pdp_attributes_table = dom.attributes_table;
  }

  const subs = String(dom.condition_sales_subtitle || '').trim();
  if (subs) partial.pdp_subtitle = subs;

  const subsAria = String(dom.condition_sales_aria || '').trim();
  const soldGuess = parseSalesCountFromMlPdpText(`${subs} ${subsAria}`);
  if (soldGuess > 0) {
    partial.sales_count = soldGuess;
    partial.sales_count_precision = 'approximate';
  }

  const inst = String(dom.installments_text_dom || '').trim();
  if (inst) partial.pdp_installments = inst;

  const ship = String(dom.shipping_summary_dom || '').trim();
  if (ship) partial.pdp_shipping_snippet = ship;

  const shipMerged = shippingPartialFromMlDomSnippet(ship);
  if (shipMerged && String(shipMerged.text || '').trim() && shipMerged.text !== 'unknown') {
    partial.shipping = shipMerged;
  }

  const unit = String(dom.unit_price_text_dom || '').trim();
  if (unit) partial.pdp_unit_price_label = unit;

  const saria = String(dom.stock_quantity_aria || dom.stock_available_phrase || '').trim();
  if (saria) partial.stock_hint = saria;

  const sst = String(dom.stock_status_dom || '').trim();
  if (sst) partial.stock_status = sst;

  const srep = String(dom.seller_reputation_text_dom || '').trim();
  if (srep) partial.seller_reputation_snippet = srep;

  const rai = String(dom.review_ai_summary_dom || '').trim();
  if (rai) partial.review_summary_ai = rai;

  if (dom.rating_histogram_dom && typeof dom.rating_histogram_dom === 'object') {
    partial.rating_distribution = dom.rating_histogram_dom;
  }

  if (Array.isArray(dom.related_products_sample) && dom.related_products_sample.length) {
    partial.pdp_related_products = dom.related_products_sample;
  }

  if (Array.isArray(dom.gallery_image_urls) && dom.gallery_image_urls.length) {
    partial.images = dom.gallery_image_urls;
    if (!partial.image_main) partial.image_main = String(dom.gallery_image_urls[0]);
  }

  if (dom.has_window_preloaded_state === true) {
    partial.pdp_has_client_state = true;
  }

  partial.url = pageUrl;
  partial.url_primary = pageUrl;

  return partial;
}
