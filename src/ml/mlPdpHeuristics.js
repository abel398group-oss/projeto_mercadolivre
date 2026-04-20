import { normalizeShippingEntry } from '../shippingExtract.js';

/**
 * Vendas a partir do subtítulo PDP (ex.: "Novo | +1000 vendidos", "+ 1.200 vendidos", "10 mil vendidos").
 * @param {string} text
 * @returns {number}
 */
export function parseSalesCountFromMlPdpText(text) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return 0;

  let m = s.match(/\+\s*([\d\.,]+)\s*vendidos/i);
  if (m) {
    const n = parseInt(String(m[1]).replace(/\./g, '').replace(/,/g, ''), 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  m = s.match(/(\d+)\s*mil\s*vendidos/i);
  if (m) {
    const k = parseInt(m[1], 10);
    return Number.isFinite(k) && k > 0 ? k * 1000 : 0;
  }

  m = s.match(/([\d\.,]+)\s*vendidos/i);
  if (m) {
    const n = parseInt(String(m[1]).replace(/\./g, '').replace(/,/g, ''), 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  return 0;
}

/**
 * Limpa título da loja quando o ML junta vários blocos (oficial + vendido por + marketplace).
 * @param {string} raw
 * @returns {string}
 */
export function cleanMlShopNameFromDom(raw) {
  let t = String(raw || '').replace(/\s+/g, ' ').trim();
  if (!t) return '';
  t = t.replace(/Tienda oficial/gi, 'Loja oficial');
  t = t.replace(/(Loja oficial)([A-Za-zÀ-ÿ])/gi, '$1 $2');
  t = t.replace(/(Mercado\s+Livre\s+Moda)(Vendido)/gi, '$1 · $2');
  t = t.replace(/(Reserva)(Vendido)/gi, '$1 · $2');
  t = t.replace(/Vendido\s+por/gi, ' · Vendido por ');
  t = t.replace(/\s*·\s*·+/g, ' · ').replace(/^\s*·\s*/, '').trim();
  return t.slice(0, 280);
}

/**
 * Constrói objeto `shipping` a partir do texto visível do bloco de envio (DOM).
 * @param {string} snippet
 * @returns {ReturnType<typeof emptyShipping> | null}
 */
export function shippingPartialFromMlDomSnippet(snippet) {
  const t = String(snippet || '').replace(/\s+/g, ' ').trim();
  if (t.length < 4) return null;

  const lower = t.toLowerCase();
  const looksFree =
    /frete\s+gr[áa]tis|chegar[áa]\s+gr[áa]tis|gr[áa]tis\s+(segunda|ter[cç]a|quinta|sexta|s[áa]bado|domingo|hoje|amanh)/i.test(
      t
    ) ||
    (lower.includes('grátis') && /chegar|entrega|frete|envio/i.test(t));

  /** @type {Record<string, unknown>} */
  const o = {
    text: t.length > 400 ? `${t.slice(0, 397)}…` : t,
    is_free: looksFree,
    price: 0,
  };

  const rm = t.match(/R\$\s*([\d\.,]+)/g);
  if (rm && rm.length) {
    const last = rm[rm.length - 1];
    const m = last.match(/R\$\s*([\d\.,]+)/);
    if (m) {
      const br = String(m[1]).replace(/\./g, '').replace(',', '.');
      const p = parseFloat(br);
      if (Number.isFinite(p) && p > 0) o.price = p;
    }
  }

  if (looksFree && o.price === 0) {
    o.price = 0;
  }

  return normalizeShippingEntry(o);
}
