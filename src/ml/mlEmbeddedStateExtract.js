/**
 * Extração de estado embutido no HTML da PDP (scripts inline / hydration).
 * Funções puras; sem eval/Function/vm. Erros não fatais e blobs auditáveis.
 */

/** @typedef {'preloaded_state' | 'initial_state' | 'app_state' | 'next_data' | 'inline_json' | 'unknown'} EmbeddedBlobType */

/**
 * @typedef {{
 *   type: EmbeddedBlobType,
 *   subtype: string,
 *   raw_excerpt: string,
 *   parsed: object | null,
 *   parse_error: string | null,
 *   extraction_method: string,
 *   confidence: 'high' | 'medium' | 'low'
 * }} EmbeddedBlob
 */

import { emptyShipping, shippingHasData } from '../shippingExtract.js';

const MAX_HTML_SCAN = 4_500_000;
const MAX_INLINE_JSON_CHUNK = 1_200_000;
const MIN_JSON_PAYLOAD_LEN = 24;
const MAX_JSON_STRING_FOR_PARSE = 2_500_000;
const MAX_BASE64_CHUNK = 800_000;
const MIN_BASE64_RUN = 120;
const EXCERPT = 220;
const WALK_MAX_DEPTH = 16;
const WALK_MAX_NODES = 14_000;

/** Indicadores ML / PDP para reduzir falso positivo. */
const ML_HINT_RE =
  /MLB\d|MLBU|mercadoliv|mercadolibre|mercadolibre\.com|sold_quantity|permalink|seller_id|category_id|domain_id|free_shipping|"price"\s*:|shipping_labels|review_count|available_quantity|plain_text|ui-pdp/i;

/**
 * @param {unknown} o
 */
function payloadLooksMlRelevant(o) {
  if (!o || typeof o !== 'object') return false;
  let s;
  try {
    s = JSON.stringify(o);
  } catch {
    return false;
  }
  if (s.length < MIN_JSON_PAYLOAD_LEN) return false;
  return ML_HINT_RE.test(s.slice(0, 80_000));
}

/**
 * decodeURIComponent seguro (sem lançar).
 * @param {string} s
 */
export function safeDecodeURIComponent(s) {
  try {
    return decodeURIComponent(s);
  } catch {
    return null;
  }
}

/**
 * Literal de string JS (" ou ') a partir de start no índice da aspa de abertura.
 * @param {string} full
 * @param {number} start
 * @returns {{ value: string, end: number } | null}
 */
export function parseJsStringLiteral(full, start) {
  const q = full[start];
  if (q !== '"' && q !== "'") return null;
  let i = start + 1;
  let buf = '';
  while (i < full.length) {
    const c = full[i];
    if (c === '\\' && i + 1 < full.length) {
      const n = full[i + 1];
      if (n === 'n') {
        buf += '\n';
        i += 2;
        continue;
      }
      if (n === 'r') {
        buf += '\r';
        i += 2;
        continue;
      }
      if (n === 't') {
        buf += '\t';
        i += 2;
        continue;
      }
      if (n === 'u' && i + 5 < full.length) {
        const hex = full.slice(i + 2, i + 6);
        if (/^[0-9a-fA-F]{4}$/.test(hex)) {
          buf += String.fromCharCode(parseInt(hex, 16));
          i += 6;
          continue;
        }
      }
      if (n === q || n === '\\' || n === '"' || n === "'") {
        buf += n;
        i += 2;
        continue;
      }
      if (n === '\r' && full[i + 2] === '\n') {
        i += 3;
        continue;
      }
      if (n === '\n' || n === '\r') {
        i += 2;
        continue;
      }
      buf += n;
      i += 2;
      continue;
    }
    if (c === q) {
      return { value: buf, end: i + 1 };
    }
    buf += c;
    i++;
  }
  return null;
}

/**
 * @param {string} raw
 */
function tryJsonParse(raw) {
  if (raw.length > MAX_JSON_STRING_FOR_PARSE) {
    return /** @type {{ ok: false, error: string }} */ ({ ok: false, error: 'json_too_large' });
  }
  try {
    return /** @type {{ ok: true, value: unknown }} */ ({ ok: true, value: JSON.parse(raw) });
  } catch (e) {
    return /** @type {{ ok: false, error: string }} */ ({
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    });
  }
}

/**
 * A partir de "JSON.parse" em `html[pos]`, extrai e parseia o argumento.
 * @param {string} html
 * @param {number} pos Índice do 'J' de JSON.parse
 */
export function tryConsumeJsonParseCall(html, pos) {
  if (!html.slice(pos, pos + 10).match(/^JSON\.parse/i)) return null;
  let j = pos + 'JSON.parse'.length;
  while (j < html.length && /\s/.test(html[j])) j++;
  if (html[j] !== '(') return null;
  j++;
  while (j < html.length && /\s/.test(html[j])) j++;

  let usedDecode = false;
  /** Case-insensitive: `toLowerCase()` altera URI → uri e quebraria `startsWith('decodeURIComponent')`. */
  const decodeUriMatch = html.slice(j).match(/^decodeURIComponent\s*\(\s*/i);
  if (decodeUriMatch) {
    usedDecode = true;
    j += decodeUriMatch[0].length;
  }

  const lit = parseJsStringLiteral(html, j);
  if (!lit) return null;
  let payload = lit.value;
  let after = lit.end;
  while (after < html.length && /\s/.test(html[after])) after++;
  if (usedDecode) {
    if (html[after] !== ')') return null;
    after++;
    while (after < html.length && /\s/.test(html[after])) after++;
  }
  if (html[after] !== ')') return null;
  after++;

  if (usedDecode) {
    const dec = safeDecodeURIComponent(payload);
    if (dec == null) {
      return {
        parsed: null,
        parse_error: 'decodeURIComponent_failed',
        end: after,
        usedDecode,
        excerpt: html.slice(pos, Math.min(html.length, pos + EXCERPT)),
      };
    }
    payload = dec;
  }

  const parsed = tryJsonParse(payload.trim());
  return {
    parsed: parsed.ok && parsed.value && typeof parsed.value === 'object' ? /** @type {object} */ (parsed.value) : null,
    parse_error: parsed.ok ? (typeof parsed.value === 'object' ? null : 'not_object') : parsed.error,
    end: after,
    usedDecode,
    excerpt: html.slice(pos, Math.min(html.length, pos + EXCERPT)),
  };
}

/**
 * @param {string} s
 * @param {number} start
 */
function balancedJsonSlice(s, start) {
  if (start < 0 || start >= s.length) return null;
  const open = s[start];
  if (open !== '{' && open !== '[') return null;
  const close = open === '{' ? '}' : ']';
  let depth = 0;
  let inStr = false;
  /** @type {string} */
  let q = '';
  let esc = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (inStr) {
      if (esc) {
        esc = false;
        continue;
      }
      if (c === '\\') {
        esc = true;
        continue;
      }
      if (c === q) inStr = false;
      continue;
    }
    if (c === '"' || c === "'") {
      inStr = true;
      q = c;
      continue;
    }
    if (c === open) depth++;
    else if (c === close) {
      depth--;
      if (depth === 0) return s.slice(start, i + 1);
    }
  }
  return null;
}

/**
 * @param {string} body
 * @returns {EmbeddedBlob[]}
 */
function extractAliasesAndGlobalsInScript(body) {
  /** @type {EmbeddedBlob[]} */
  const out = [];
  /** @type {Map<string, object>} */
  const aliases = new Map();

  const declRe = /\b(?:const|let|var)\s+([a-zA-Z_$][\w$]*)\s*=\s*/g;
  let m;
  while ((m = declRe.exec(body)) !== null) {
    const name = m[1];
    let pos = m.index + m[0].length;
    while (pos < body.length && /\s/.test(body[pos])) pos++;

    if (body.slice(pos, pos + 10).match(/^JSON\.parse/i)) {
      const r = tryConsumeJsonParseCall(body, pos);
      if (r && r.parsed && payloadLooksMlRelevant(r.parsed)) {
        aliases.set(name, r.parsed);
        const subtype = r.usedDecode ? 'embedded_json_parse_uri' : 'embedded_json_parse';
        const method = r.usedDecode ? 'json_parse_decode_uri' : 'json_parse_string';
        out.push({
          type: 'unknown',
          subtype,
          raw_excerpt: r.excerpt.slice(0, EXCERPT),
          parsed: r.parsed,
          parse_error: null,
          extraction_method: method,
          confidence: 'high',
        });
      }
    } else if (body[pos] === '{' || body[pos] === '[') {
      const slice = balancedJsonSlice(body, pos);
      if (slice && slice.length >= MIN_JSON_PAYLOAD_LEN) {
        const p = tryJsonParse(slice);
        if (p.ok && p.value && typeof p.value === 'object' && payloadLooksMlRelevant(p.value)) {
          aliases.set(name, /** @type {object} */ (p.value));
          out.push({
            type: 'unknown',
            subtype: 'embedded_alias_assignment',
            raw_excerpt: slice.slice(0, EXCERPT),
            parsed: /** @type {object} */ (p.value),
            parse_error: null,
            extraction_method: 'local_var_object_literal',
            confidence: 'medium',
          });
        }
      }
    }
  }

  const globalAssign =
    /(?:window\.)?(__PRELOADED_STATE__|__INITIAL_STATE__|__APP_STATE__)\s*=\s*([a-zA-Z_$][\w$]*)\s*;?/g;
  let m2;
  while ((m2 = globalAssign.exec(body)) !== null) {
    const g = m2[1];
    const alias = m2[2];
    const obj = aliases.get(alias);
    if (!obj) continue;
    /** @type {EmbeddedBlobType} */
    let type = 'unknown';
    if (g === '__PRELOADED_STATE__') type = 'preloaded_state';
    else if (g === '__INITIAL_STATE__') type = 'initial_state';
    else if (g === '__APP_STATE__') type = 'app_state';
    out.push({
      type,
      subtype: 'embedded_alias_assignment',
      raw_excerpt: body.slice(Math.max(0, m2.index - 10), Math.min(body.length, m2.index + EXCERPT)),
      parsed: obj,
      parse_error: null,
      extraction_method: 'alias_to_global_window',
      confidence: 'medium',
    });
  }

  return out;
}

/**
 * @param {string} scriptBody
 * @param {EmbeddedBlobType} type
 */
function extractJsonParseWindowAssignments(scriptBody, type) {
  /** @type {EmbeddedBlob[]} */
  const out = [];
  const escaped =
    type === 'preloaded_state'
      ? '__PRELOADED_STATE__'
      : type === 'initial_state'
        ? '__INITIAL_STATE__'
        : '__APP_STATE__';
  const re = new RegExp(`(?:window\\.)?${escaped.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*=\\s*`, 'gi');
  let m;
  while ((m = re.exec(scriptBody)) !== null) {
    let j = m.index + m[0].length;
    while (j < scriptBody.length && /\s/.test(scriptBody[j])) j++;
    if (scriptBody[j] === '(') {
      j++;
      while (j < scriptBody.length && /\s/.test(scriptBody[j])) j++;
    }
    if (!scriptBody.slice(j, j + 10).match(/^JSON\.parse/i)) continue;
    const r = tryConsumeJsonParseCall(scriptBody, j);
    if (!r) continue;
    const subtype = r.usedDecode ? 'embedded_json_parse_uri' : 'embedded_json_parse';
    const method = r.usedDecode ? 'json_parse_decode_uri' : 'json_parse_string';
    if (r.parsed && payloadLooksMlRelevant(r.parsed)) {
      out.push({
        type,
        subtype,
        raw_excerpt: r.excerpt.slice(0, EXCERPT),
        parsed: r.parsed,
        parse_error: null,
        extraction_method: method,
        confidence: 'high',
      });
    } else if (r.parse_error) {
      out.push({
        type,
        subtype,
        raw_excerpt: r.excerpt.slice(0, EXCERPT),
        parsed: null,
        parse_error: r.parse_error,
        extraction_method: method,
        confidence: 'low',
      });
    }
  }
  return out;
}

/**
 * @param {string} html
 * @param {EmbeddedBlobType} type
 * @param {string} varName
 */
function extractWindowAssignmentDirectJson(html, type, varName) {
  const escaped = varName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const assignRe = new RegExp(`(?:window\\.)?${escaped}\\s*=\\s*`, 'gi');
  /** @type {EmbeddedBlob[]} */
  const out = [];
  let m;
  while ((m = assignRe.exec(html)) !== null) {
    let j = m.index + m[0].length;
    while (j < html.length && /\s/.test(html[j])) j++;
    if (html[j] === '(') {
      j++;
      while (j < html.length && /\s/.test(html[j])) j++;
    }
    if (html.slice(j, j + 10).match(/^JSON\.parse/i)) continue;

    const slice = balancedJsonSlice(html, j);
    if (!slice) {
      out.push({
        type,
        subtype:
          type === 'preloaded_state'
            ? 'embedded_preloaded_state'
            : type === 'initial_state'
              ? 'embedded_initial_state'
              : 'embedded_app_state',
        raw_excerpt: html.slice(Math.max(0, j - 20), Math.min(html.length, j + EXCERPT)),
        parsed: null,
        parse_error: 'balanced_json_incomplete',
        extraction_method: 'direct_balanced_object',
        confidence: 'low',
      });
      continue;
    }
    const parsed = tryJsonParse(slice);
    if (!parsed.ok || !parsed.value || typeof parsed.value !== 'object') {
      out.push({
        type,
        subtype: 'embedded_inline_json',
        raw_excerpt: slice.slice(0, EXCERPT),
        parsed: null,
        parse_error: parsed.ok ? 'not_object' : parsed.error,
        extraction_method: 'direct_balanced_object',
        confidence: 'low',
      });
      continue;
    }
    if (!payloadLooksMlRelevant(parsed.value)) continue;

    /** @type {string} */
    let subtype = 'embedded_preloaded_state';
    if (type === 'initial_state') subtype = 'embedded_initial_state';
    else if (type === 'app_state') subtype = 'embedded_app_state';
    out.push({
      type,
      subtype,
      raw_excerpt: slice.slice(0, EXCERPT),
      parsed: /** @type {object} */ (parsed.value),
      parse_error: null,
      extraction_method: 'direct_balanced_object',
      confidence: 'high',
    });
  }
  return out;
}

/**
 * Tenta decodificar Base64 conservadoramente e parsear JSON.
 * @param {string} b64
 */
function tryBase64ToJsonObject(b64) {
  const t = b64.replace(/\s+/g, '').trim();
  if (t.length < MIN_BASE64_RUN || t.length > MAX_BASE64_CHUNK) return null;
  if (!/^[A-Za-z0-9+/]+=*$/.test(t)) return null;
  let buf;
  try {
    buf = Buffer.from(t, 'base64');
  } catch {
    return null;
  }
  if (buf.length < 20 || buf.length > 750_000) return null;
  let text;
  try {
    text = buf.toString('utf8');
  } catch {
    return null;
  }
  const t2 = text.trim();
  if (!t2.startsWith('{') && !t2.startsWith('[')) return null;
  const parsed = tryJsonParse(t2);
  if (!parsed.ok || !parsed.value || typeof parsed.value !== 'object') return null;
  if (!payloadLooksMlRelevant(parsed.value)) return null;
  return /** @type {object} */ (parsed.value);
}

/**
 * Procura runs Base64 num script; muito conservador.
 * @param {string} scriptBody
 */
function extractBase64JsonBlobs(scriptBody) {
  /** @type {EmbeddedBlob[]} */
  const out = [];
  const re = /["']([A-Za-z0-9+/]{120,}={0,2})["']/g;
  let m;
  while ((m = re.exec(scriptBody)) !== null) {
    const obj = tryBase64ToJsonObject(m[1]);
    if (obj) {
      out.push({
        type: 'inline_json',
        subtype: 'embedded_base64_json',
        raw_excerpt: m[1].slice(0, EXCERPT),
        parsed: obj,
        parse_error: null,
        extraction_method: 'base64_quoted_chunk',
        confidence: 'medium',
      });
    }
    if (out.length >= 3) break;
  }
  return out;
}

/**
 * @param {string} html
 */
function extractNextDataBlobs(html) {
  /** @type {EmbeddedBlob[]} */
  const out = [];
  const re = /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const inner = (m[1] || '').trim();
    if (!inner) continue;
    const parsed = tryJsonParse(inner);
    if (parsed.ok && parsed.value && typeof parsed.value === 'object' && payloadLooksMlRelevant(parsed.value)) {
      out.push({
        type: 'next_data',
        subtype: 'embedded_next_data',
        raw_excerpt: inner.slice(0, EXCERPT),
        parsed: /** @type {object} */ (parsed.value),
        parse_error: null,
        extraction_method: 'next_data_script_tag',
        confidence: 'high',
      });
    } else {
      /** @type {string} */
      let err = parsed.ok ? 'not_object' : parsed.error;
      if (parsed.ok && parsed.value && typeof parsed.value === 'object' && !payloadLooksMlRelevant(parsed.value)) {
        err = 'not_ml_relevant';
      }
      out.push({
        type: 'next_data',
        subtype: 'embedded_next_data',
        raw_excerpt: inner.slice(0, EXCERPT),
        parsed: null,
        parse_error: err,
        extraction_method: 'next_data_script_tag',
        confidence: 'low',
      });
    }
  }
  return out;
}

/**
 * @param {string} html
 */
function extractInlineJsonHeuristic(html) {
  /** @type {EmbeddedBlob[]} */
  const out = [];
  const scriptRe = /<script(?![^>]*type=["']application\/ld\+json["'])[^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  let scripts = 0;
  while ((m = scriptRe.exec(html)) !== null) {
    if (++scripts > 200) break;
    const body = m[1] || '';
    if (body.length < 80 || body.length > MAX_INLINE_JSON_CHUNK) continue;
    if (!ML_HINT_RE.test(body)) continue;
    const idx = body.indexOf('{');
    if (idx < 0) continue;
    const slice = balancedJsonSlice(body, idx);
    if (!slice || slice.length < 100) continue;
    const parsed = tryJsonParse(slice);
    if (parsed.ok && parsed.value && typeof parsed.value === 'object' && payloadLooksMlRelevant(parsed.value)) {
      out.push({
        type: 'inline_json',
        subtype: 'embedded_inline_json',
        raw_excerpt: slice.slice(0, EXCERPT),
        parsed: /** @type {object} */ (parsed.value),
        parse_error: null,
        extraction_method: 'inline_balanced_object_heuristic',
        confidence: 'medium',
      });
    }
  }
  return out.slice(0, 8);
}

/**
 * Enumera scripts inline e enriquece extrações.
 * @param {string} html
 */
function extractAllScriptBodies(html) {
  const scriptRe = /<script(?![^>]*type=["']application\/ld\+json["'])[^>]*>([\s\S]*?)<\/script>/gi;
  /** @type {string[]} */
  const bodies = [];
  let m;
  while ((m = scriptRe.exec(html)) !== null) {
    bodies.push(m[1] || '');
  }
  return bodies;
}

/**
 * Localiza e desserializa blobs de estado embutidos no HTML.
 * @param {string} html
 */
export function extractEmbeddedStateFromHtml(html) {
  const htmlStr = String(html || '').slice(0, MAX_HTML_SCAN);
  /** @type {EmbeddedBlob[]} */
  const blobs = [];

  const bodies = extractAllScriptBodies(htmlStr);
  for (const body of bodies) {
    blobs.push(...extractAliasesAndGlobalsInScript(body));
    blobs.push(...extractJsonParseWindowAssignments(body, 'preloaded_state'));
    blobs.push(...extractJsonParseWindowAssignments(body, 'initial_state'));
    blobs.push(...extractJsonParseWindowAssignments(body, 'app_state'));
    blobs.push(...extractBase64JsonBlobs(body));
  }

  for (const b of extractWindowAssignmentDirectJson(htmlStr, 'preloaded_state', '__PRELOADED_STATE__')) blobs.push(b);
  for (const b of extractWindowAssignmentDirectJson(htmlStr, 'initial_state', '__INITIAL_STATE__')) blobs.push(b);
  for (const b of extractWindowAssignmentDirectJson(htmlStr, 'app_state', '__APP_STATE__')) blobs.push(b);
  for (const b of extractNextDataBlobs(htmlStr)) blobs.push(b);
  for (const b of extractInlineJsonHeuristic(htmlStr)) blobs.push(b);

  /** @type {Map<string, true>} */
  const seen = new Map();
  /** @type {EmbeddedBlob[]} */
  const deduped = [];
  for (const b of blobs) {
    if (!b.parsed) {
      deduped.push(b);
      continue;
    }
    let key;
    try {
      key = `${b.type}:${b.subtype}:${JSON.stringify(b.parsed).slice(0, 500)}`;
    } catch {
      key = `${b.type}:${b.subtype}:${String(Math.random())}`;
    }
    if (seen.has(key)) continue;
    seen.set(key, true);
    deduped.push(b);
  }

  const found = deduped.some((x) => x.parsed != null);
  return { found, blobs: deduped };
}

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/** @param {unknown} v */
function normMlbItem(v) {
  const s = str(v);
  const m = s.match(/^(?:MLB-?)?0*(\d{6,})$/i);
  return m ? `MLB${m[1]}` : s;
}

/** @param {unknown} v */
function pickName(prev, o) {
  const plain = str(o.plain_text);
  const title = str(o.title);
  const name = str(o.name);
  const long = [plain, title, name].filter(Boolean).sort((a, b) => b.length - a.length)[0] || '';
  if (!long) return prev;
  if (!prev) return long;
  return long.length >= prev.length ? long : prev;
}

/**
 * @param {unknown} n
 */
function numPrice(n) {
  if (n == null || n === '') return 0;
  const x = typeof n === 'number' ? n : Number(String(n).replace(',', '.'));
  return Number.isFinite(x) && x > 0 ? x : 0;
}

/**
 * @param {Record<string, unknown>} o
 */
function shippingFromObject(o) {
  const sh = o.shipping && typeof o.shipping === 'object' ? /** @type {Record<string, unknown>} */ (o.shipping) : null;
  if (sh) {
    const free = sh.free_shipping === true || sh.logistic_type === 'fulfillment';
    const price = sh.shipping_cost != null ? numPrice(sh.shipping_cost) : free ? 0 : numPrice(sh.price);
    const base = { ...emptyShipping() };
    if (free || price === 0) {
      base.is_free = true;
      base.price = 0;
      base.text = str(sh.text) || 'Frete (embedded)';
    } else if (price > 0) {
      base.is_free = false;
      base.price = price;
      base.text = str(sh.text) || `Frete R$ ${price}`;
    }
    if (shippingHasData(base) && base.text !== 'unknown') return base;
  }

  const labels = o.shipping_labels;
  if (Array.isArray(labels) && labels.some((x) => /gr[áa]tis|free/i.test(String(x)))) {
    return { ...emptyShipping(), is_free: true, price: 0, text: 'Frete grátis (labels)' };
  }
  return null;
}

/**
 * @param {Record<string, unknown>} o
 * @param {{ ctx: Record<string, unknown>, seen: WeakSet<object>, nodes: number }} bag
 * @param {number} depth
 */
function walkStateObject(o, bag, depth) {
  if (bag.nodes >= WALK_MAX_NODES || depth > WALK_MAX_DEPTH) return;
  if (!o || typeof o !== 'object') return;
  if (bag.seen.has(o)) return;
  bag.seen.add(o);
  bag.nodes++;

  const ctx = bag.ctx;

  const idRaw = o.id;
  if (typeof idRaw === 'string') {
    if (/^MLBU[0-9]+$/i.test(idRaw)) ctx.catalog_product_id = idRaw;
    else if (/^MLB/i.test(idRaw)) ctx.item_id = normMlbItem(idRaw);
  }

  const catId = str(o.catalog_product_id || o.parent_product_id);
  if (/^MLBU/i.test(catId)) ctx.catalog_product_id = catId;

  const itemStr = str(o.item_id);
  if (/^MLB/i.test(itemStr)) ctx.item_id = normMlbItem(itemStr);

  const v = str(o.variation_id ?? o.variationId);
  if (v && /^\d+$/.test(v)) ctx.variation_id = v;

  const sid = o.seller_id ?? (o.seller && typeof o.seller === 'object' ? /** @type {Record<string, unknown>} */ (o.seller).id : null);
  if (sid != null && String(sid).trim()) ctx.seller_id = String(sid).trim();

  if (o.domain_id) ctx.domain_id = str(o.domain_id);
  if (o.category_id) ctx.category_id = str(o.category_id);

  ctx.name = pickName(/** @type {string} */ (ctx.name || ''), o);

  const p1 = numPrice(o.price);
  const p2 = o.price_info && typeof o.price_info === 'object' ? numPrice(/** @type {Record<string, unknown>} */ (o.price_info).amount) : 0;
  const p3 =
    o.prices && typeof o.prices === 'object'
      ? numPrice(
          /** @type {Record<string, unknown>} */ (o.prices).price ||
            /** @type {Record<string, unknown>} */ (o.prices).amount
        )
      : 0;
  const pc = Math.max(p1, p2, p3);
  if (pc > 0) ctx.price_current = Math.max(numPrice(ctx.price_current), pc);

  const po =
    numPrice(o.original_price) ||
    (o.prices && typeof o.prices === 'object' ? numPrice(/** @type {Record<string, unknown>} */ (o.prices).original_price) : 0);
  if (po > 0) ctx.price_original = Math.max(numPrice(ctx.price_original), po);

  const cur = str(o.currency_id || o.currency);
  if (cur && cur.length <= 5) ctx.currency = cur;

  if (typeof o.sold_quantity === 'number' && Number.isFinite(o.sold_quantity) && o.sold_quantity >= 0) {
    ctx.sales_count = Math.max(numPrice(ctx.sales_count), o.sold_quantity);
    ctx.sales_from_exact_field = true;
  }
  if (o.sold_quantity_estimated === true || o.is_sold_quantity_estimated === true) ctx.sales_quantity_estimated_flag = true;

  const avail = o.available_quantity ?? o.stock_quantity ?? o.stock;
  if (avail != null && Number.isFinite(Number(avail)) && Number(avail) >= 0) {
    ctx.stock_hint = `${avail} unidades (embedded)`;
    ctx.available_quantity_embedded = Number(avail);
  }

  const rating = o.rating ?? o.stars ?? (o.reviews && typeof o.reviews === 'object' ? /** @type {Record<string, unknown>} */ (o.reviews).rating : null);
  if (typeof rating === 'number' && rating > 0) ctx.rating = Math.max(numPrice(ctx.rating), rating);

  const rc =
    o.review_count ?? o.reviews_count ?? (o.reviews && typeof o.reviews === 'object' ? /** @type {Record<string, unknown>} */ (o.reviews).count : null);
  if (rc != null && rc !== '') ctx.rating_count = Math.max(numPrice(ctx.rating_count), numPrice(rc));

  const perm = str(o.permalink || o.permalink_url || o.url || o.share_url);
  if (perm && /mercadoliv|mercadolibre/i.test(perm)) {
    if (!ctx.permalink || perm.length > String(ctx.permalink).length) ctx.permalink = perm;
  }

  const sn = o.shop_name ?? (o.seller && typeof o.seller === 'object' ? /** @type {Record<string, unknown>} */ (o.seller).nickname : null);
  if (sn && str(sn)) ctx.shop_name = str(sn);

  const path = o.bread_crumbs || o.breadcrumbs || o.categories_from_root;
  if (Array.isArray(path)) {
    /** @type {string[]} */
    const labels = [];
    for (const c of path) {
      if (typeof c === 'string' && c.trim()) labels.push(c.trim());
      else if (c && typeof c === 'object') {
        const l = str(/** @type {Record<string, unknown>} */ (c).label || /** @type {Record<string, unknown>} */ (c).name);
        if (l) labels.push(l);
      }
    }
    if (labels.length) ctx.breadcrumbs = labels;
  }

  if (Array.isArray(o.pictures)) {
    for (const pic of o.pictures) {
      if (pic && typeof pic === 'object') {
        const u =
          str(/** @type {Record<string, unknown>} */ (pic).secure_url) ||
          str(/** @type {Record<string, unknown>} */ (pic).url);
        if (u) {
          if (!Array.isArray(ctx.images)) ctx.images = [];
          /** @type {string[]} */ (ctx.images).push(u);
        }
      } else if (typeof pic === 'string' && pic.startsWith('http')) {
        if (!Array.isArray(ctx.images)) ctx.images = [];
        /** @type {string[]} */ (ctx.images).push(pic);
      }
    }
  }
  const thumb = str(o.thumbnail || o.secure_thumbnail);
  if (thumb) {
    if (!Array.isArray(ctx.images)) ctx.images = [];
    /** @type {string[]} */ (ctx.images).unshift(thumb);
  }

  const shObj = shippingFromObject(o);
  if (shObj && shippingHasData(shObj)) {
    const prev = /** @type {Record<string, unknown> | null} */ (ctx.shipping);
    if (!prev || !shippingHasData(prev) || JSON.stringify(shObj).length > JSON.stringify(prev).length) {
      ctx.shipping = shObj;
    }
  }

  if (Array.isArray(o.installments) && o.installments.length) {
    ctx.installments_embedded = o.installments;
  }

  if (Array.isArray(o.attributes) && o.attributes.length) {
    /** @type {{ name: string; value: string }[]} */
    const vv = [];
    for (const a of o.attributes) {
      if (!a || typeof a !== 'object') continue;
      const ar = /** @type {Record<string, unknown>} */ (a);
      const n = str(ar.name || ar.id);
      const val = str(ar.value_name || ar.value || ar.value_id);
      if (n && val) vv.push({ name: n, value: val });
    }
    if (vv.length) ctx.variants = vv;
  }

  for (const v of Object.values(o)) {
    if (v == null) continue;
    if (Array.isArray(v)) {
      for (const x of v) {
        if (x && typeof x === 'object') walkStateObject(/** @type {Record<string, unknown>} */ (x), bag, depth + 1);
      }
    } else if (typeof v === 'object') {
      walkStateObject(/** @type {Record<string, unknown>} */ (v), bag, depth + 1);
    }
  }
}

/**
 * Normaliza um único objeto de estado (um blob parseado) para partial canónico.
 * @param {unknown} stateBlob
 */
export function normalizeEmbeddedStateToPartial(stateBlob) {
  if (!stateBlob || typeof stateBlob !== 'object') {
    return {
      partial: {},
      field_sources: {},
      sales_count_precision: undefined,
      shipping_precision: undefined,
    };
  }

  /** @type {Record<string, unknown>} */
  const ctx = {
    item_id: '',
    catalog_product_id: '',
    variation_id: '',
    seller_id: '',
    domain_id: '',
    category_id: '',
    name: '',
    price_current: 0,
    price_original: 0,
    currency: '',
    rating: 0,
    rating_count: 0,
    sales_count: 0,
    permalink: '',
    shop_name: '',
    breadcrumbs: [],
    /** @type {string[]} */
    images: [],
    sales_from_exact_field: false,
    sales_quantity_estimated_flag: false,
  };

  walkStateObject(/** @type {Record<string, unknown>} */ (stateBlob), { ctx, seen: new WeakSet(), nodes: 0 }, 0);

  /** @type {Record<string, unknown>} */
  const partial = {};
  if (str(ctx.catalog_product_id)) partial.catalog_product_id = str(ctx.catalog_product_id);
  if (str(ctx.item_id)) partial.item_id = str(ctx.item_id);
  if (str(ctx.variation_id)) partial.variation_id = str(ctx.variation_id);
  if (str(ctx.seller_id)) partial.seller_id = str(ctx.seller_id);
  if (str(ctx.domain_id)) partial.domain_id = str(ctx.domain_id);
  if (str(ctx.category_id)) partial.category_id = str(ctx.category_id);
  if (str(ctx.name)) partial.name = str(ctx.name);
  if (numPrice(ctx.price_current) > 0) partial.price_current = numPrice(ctx.price_current);
  if (numPrice(ctx.price_original) > 0) partial.price_original = numPrice(ctx.price_original);
  if (str(ctx.currency)) partial.price_currency = str(ctx.currency);
  if (numPrice(ctx.rating) > 0) partial.rating = numPrice(ctx.rating);
  if (numPrice(ctx.rating_count) > 0) partial.rating_count = Math.floor(numPrice(ctx.rating_count));
  if (numPrice(ctx.sales_count) > 0) partial.sales_count = numPrice(ctx.sales_count);
  if (str(ctx.stock_hint)) partial.stock_hint = str(ctx.stock_hint);
  if (ctx.available_quantity_embedded != null) partial.available_quantity_embedded = ctx.available_quantity_embedded;
  if (str(ctx.permalink)) {
    partial.url = str(ctx.permalink);
    partial.url_primary = str(ctx.permalink);
  }
  if (str(ctx.shop_name)) partial.shop_name = str(ctx.shop_name);
  if (Array.isArray(ctx.breadcrumbs) && ctx.breadcrumbs.length) {
    partial.product_category_from_breadcrumb = ctx.breadcrumbs.join(' > ');
    partial.taxonomy_path = ctx.breadcrumbs.join(' > ');
    partial.categories = [.../** @type {string[]} */ (ctx.breadcrumbs)];
  }
  if (ctx.shipping && shippingHasData(/** @type {Record<string, unknown>} */ (ctx.shipping))) {
    partial.shipping = ctx.shipping;
  }
  if (Array.isArray(ctx.images)) {
    const imgs = [...new Set(/** @type {string[]} */ (ctx.images))].filter(Boolean);
    partial.images = imgs;
    if (imgs[0]) partial.image_main = imgs[0];
  }
  if (Array.isArray(ctx.variants) && ctx.variants.length) partial.variants = ctx.variants;
  if (Array.isArray(ctx.installments_embedded) && ctx.installments_embedded.length) {
    partial.installments_embedded = ctx.installments_embedded;
  }

  partial.collected_at = new Date().toISOString();

  /** @type {Record<string, string>} */
  const field_sources = {};
  for (const k of Object.keys(partial)) {
    if (k === 'collected_at') continue;
    field_sources[k] = 'embedded_json';
  }

  /** @type {'exact' | 'approximate' | 'conditional' | 'unknown' | undefined} */
  let sales_count_precision;
  if (numPrice(ctx.sales_count) > 0) {
    if (ctx.sales_quantity_estimated_flag) sales_count_precision = 'approximate';
    else if (ctx.sales_from_exact_field) sales_count_precision = 'exact';
    else sales_count_precision = 'approximate';
  }

  /** @type {'exact' | 'approximate' | 'conditional' | 'unknown' | undefined} */
  let shipping_precision;
  if (partial.shipping && typeof partial.shipping === 'object') {
    const t = str(/** @type {Record<string, unknown>} */ (partial.shipping).text);
    if (/primeira\s+compra|1[ªa]\s+compra/i.test(t)) shipping_precision = 'conditional';
    else if (/** @type {Record<string, unknown>} */ (partial.shipping).is_free === true) shipping_precision = 'exact';
    else if (numPrice(/** @type {Record<string, unknown>} */ (partial.shipping).price) > 0) shipping_precision = 'exact';
    else shipping_precision = 'unknown';
  }

  if (sales_count_precision) partial.sales_count_precision = sales_count_precision;
  if (shipping_precision) partial.shipping_precision = shipping_precision;

  if (sales_count_precision) field_sources.sales_count_precision = 'embedded_json';
  if (shipping_precision) field_sources.shipping_precision = 'embedded_json';

  return { partial, field_sources, sales_count_precision, shipping_precision };
}

/**
 * @param {Record<string, unknown>} a
 * @param {Record<string, unknown>} b
 */
function mergeShallowPartials(a, b) {
  const out = { ...a };
  for (const [k, v] of Object.entries(b)) {
    if (v === undefined || v === null) continue;
    if (k === 'images' && Array.isArray(v)) {
      const prev = Array.isArray(out.images) ? /** @type {string[]} */ (out.images) : [];
      out.images = [...new Set([...prev, .../** @type {string[]} */ (v)])];
      continue;
    }
    if (k === 'shipping' && v && typeof v === 'object') {
      const prev = out.shipping;
      if (!prev || !shippingHasData(/** @type {Record<string, unknown>} */ (prev))) out.shipping = v;
      else if (shippingHasData(/** @type {Record<string, unknown>} */ (v))) {
        if (JSON.stringify(v).length >= JSON.stringify(prev).length) out.shipping = v;
      }
      continue;
    }
    if (k === 'variants' && Array.isArray(v) && v.length) {
      if (!Array.isArray(out.variants) || /** @type {unknown[]} */ (out.variants).length < v.length) out.variants = v;
      continue;
    }
    if (k === 'categories' && Array.isArray(v) && v.length) {
      if (!Array.isArray(out.categories) || /** @type {unknown[]} */ (out.categories).length < v.length) {
        out.categories = v;
      }
      continue;
    }
    if (typeof v === 'number' && typeof out[k] === 'number') {
      out[k] = Math.max(/** @type {number} */ (out[k]), v);
      continue;
    }
    if (typeof v === 'string' && typeof out[k] === 'string') {
      const ps = /** @type {string} */ (out[k]);
      const ns = v;
      if (ns.length > ps.length) out[k] = ns;
      continue;
    }
    if (out[k] == null || out[k] === '' || out[k] === 0) out[k] = v;
  }
  return out;
}

/**
 * Extrai estado embutido do HTML e devolve partial fundido + metadados.
 * @param {string} html
 */
export function buildEmbeddedPartialFromHtml(html) {
  const { found, blobs } = extractEmbeddedStateFromHtml(html);
  /** @type {Record<string, unknown>} */
  let mergedPartial = {};
  /** @type {Record<string, string>} */
  const field_sources = {};

  for (const b of blobs) {
    if (!b.parsed) continue;
    const { partial, field_sources: fs } = normalizeEmbeddedStateToPartial(b.parsed);
    mergedPartial = mergeShallowPartials(mergedPartial, partial);
    Object.assign(field_sources, fs);
  }

  return {
    found: found && Object.keys(mergedPartial).length > 0,
    blobs,
    partial: mergedPartial,
    field_sources,
    source_conflicts: [],
  };
}
