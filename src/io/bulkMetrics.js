/**
 * Métricas operacionais do bulk PDP (sem analytics comercial).
 *
 * O snapshot inclui:
 * - **Campos raiz legados** (`processed_items`, `fields_filled`, …): compatibilidade com leitores existentes.
 * - **Blocos agrupados** (`run_counts`, `storage_counts`, `discovery_counts`, `quality_counts`, `block_stats`):
 *   visão operacional recomendada (mesmos dados, organizados).
 * - **`work_counts` + `snapshot_counts`**: carga de trabalho da sessão vs tamanho do agregado em memória — **não misturar** com `total_input_items` no pipeline.
 *
 * **`total_input_items` — semântica por fluxo:**
 * - **Bulk** (`runMlPdpBulk`): `toRun.length` = tamanho do lote desta execução (coerente).
 * - **Pipeline** (`mlPipelineConsumer`): valor **legado** mantido por compatibilidade:
 *   `|itemsOut| + queue.length + processedCount`. Essa soma mistura tamanho do snapshot com tentativas da corrida e
 *   **conta cada sucesso duas vezes** (em `itemsOut` e de novo em `processedCount`). **Não usar** para leitura operacional
 *   no pipeline — usar `work_counts` e `snapshot_counts`.
 *
 * `source_meta`: opcionalmente o `meta` do payload agregado (ex.: `catalog.meta` no bulk) para preencher
 * `discovery_counts` / parte de `block_stats` quando `meta.stats` existe (API ou lista).
 *
 * `work_units_pending`: fila ainda não processada nesta sessão (obrigatório no pipeline; bulk passa `toRun.length - processedCount`).
 */

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/** @param {Record<string, unknown>} p */
function fieldFilled(p, key) {
  const v = p[key];
  if (v == null || v === '') return false;
  if (typeof v === 'number' && !Number.isFinite(v)) return false;
  if (key === 'rating' && Number(v) <= 0) return false;
  if (key === 'price_current' && Number(v) <= 0) return false;
  if (Array.isArray(v) && v.length === 0) return false;
  if (key === 'shipping' && v && typeof v === 'object') {
    const t = str(/** @type {Record<string, unknown>} */ (v).text);
    if (t === 'unknown' || t === '') return false;
  }
  return true;
}

/** @param {unknown} stats */
function statsRecord(stats) {
  return stats && typeof stats === 'object' && !Array.isArray(stats) ? /** @type {Record<string, unknown>} */ (stats) : null;
}

/**
 * @param {Record<string, unknown> | null} stats
 * @param {string} key
 */
function statNumberOrNull(stats, key) {
  if (!stats) return null;
  const v = stats[key];
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

/** @param {string} msg */
export function classifyBulkError(msg) {
  const s = str(msg).slice(0, 400);
  if (/account_verification/i.test(s)) return 'account_verification';
  if (/login|verificação|bloqueio|suspicious-traffic/i.test(s)) return 'blocked_or_challenge';
  if (/JSON-LD|schema\.org|embedded/i.test(s)) return 'pdp_content';
  if (/Navigation failed|timeout|net::ERR/i.test(s)) return 'navigation';
  if (/Target closed|Session closed|Protocol error|Execution context was destroyed/i.test(s)) return 'browser_session';
  if (/TypeError|ReferenceError/i.test(s)) return 'unexpected_error';
  return 'other';
}

/**
 * Agrega contagens a partir dos itens únicos gravados e erros.
 * @param {{
 *   run_started_at: string;
 *   run_finished_at: string;
 *   duration_seconds: number;
 *   total_input_items: number;
 *   processed_items: number;
 *   success_items: number;
 *   failed_items: number;
 *   unique_items_stored: number;
 *   duplicate_items: number;
 *   browser_restarts: number;
 *   page_reloads: number;
 *   interrupted: boolean;
 *   flush_reason?: string;
 *   run_stopped_reason?: 'complete' | 'max_duration' | 'sigint' | string;
 *   debug_outputs: boolean;
 *   items: Record<string, Record<string, unknown>>;
 *   errors: { product_id: string; url: string; message: string }[];
 *   source_meta?: Record<string, unknown> | null;
 *   new_discovered_this_run?: number | null;
 *   work_units_pending?: number;
 * }} args
 */
export function buildBulkMetricsSnapshot(args) {
  const items = Object.values(args.items || {});
  const metaTop = args.source_meta && typeof args.source_meta === 'object' && !Array.isArray(args.source_meta)
    ? /** @type {Record<string, unknown>} */ (args.source_meta)
    : null;
  const st = statsRecord(metaTop?.stats);
  const pipelineMode = Boolean(metaTop?.pipeline === true);

  const workPendingRaw = args.work_units_pending;
  const workPending =
    typeof workPendingRaw === 'number' && Number.isFinite(workPendingRaw)
      ? Math.max(0, workPendingRaw)
      : Math.max(0, args.total_input_items - args.processed_items);

  const snapshotItemsTotal = Object.keys(args.items || {}).length;
  const workUnitsTotal = args.processed_items + workPending;

  /** @type {Record<string, number>} */
  const errors_by_type = {};
  for (const e of args.errors || []) {
    const k = classifyBulkError(e.message);
    errors_by_type[k] = (errors_by_type[k] || 0) + 1;
  }

  let valid_items = 0;
  let invalid_items = 0;
  let validation_missing_items = 0;
  let embedded_found = 0;
  let json_ld_found = 0;
  let dom_used = 0;
  let api_item_used = 0;
  let api_product_used = 0;

  /** @type {Record<string, number>} */
  const fields_filled = {
    item_id: 0,
    catalog_product_id: 0,
    seller_id: 0,
    name: 0,
    price_current: 0,
    rating: 0,
    rating_count: 0,
    shipping: 0,
    images: 0,
    image_main: 0,
    description: 0,
    permalink_or_url: 0,
    category_id: 0,
    domain_id: 0,
  };

  for (const raw of items) {
    const p = raw && typeof raw === 'object' ? /** @type {Record<string, unknown>} */ (raw) : {};

    const v = p.validation && typeof p.validation === 'object' ? /** @type {Record<string, boolean>} */ (p.validation) : null;
    if (!v) {
      validation_missing_items += 1;
    } else {
      const allOk =
        v.valid_identity !== false &&
        v.valid_pricing !== false &&
        v.valid_seller !== false &&
        v.valid_shipping !== false &&
        v.valid_category !== false &&
        v.valid_media !== false;
      if (allOk) valid_items += 1;
      else invalid_items += 1;
    }

    const fs = p._field_sources && typeof p._field_sources === 'object' ? /** @type {Record<string, string>} */ (p._field_sources) : {};
    const vals = Object.values(fs);
    if (vals.some((x) => x === 'embedded_json')) embedded_found += 1;
    if (vals.some((x) => x === 'json_ld')) json_ld_found += 1;
    if (vals.some((x) => x === 'dom')) dom_used += 1;
    if (vals.some((x) => x === 'api_item')) api_item_used += 1;
    if (vals.some((x) => x === 'api_product')) api_product_used += 1;

    if (fieldFilled(p, 'item_id')) fields_filled.item_id += 1;
    if (fieldFilled(p, 'catalog_product_id')) fields_filled.catalog_product_id += 1;
    if (fieldFilled(p, 'seller_id')) fields_filled.seller_id += 1;
    if (fieldFilled(p, 'name')) fields_filled.name += 1;
    if (fieldFilled(p, 'price_current')) fields_filled.price_current += 1;
    if (fieldFilled(p, 'rating')) fields_filled.rating += 1;
    if (p.rating_count != null && p.rating_count !== '' && Number(p.rating_count) > 0) fields_filled.rating_count += 1;
    if (fieldFilled(p, 'shipping')) fields_filled.shipping += 1;
    if (Array.isArray(p.images) && p.images.length > 0) fields_filled.images += 1;
    if (fieldFilled(p, 'image_main')) fields_filled.image_main += 1;
    if (fieldFilled(p, 'description')) fields_filled.description += 1;
    if (fieldFilled(p, 'url') || fieldFilled(p, 'url_primary')) fields_filled.permalink_or_url += 1;
    if (fieldFilled(p, 'category_id')) fields_filled.category_id += 1;
    if (fieldFilled(p, 'domain_id')) fields_filled.domain_id += 1;
  }

  const run_stopped_reason =
    args.run_stopped_reason != null && String(args.run_stopped_reason).trim()
      ? String(args.run_stopped_reason).trim()
      : args.interrupted
        ? 'sigint'
        : 'complete';

  const listaAccount = st ? statNumberOrNull(st, 'lista_account_verification') ?? 0 : 0;
  const errAccount = errors_by_type.account_verification ?? 0;
  const listaFallback = st ? statNumberOrNull(st, 'lista_browser_fallbacks') ?? 0 : 0;
  const listaBlocked = st ? statNumberOrNull(st, 'lista_blocked_pages') ?? 0 : 0;

  const newDiscovered =
    args.new_discovered_this_run != null && typeof args.new_discovered_this_run === 'number' && Number.isFinite(args.new_discovered_this_run)
      ? args.new_discovered_this_run
      : null;

  return {
    metrics_note:
      'Campos no nível raiz (processed_items, fields_filled, …) mantêm compatibilidade. Para operação: run_counts, storage_counts, work_counts, snapshot_counts, discovery_counts, quality_counts, block_stats. No pipeline, ignorar total_input_items para “tamanho de trabalho” — ver input_total_readme.',

    run_type: 'bulk_pdp',
    run_started_at: args.run_started_at,
    run_finished_at: args.run_finished_at,
    duration_seconds: args.duration_seconds,
    run_stopped_reason,

    run_counts: {
      processed_this_run: args.processed_items,
      success_this_run: args.success_items,
      failed_this_run: args.failed_items,
    },
    storage_counts: {
      stored_total: args.unique_items_stored,
      valid_total: valid_items,
      invalid_total: invalid_items,
      duplicate_total: args.duplicate_items,
    },
    discovery_counts: {
      categories_total: statNumberOrNull(st, 'categories_total'),
      categories_done: statNumberOrNull(st, 'categories_done'),
      catalog_items_total: statNumberOrNull(st, 'items_unique'),
      new_discovered_this_run: newDiscovered,
    },
    quality_counts: {
      with_item_id: fields_filled.item_id,
      with_seller_id: fields_filled.seller_id,
      with_name: fields_filled.name,
      with_price_current: fields_filled.price_current,
      with_rating: fields_filled.rating,
      with_rating_count: fields_filled.rating_count,
      with_shipping: fields_filled.shipping,
      with_images: fields_filled.images,
      with_image_main: fields_filled.image_main,
      with_description: fields_filled.description,
      with_category_id: fields_filled.category_id,
      with_domain_id: fields_filled.domain_id,
    },
    block_stats: {
      account_verification_count: errAccount + listaAccount,
      browser_fallback_count: listaFallback,
      blocked_pages_count: listaBlocked,
    },

    work_counts: {
      work_units_total: workUnitsTotal,
      work_units_processed: args.processed_items,
      work_units_pending: workPending,
    },
    snapshot_counts: {
      snapshot_items_total: snapshotItemsTotal,
    },

    input_total_readme: pipelineMode
      ? 'Pipeline: total_input_items é legado (|itemsOut|+fila+processed neste flush) e dupla-conta sucessos. Use work_counts + snapshot_counts.'
      : 'Bulk: total_input_items = toRun.length (lote). work_counts.reflete processado + pendente neste flush.',

    total_input_items: args.total_input_items,
    processed_items: args.processed_items,
    success_items: args.success_items,
    failed_items: args.failed_items,
    unique_items_stored: args.unique_items_stored,
    valid_items,
    invalid_items,
    duplicate_items: args.duplicate_items,
    validation_missing_items,
    browser_restarts: args.browser_restarts,
    page_reloads: args.page_reloads,
    embedded_found,
    json_ld_found,
    dom_used,
    api_item_used,
    api_product_used,
    fields_filled,
    errors_by_type,
    interrupted: args.interrupted,
    flush_reason: args.flush_reason ?? null,
    debug_outputs: args.debug_outputs,
  };
}
