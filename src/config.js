import 'dotenv/config';

function boolEnv(name, defaultValue) {
  const v = process.env[name];
  if (v === undefined || v === '') return defaultValue;
  return String(v).toLowerCase() === 'true' || v === '1';
}

function numEnv(name, defaultValue) {
  const v = process.env[name];
  if (v === undefined || v === '') return defaultValue;
  const n = Number(v);
  return Number.isFinite(n) ? n : defaultValue;
}

/** Limite opcional (testes). `null` = sem limite. */
function optionalPositiveIntEnv(name) {
  const v = process.env[name];
  if (v === undefined || v === '') return null;
  const n = Number(String(v).trim());
  if (!Number.isFinite(n) || n < 1) return null;
  return Math.floor(n);
}

/** Configuração mínima para Puppeteer (Mercado Livre). */
export const config = {
  /**
   * Além dos ficheiros “latest” em output/, grava cópias em output/history/{catalog,pdp,debug,metrics}/ com timestamp.
   */
  saveHistoryOutputs: boolEnv('SAVE_HISTORY_OUTPUTS', false),
  /** Raiz das pastas de histórico (subpastas: catalog, pdp, debug, metrics). */
  historyOutputRoot: (process.env.SAVE_HISTORY_ROOT && String(process.env.SAVE_HISTORY_ROOT).trim()) || './output/history',

  headless: boolEnv('HEADLESS', false),
  userDataDir: process.env.USER_DATA_DIR || './chrome-profile',
  /**
   * Chrome em modo anónimo (--incognito) + sem pasta de perfil persistente.
   * Útil quando o ML fica a pedir login em ciclo no mesmo USER_DATA_DIR.
   * Ignora USER_DATA_DIR e ML_LISTA_USER_DATA_DIR neste arranque.
   */
  mlBrowserIncognito: boolEnv('ML_BROWSER_INCOGNITO', false),
  /**
   * User-Agent fixo para o Puppeteer (recomendado para o ML aceitar a sessão guardada no perfil).
   * Vazio = usa Chrome/Windows estável (não aleatório; antes o aleatório pedia login de novo).
   */
  browserUserAgent: (process.env.ML_BROWSER_USER_AGENT && String(process.env.ML_BROWSER_USER_AGENT).trim()) || '',
  /**
   * Não fechar o Chrome após o PDP; o Node só termina quando fechas a janela.
   * Por defeito: true com HEADLESS=false (para acompanhares), false com HEADLESS=true.
   * Força fechar: ML_KEEP_BROWSER_OPEN=false
   */
  keepBrowserOpen: (() => {
    const v = process.env.ML_KEEP_BROWSER_OPEN;
    if (v !== undefined && v !== '') {
      return String(v).toLowerCase() === 'true' || v === '1';
    }
    return !boolEnv('HEADLESS', false);
  })(),

  /**
   * PDP: tempo extra (ms) a aguardar pelo JSON-LD do produto quando o ML mostra login/verificação.
   * 0 = só o tempo base (~28s); usa em conjunto com ML_PDP_AUTO_WAIT_LOGIN_WHEN_VISIBLE.
   */
  mlPdpWaitLoginMs: numEnv('ML_PDP_WAIT_LOGIN_MS', 0),
  /**
   * Com Chrome visível (HEADLESS=false), se a página for bloqueio/login, aguardar automaticamente
   * até ML_PDP_AUTO_WAIT_LOGIN_MAX_MS para completares o login e o produto carregar.
   */
  mlPdpAutoWaitLoginWhenVisible: boolEnv('ML_PDP_AUTO_WAIT_LOGIN_WHEN_VISIBLE', true),
  /** Máx. extra (ms) quando há interstitial de login; reduz se não precisares de 3 min. */
  mlPdpAutoWaitLoginMaxMs: numEnv('ML_PDP_AUTO_WAIT_LOGIN_MAX_MS', 90_000),
  /**
   * Se true (defeito), ao detetar verificação de conta/login no PDP, falha logo com erro account_verification
   * sem esperar JSON-LD. Para completar login manualmente no Chrome visível: ML_PDP_FAIL_FAST_ON_ACCOUNT_CHALLENGE=false
   * e mantém ML_PDP_AUTO_WAIT_LOGIN_WHEN_VISIBLE=true.
   */
  mlPdpFailFastOnAccountChallenge: boolEnv('ML_PDP_FAIL_FAST_ON_ACCOUNT_CHALLENGE', true),
  /**
   * Na listagem HTML, após detetar verificação de conta, fecha o browser lista antes de continuar
   * (próxima página abre sessão nova).
   */
  mlListaCloseBrowserOnAccountChallenge: boolEnv('ML_LISTA_CLOSE_BROWSER_ON_ACCOUNT_CHALLENGE', true),
  /** Tempo máximo (ms) a procurar JSON-LD quando a página já carregou sem bloqueio óbvio. */
  mlPdpJsonLdMaxWaitMs: numEnv('ML_PDP_JSONLD_MAX_WAIT_MS', 18_000),
  /** Intervalo entre tentativas de ler JSON-LD (ms). */
  mlPdpPollIntervalMs: numEnv('ML_PDP_POLL_INTERVAL_MS', 280),
  /** Pausa após scroll antes de extrair DOM (ms). */
  mlPdpDomScrollDelayMs: numEnv('ML_PDP_DOM_SCROLL_DELAY_MS', 600),
  /**
   * Antes do URL do produto, abrir esta página (home / categorias) para “aquecer” a sessão e evitar salto direto para registro.
   * Desliga com ML_PDP_WARMUP_URL=0 ou ML_PDP_WARMUP_URL=false
   */
  mlPdpWarmupUrl: (() => {
    const v = process.env.ML_PDP_WARMUP_URL;
    if (v === '0' || String(v).toLowerCase() === 'false') return '';
    if (v !== undefined && v !== '') return String(v).trim();
    return 'https://www.mercadolivre.com.br/';
  })(),
  /** Pausa após o warm-up antes de ir ao PDP (ms). */
  mlPdpWarmupDelayMs: numEnv('ML_PDP_WARMUP_DELAY_MS', 800),
  /**
   * Milissegundos antes de fechar o browser após o scrape (só quando não usas ML_KEEP_BROWSER_OPEN).
   * Se não definires (env vazio): com HEADLESS=false usa 8s para conseguires ver a página; com HEADLESS=true usa 0.
   */
  mlPdpPauseBeforeCloseMs: (() => {
    const v = process.env.ML_PDP_PAUSE_BEFORE_CLOSE_MS;
    if (v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? Math.max(0, n) : null;
  })(),

  /**
   * Ficheiro onde `npm start` grava o JSON do PDP (além do stdout).
   * Desliga gravação: ML_PDP_OUTPUT=- ou ML_PDP_OUTPUT=none
   */
  mlPdpOutput: (() => {
    const v = process.env.ML_PDP_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/pdp_last.json';
  })(),
  /** JSON indentado no ficheiro do PDP. */
  mlPdpPretty: boolEnv('ML_PDP_PRETTY', true),
  /**
   * PDP agregado enxuto (paralelo a pdp_all). Desliga: ML_PDP_LEAN_OUTPUT=- ou none
   * (Sem `_field_sources` / conflitos / rejeições — ver `mlPdpDebugLeanOutput`.)
   */
  mlPdpLeanOutput: (() => {
    const v = process.env.ML_PDP_LEAN_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/pdp_all_lean.json';
  })(),
  /**
   * PDP debug enxuto (conflitos/rejeições sanitizados). Desliga: ML_PDP_DEBUG_LEAN_OUTPUT=- ou none
   */
  mlPdpDebugLeanOutput: (() => {
    const v = process.env.ML_PDP_DEBUG_LEAN_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/pdp_debug_lean.json';
  })(),

  /** Catálogo (API + opcional browser) */
  mlSiteId: process.env.ML_SITE_ID || 'MLB',
  mlCatalogOutput: process.env.ML_CATALOG_OUTPUT || './output/catalogo_ml.json',
  /**
   * Catálogo enxuto (só campos de descoberta). Desliga: ML_CATALOG_LEAN_OUTPUT=- ou none
   */
  mlCatalogLeanOutput: (() => {
    const v = process.env.ML_CATALOG_LEAN_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/catalogo_ml_lean.json';
  })(),
  /** JSON indentado (2 espaços). `ML_CATALOG_PRETTY=false` = uma linha (ficheiro menor). */
  mlCatalogPretty: boolEnv('ML_CATALOG_PRETTY', true),
  mlApiDelayMs: numEnv('ML_API_DELAY_MS', 150),
  /** Força sempre fetch via Puppeteer (útil se o teu IP levar 403 em Node). */
  mlUseBrowserForApi: boolEnv('ML_USE_BROWSER_FOR_API', false),
  /** Máx. itens únicos guardados (omitir = ilimitado). */
  mlCatalogMaxItems: optionalPositiveIntEnv('ML_CATALOG_MAX_ITEMS'),
  /** Só N categorias-folha (omitir = todas). Para teste rápido. */
  mlCatalogMaxCategories: optionalPositiveIntEnv('ML_CATALOG_MAX_CATEGORIES'),
  mlSearchPageSize: numEnv('ML_SEARCH_PAGE_SIZE', 50),
  /**
   * Gravar `catalogo_ml.json` a cada N categorias processadas (0 = só no fim e no Ctrl+C).
   * Default 1 = após cada categoria. Combinar com `ML_LISTA_FLUSH_JSON_EVERY_PAGES` para gravar também *dentro* da categoria.
   */
  mlCatalogFlushEvery: numEnv('ML_CATALOG_FLUSH_EVERY', 1),
  /**
   * Modo lista HTML: gravar o JSON a cada N **páginas** de listagem dentro da mesma categoria.
   * Default 1 = cada página (vês o ficheiro a crescer sem esperar a categoria inteira). 0 = desliga.
   */
  mlListaFlushJsonEveryPages: numEnv('ML_LISTA_FLUSH_JSON_EVERY_PAGES', 1),
  /**
   * Modo API: gravar o JSON após cada resposta paginada da busca (~50 itens por chamada).
   */
  mlCatalogFlushApiEveryPage: boolEnv('ML_CATALOG_FLUSH_API_EVERY_PAGE', true),

  /**
   * PDP em massa: lê o JSON do catálogo (`meta` + `items`).
   * `ML_BULK_INPUT` tem prioridade; senão usa `ML_CATALOG_OUTPUT` ou `./output/catalogo_ml.json`.
   */
  mlBulkInput:
    (process.env.ML_BULK_INPUT && String(process.env.ML_BULK_INPUT).trim()) ||
    process.env.ML_CATALOG_OUTPUT ||
    './output/catalogo_ml.json',
  /**
   * Onde gravar o resultado agregado. Desliga: ML_BULK_OUTPUT=- ou none
   */
  mlBulkOutput: (() => {
    const v = process.env.ML_BULK_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/pdp_all.json';
  })(),
  mlBulkPretty: boolEnv('ML_BULK_PRETTY', true),
  /** Pausa entre um PDP e o seguinte (ms). */
  mlBulkDelayMs: numEnv('ML_BULK_DELAY_MS', 2000),
  /** Gravar ficheiro a cada N produtos (0 = só no fim e no Ctrl+C). */
  mlBulkFlushEvery: numEnv('ML_BULK_FLUSH_EVERY', 5),
  /** Limite de itens (testes). Omitir = todos os URLs resolvíveis. */
  mlBulkMaxItems: optionalPositiveIntEnv('ML_BULK_MAX_ITEMS'),
  /**
   * Duração máxima do lote em ms (ex.: 180000 = 3 min). Omitir = sem limite de tempo.
   * Ao atingir, o loop termina de forma limpa (flush de `pdp_all.json` e `metrics.json`, browser fechado).
   */
  mlBulkMaxDurationMs: optionalPositiveIntEnv('ML_BULK_MAX_DURATION_MS'),

  /**
   * Fechar e relançar o browser a cada N produtos no bulk (0 / omitido = nunca).
   * Útil para mitigar memória ou sessão degradada. A mesma pasta USER_DATA_DIR é reutilizada.
   */
  mlBulkBrowserRecycleEvery: optionalPositiveIntEnv('ML_BULK_BROWSER_RECYCLE_EVERY'),

  /**
   * Log de sessão (JSONL, uma linha por evento: recycle, nova página, etc.).
   * Ex.: ./output/bulk_debug/session.log — ver comentário no topo de runMlPdpBulk.js sobre artefactos.
   */
  mlBulkSessionLog: (() => {
    const v = process.env.ML_BULK_SESSION_LOG;
    if (v === undefined || v === '' || String(v).toLowerCase() === 'none' || v === '-') return '';
    return String(v).trim();
  })(),

  /**
   * Métricas operacionais do bulk PDP. Desliga: ML_METRICS_OUTPUT=- ou none
   */
  mlMetricsOutput: (() => {
    const v = process.env.ML_METRICS_OUTPUT;
    if (v === '-' || String(v || '').toLowerCase() === 'none') return '';
    return (v && String(v).trim()) || './output/metrics.json';
  })(),
  /**
   * JSONL auxiliares (discovered/enriched/invalid/duplicate). Por defeito desligado.
   * Liga com ML_DEBUG_OUTPUTS=true ou ML_BULK_JSONL=true (compatibilidade).
   */
  mlDebugOutputs: boolEnv('ML_DEBUG_OUTPUTS', false),
  mlBulkJsonl: boolEnv('ML_BULK_JSONL', false),
  mlDiscoveredJsonl:
    (process.env.ML_DISCOVERED_JSONL && String(process.env.ML_DISCOVERED_JSONL).trim()) ||
    './output/discovered_products.jsonl',
  mlEnrichedJsonl:
    (process.env.ML_ENRICHED_JSONL && String(process.env.ML_ENRICHED_JSONL).trim()) ||
    './output/enriched_products.jsonl',
  mlInvalidJsonl:
    (process.env.ML_INVALID_JSONL && String(process.env.ML_INVALID_JSONL).trim()) ||
    './output/invalid_products.jsonl',
  mlDuplicateJsonl:
    (process.env.ML_DUPLICATE_JSONL && String(process.env.ML_DUPLICATE_JSONL).trim()) ||
    './output/duplicate_products.jsonl',

  /**
   * Pipeline paralelo (`npm run pipeline`): fila JSONL de descoberta + registo de PDP processados.
   */
  mlPipelineDiscoveredJsonl:
    (process.env.ML_PIPELINE_DISCOVERED_JSONL && String(process.env.ML_PIPELINE_DISCOVERED_JSONL).trim()) ||
    './output/pipeline_discovered.jsonl',
  mlPipelineProcessedJsonl:
    (process.env.ML_PIPELINE_PROCESSED_JSONL && String(process.env.ML_PIPELINE_PROCESSED_JSONL).trim()) ||
    './output/pipeline_processed.jsonl',
  mlPipelineOffsetFile:
    (process.env.ML_PIPELINE_OFFSET_FILE && String(process.env.ML_PIPELINE_OFFSET_FILE).trim()) ||
    './output/pipeline_consumer.offset',
  /** Intervalo (ms) entre leituras da fila descoberta pelo consumidor. */
  mlPipelinePollMs: numEnv('ML_PIPELINE_POLL_MS', 900),
  /** Se true, apaga fila + offset + processed ao iniciar o pipeline (novo run limpo). */
  mlPipelineFresh: boolEnv('ML_PIPELINE_FRESH', false),

  /**
   * Catálogo: `auto` tenta API e cai para lista.mercadolivre.com.br se 403;
   * `api` só API; `lista` só HTML de listagem (sem preço no card).
   */
  mlCatalogSource: (process.env.ML_CATALOG_SOURCE || 'auto').toLowerCase(),

  /**
   * Se o GET da listagem devolver página de tráfego suspeito, tentar de novo com Puppeteer (perfil em USER_DATA_DIR).
   * Mais lento, mas útil quando o fetch “nu” é bloqueado.
   */
  mlListaBrowserOnBlock: boolEnv('ML_LISTA_BROWSER_ON_BLOCK', false),

  /**
   * Perfil Chrome só para o crawl lista (evita “Target closed” se o PDP deixou Chrome aberto no mesmo USER_DATA_DIR).
   * Por defeito: `{USER_DATA_DIR ou ./chrome-profile}-lista`. OneDrive a sincronizar a pasta do perfil também causa falhas.
   */
  mlListaUserDataDir: (() => {
    const v = process.env.ML_LISTA_USER_DATA_DIR;
    if (v !== undefined && String(v).trim() !== '') return String(v).trim();
    const main = process.env.USER_DATA_DIR || './chrome-profile';
    const trimmed = main.replace(/[/\\]+$/, '');
    return `${trimmed}-lista`;
  })(),

  /** Modo lista: atraso entre pedidos HTML (ms). */
  mlListaDelayMs: numEnv('ML_LISTA_DELAY_MS', 280),
  /** Após goto no browser, pausa extra para o React da listagem pintar (evita HTML vazio). */
  mlListaBrowserSettleMs: numEnv('ML_LISTA_BROWSER_SETTLE_MS', 900),
  /** Offset entre páginas (_Desde_) — típico 49. */
  mlListaPageStep: numEnv('ML_LISTA_PAGE_STEP', 49),
  /** Máx. páginas por categoria no modo lista (omitir = sem limite). */
  mlListaMaxPagesPerCategory: optionalPositiveIntEnv('ML_LISTA_MAX_PAGES_PER_CATEGORY'),

  /** Análise (npm run analyze): entrada JSON agregado do bulk PDP. */
  mlAnalyzeInput: (process.env.ML_ANALYZE_INPUT && String(process.env.ML_ANALYZE_INPUT).trim()) || './output/pdp_all.json',
  mlAnalyzeOutput: (process.env.ML_ANALYZE_OUTPUT && String(process.env.ML_ANALYZE_OUTPUT).trim()) || './output/analise_ml.json',
  mlAnalyzePretty: boolEnv('ML_ANALYZE_PRETTY', true),
};
