/**
 * Deteção de verificação de conta / login / bloqueio do Mercado Livre.
 * Cuidado: frases como "Sou novo" / "Já tenho conta" vêm no HTML normal de listagens — não usar só isso sem contexto.
 */

/** Prefixo estável para erros e métricas. */
export const ML_ACCOUNT_VERIFICATION_PREFIX = 'account_verification';

/**
 * URL final ou pedido (Node ou browser location.href).
 * @param {string} url
 */
export function challengeUrlLooksBlocked(url) {
  const u = String(url || '').toLowerCase();
  if (!u) return false;
  if (u.includes('account-verification')) return true;
  if (u.includes('gz/account-verification')) return true;
  if (u.includes('/registration')) return true;
  if (u.includes('registrationtype=')) return true;
  if (u.includes('/jms/') && u.includes('login')) return true;
  if ((u.includes('mercadolivre') || u.includes('mercadolibre')) && u.includes('/login')) return true;
  return false;
}

/**
 * Marcadores fortes no HTML (tráfego suspeito / verificação). Seguro para `fetch` de listagem.
 * @param {string} html
 */
export function challengeHtmlHardBlocked(html) {
  if (!html || typeof html !== 'string') return false;
  const low = html.toLowerCase();
  if (low.includes('suspicious-traffic-frontend') || low.includes('/security/suspicious_traffic')) return true;
  if (low.includes('account-verification-main')) return true;
  if (low.includes('account-verification') && low.includes('negative_traffic')) return true;
  return false;
}

/**
 * Textos típicos de interstitial de login (falso positivo em páginas normais se usado sozinho).
 * @param {string} html
 */
export function challengeHtmlSoftAuthWall(html) {
  if (!html || typeof html !== 'string') return false;
  const h = html;
  if (h.includes('Olá! Para continuar') && h.includes('acesse sua conta')) return true;
  if (h.includes('Sou novo') && h.includes('Já tenho conta')) return true;
  return false;
}

/**
 * URL sugere fluxo de auth (combinar com soft wall, não com listagem limpa).
 * @param {string} url
 */
export function challengeUrlHintsAuthFlow(url) {
  const u = String(url || '').toLowerCase();
  if (!u) return false;
  return (
    u.includes('login') ||
    u.includes('registration') ||
    u.includes('jms/') ||
    u.includes('account-verification')
  );
}

/**
 * HTML de página (listagem ou PDP) — Node. Só marcadores duros + texto mole se URL já for de auth.
 * @param {string} html
 * @param {string} [pageUrl] URL final da página (evita falso positivo em lista.mercadolivre)
 */
export function challengeHtmlLooksBlocked(html, pageUrl = '') {
  if (challengeHtmlHardBlocked(html)) return true;
  if (challengeUrlHintsAuthFlow(pageUrl) && challengeHtmlSoftAuthWall(html)) return true;
  return false;
}

/**
 * @param {{ url?: string; html?: string }} p
 */
export function isMlAccountChallengePage(p) {
  const url = p.url ?? '';
  const html = p.html ?? '';
  if (challengeUrlLooksBlocked(url)) return true;
  return challengeHtmlLooksBlocked(html, url);
}

/**
 * Mensagem de erro para falha controlada (bulk / pipeline / métricas).
 * @param {string} context ex.: pdp, listagem
 */
export function mlAccountVerificationErrorMessage(context) {
  return `${ML_ACCOUNT_VERIFICATION_PREFIX}: Mercado Livre exige verificação de conta, login ou bloqueou o acesso (${context}).`;
}

/**
 * Função executada no browser via `page.evaluate` — autocontida.
 * Não usa só "Sou novo" em páginas que já têm listagem ou PDP carregado.
 * @returns {boolean}
 */
export function browserDetectMlAccountChallenge() {
  let u = '';
  try {
    u = String(location.href || '').toLowerCase();
    if (u.includes('account-verification')) return true;
    if (u.includes('gz/account-verification')) return true;
    if (u.includes('/registration')) return true;
    if (u.includes('registrationtype=')) return true;
    if (u.includes('/jms/') && u.includes('login')) return true;
    if ((u.includes('mercadolivre') || u.includes('mercadolibre')) && u.includes('/login')) return true;
  } catch {
    /* ignore */
  }
  try {
    const h = document.documentElement?.innerHTML || '';
    const low = h.toLowerCase();
    if (low.includes('suspicious-traffic-frontend') || low.includes('/security/suspicious_traffic')) return true;
    if (low.includes('account-verification-main')) return true;
    if (low.includes('account-verification') && low.includes('negative_traffic')) return true;

    const looksLikeListingOrPdp = !!document.querySelector(
      '.ui-search-result, .ui-search-layout, .poly-card, [class*="poly-card"], h1.ui-pdp-title, .ui-pdp-title, script[type="application/ld+json"]'
    );
    if (looksLikeListingOrPdp) return false;

    if (h.includes('Olá! Para continuar') && h.includes('acesse sua conta')) return true;
    const t = document.body?.innerText || '';
    if (t.includes('Sou novo') && t.includes('Já tenho conta')) return true;
  } catch {
    /* ignore */
  }
  return false;
}
