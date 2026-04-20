/**
 * Deteção de verificação de conta / login / bloqueio do Mercado Livre.
 * Usado em listagem e PDP — não altera parsing de produto.
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
 * HTML de página (listagem ou PDP).
 * @param {string} html
 */
export function challengeHtmlLooksBlocked(html) {
  if (!html || typeof html !== 'string') return false;
  const h = html;
  const low = h.toLowerCase();
  if (low.includes('suspicious-traffic-frontend') || low.includes('/security/suspicious_traffic')) return true;
  if (low.includes('account-verification-main')) return true;
  if (low.includes('account-verification') && low.includes('negative_traffic')) return true;
  if (h.includes('Olá! Para continuar') && h.includes('acesse sua conta')) return true;
  if (h.includes('Sou novo') && h.includes('Já tenho conta')) return true;
  return false;
}

/**
 * @param {{ url?: string; html?: string }} p
 */
export function isMlAccountChallengePage(p) {
  const url = p.url ?? '';
  const html = p.html ?? '';
  return challengeUrlLooksBlocked(url) || challengeHtmlLooksBlocked(html);
}

/**
 * Mensagem de erro para falha controlada (bulk / pipeline / métricas).
 * @param {string} context ex.: pdp, listagem
 */
export function mlAccountVerificationErrorMessage(context) {
  return `${ML_ACCOUNT_VERIFICATION_PREFIX}: Mercado Livre exige verificação de conta, login ou bloqueou o acesso (${context}).`;
}

/**
 * Função executada no browser via `page.evaluate` — tem de ser autocontida.
 * @returns {boolean}
 */
export function browserDetectMlAccountChallenge() {
  try {
    const u = String(location.href || '').toLowerCase();
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
    if (h.includes('Olá! Para continuar') && h.includes('acesse sua conta')) return true;
    const t = document.body?.innerText || '';
    if (t.includes('Sou novo') && t.includes('Já tenho conta')) return true;
  } catch {
    /* ignore */
  }
  return false;
}
