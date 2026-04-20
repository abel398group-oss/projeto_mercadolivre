/**
 * Carrega a aba PDP com uma página ML antes do primeiro `goto` do produto
 * (evita ficar em about:blank enquanto a fila ainda está vazia ou após reciclar).
 * Não altera extração de produto — só navegação.
 */

import { config } from '../config.js';
import { sleep } from '../util.js';

/**
 * @param {import('puppeteer').Page} page
 * @param {string} logPrefix ex.: [pipeline] ou [ml-pdp-bulk]
 */
export async function warmPdpIdleTab(page, logPrefix) {
  let warm = String(config.mlPdpWarmupUrl || '').trim();
  if (!warm || warm === '0' || warm.toLowerCase() === 'false') {
    warm = 'https://www.mercadolivre.com.br/';
  }
  try {
    console.info(`${logPrefix} a carregar aba (${warm}) — não é PDP ainda; primeiro item usará esta sessão`);
    await page.goto(warm, { waitUntil: 'domcontentloaded', timeout: 60_000 });
    const d = Math.max(0, config.mlPdpWarmupDelayMs);
    if (d > 0) await sleep(d);
  } catch (e) {
    console.warn(`${logPrefix} aquecimento da aba falhou:`, e instanceof Error ? e.message : e);
  }
}
