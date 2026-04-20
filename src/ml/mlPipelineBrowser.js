/**
 * Browser Puppeteer partilhado entre produtor (listagem) e consumidor (PDP) no `npm run pipeline`.
 * Evita dois processos Chrome no mesmo USER_DATA_DIR e permite cookies/sessão numa só instância.
 */

/** @type {import('puppeteer').Browser | null} */
let sharedBrowser = null;

/** @param {import('puppeteer').Browser | null} b */
export function setPipelineSharedBrowser(b) {
  sharedBrowser = b;
}

export function getPipelineSharedBrowser() {
  return sharedBrowser;
}

export function clearPipelineSharedBrowser() {
  sharedBrowser = null;
}

/** @param {import('puppeteer').Browser | null | undefined} b */
export function isPipelineSharedBrowser(b) {
  return b != null && sharedBrowser != null && b === sharedBrowser;
}
