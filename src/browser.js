import path from 'node:path';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { config } from './config.js';
import { DEFAULT_CHROME_USER_AGENT } from './userAgents.js';

puppeteer.use(StealthPlugin());

/**
 * Log no arranque: caminhos absolutos do perfil persistente (PDP vs listagem).
 * Não altera comportamento; só documenta o que o Puppeteer vai usar.
 * @param {string} [tag] prefixo (ex.: `[pipeline]`)
 */
export function logChromePersistentProfileSummary(tag = '[browser]') {
  if (config.mlBrowserIncognito) {
    console.info(
      `${tag} ML_BROWSER_INCOGNITO=true — Chrome sem pasta de perfil persistente; USER_DATA_DIR é ignorado e a sessão não fica guardada entre corridas.`
    );
    return;
  }
  const cwd = process.cwd();
  const pdpAbs = path.resolve(cwd, config.userDataDir);
  const listaAbs = path.resolve(cwd, config.mlListaUserDataDir);
  console.info(`${tag} Sessão persistente — USER_DATA_DIR (PDP / bulk / API no browser): ${pdpAbs}`);
  console.info(`${tag} Sessão persistente — ML_LISTA_USER_DATA_DIR (listagem no browser): ${listaAbs}`);
  if (pdpAbs !== listaAbs) {
    console.warn(
      `${tag} Perfis diferentes: faz login no ML em cada pasta ou define ML_LISTA_USER_DATA_DIR igual a USER_DATA_DIR.`
    );
  } else {
    console.info(
      `${tag} Listagem e PDP usam o mesmo perfil. Faz login manualmente nessa pasta (ex.: corre uma vez com Chrome visível) antes do pipeline; evita dois Puppeteers a usar o mesmo perfil em simultâneo.`
    );
  }
}

/**
 * @param {{ userDataDir?: string } | undefined} [options]
 */
export async function launchBrowser(options = {}) {
  const incognito = Boolean(config.mlBrowserIncognito);
  const userDataDir = incognito ? undefined : options.userDataDir ?? config.userDataDir;
  const userAgent = config.browserUserAgent || DEFAULT_CHROME_USER_AGENT;

  /** @type {string[]} */
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--window-size=1366,768',
    `--user-agent=${userAgent}`,
    '--disable-dev-shm-usage',
  ];
  if (incognito) {
    args.push('--incognito');
    console.info(
      '[browser] ML_BROWSER_INCOGNITO=true — janela anónima, sem perfil persistente (cookies/sessão não são guardados entre execuções).'
    );
  }

  /** @type {import('puppeteer').LaunchOptions} */
  const launchOpts = {
    headless: config.headless,
    args,
  };
  if (userDataDir) {
    launchOpts.userDataDir = userDataDir;
  }

  const browser = await puppeteer.launch(launchOpts);

  return { browser, userAgent };
}
