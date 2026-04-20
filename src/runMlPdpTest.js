import fs from 'node:fs/promises';
import path from 'node:path';
import { launchBrowser, logChromePersistentProfileSummary } from './browser.js';
import { config } from './config.js';
import { scrapeMlPdp } from './ml/mlPdpScrape.js';

const args = process.argv.slice(2).filter((a) => a !== '--keep-open');
const keepBrowserOpen =
  process.argv.includes('--keep-open') || Boolean(config.keepBrowserOpen);

const url = (args[0] || process.env.ML_PDP_URL || '').trim();
if (!url) {
  console.error('Define ML_PDP_URL no .env ou passa a URL:');
  console.error('  npm start -- "https://www.mercadolivre.com.br/.../up/MLBU..."');
  console.error('  npm run start:open -- "https://..."   (mantém o Chrome aberto)');
  process.exit(1);
}

console.warn(
  '[ml-pdp] Modo: um anúncio por execução (só esta URL). Para PDP de todos os itens: npm run catalog → npm run catalog:pdp'
);

logChromePersistentProfileSummary('[ml-pdp]');

/** @type {import('puppeteer').Browser | undefined} */
let browser;

if (keepBrowserOpen) {
  ({ browser } = await launchBrowser());
}

try {
  const result = await scrapeMlPdp(url, { browser, keepBrowserOpen });
  console.log(JSON.stringify(result, null, 2));

  const outFile = String(config.mlPdpOutput || '').trim();
  if (outFile) {
    const outPath = path.resolve(outFile);
    await fs.mkdir(path.dirname(outPath), { recursive: true });
    const json = config.mlPdpPretty ? JSON.stringify(result, null, 2) : JSON.stringify(result);
    await fs.writeFile(outPath, json, 'utf8');
    console.info(`[ml-pdp] ficheiro: ${outPath}`);
  }

  if (keepBrowserOpen) {
    console.info('');
    console.info('──');
    console.info('Scrape concluído — o JSON em cima está completo.');
    console.info(
      'O terminal parece “parado” de propósito: o Node só termina quando FECHARES a janela do Chrome desta sessão.'
    );
    console.info('Abaixo: Chrome voltou ao topo da página para não ficares na zona de recomendações.');
    console.info('Para o script fechar o browser sozinho após imprimir o JSON: ML_KEEP_BROWSER_OPEN=false no .env');
    console.info('──');
  }
} catch (e) {
  const msg = e instanceof Error ? e.message : String(e);
  console.error(msg);
  if (/login|verifica|bloqueio/i.test(msg)) {
    console.error('\nSe o Chrome fechou rápido demais: npm run start:open -- "' + url + '"');
  }
  process.exitCode = 1;
} finally {
  if (keepBrowserOpen && browser) {
    console.info(
      '[ml-pdp] Aguardando: fecha a janela do Chrome (esta sessão Puppeteer) para o processo Node terminar.'
    );
    await new Promise((resolve) => browser.once('disconnected', resolve));
  }
}
