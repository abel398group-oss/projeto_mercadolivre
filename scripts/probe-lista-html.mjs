const headers = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
};
const url = process.argv[2] || 'https://lista.mercadolivre.com.br/beleza';
const r = await fetch(url, { headers, redirect: 'follow' });
const t = await r.text();
console.log('status', r.status, 'len', t.length);
console.log('has /p/MLB', /\/p\/MLB\d+/.test(t));
console.log('suspicious-traffic', t.includes('suspicious-traffic'));
console.log('__PRELOADED_STATE__', t.includes('__PRELOADED_STATE__'));
console.log('ItemList', t.includes('ItemList'));
console.log('ui-search-result', t.includes('ui-search-result'));
console.log('poly-component', t.includes('poly-component'));
console.log('andes-card', t.includes('andes-card--padding'));
