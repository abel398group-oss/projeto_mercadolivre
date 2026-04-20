# Fixtures de teste (HTML congelado)

Coloca aqui HTML real ou mínimo reproduzível **sem** alterar código de produção.

| Pasta | Uso |
|-------|-----|
| `lista/` | Listagem (`extractMlbIdsFromListaHtml`, `extractListaCategoryPaths`, bloqueio) |
| `pdp/` | PDP (`buildEmbeddedPartialFromHtml` e exports derivados) |

**Novos cenários:** adiciona `*.html` com nome descritivo e um bloco `test()` correspondente em `test/scraper.fixtures.spec.js`.

**Nota:** `extractListaCategoryPaths` só captura segmentos `[a-z0-9]` (sem hífen) nos paths de `lista.mercadolivre.com.br` — alinha as fixtures a isso ou ajusta o teste ao comportamento real do parser.

Rodar: `npm run test:fixtures`
