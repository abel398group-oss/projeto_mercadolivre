import { logChromePersistentProfileSummary } from './browser.js';
import { runFullCatalog } from './ml/mlCatalogRun.js';

logChromePersistentProfileSummary('[ml-catalog]');
await runFullCatalog();
