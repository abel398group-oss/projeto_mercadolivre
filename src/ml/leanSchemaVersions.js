/** Versões de schema dos JSON lean exportados (única fonte para builders e run_manifest). */

export const LEAN_SCHEMA_CATALOG = 'catalog_lean_v1';
export const LEAN_SCHEMA_PDP = 'pdp_lean_v1';
export const LEAN_SCHEMA_CORE = 'pdp_core_v1';
export const LEAN_SCHEMA_DEBUG = 'pdp_debug_lean_v1';

export const LEAN_SCHEMA_VERSIONS = {
  catalog: LEAN_SCHEMA_CATALOG,
  pdp: LEAN_SCHEMA_PDP,
  pdp_core: LEAN_SCHEMA_CORE,
  debug: LEAN_SCHEMA_DEBUG,
};
