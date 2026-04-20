import fs from 'node:fs/promises';
import path from 'node:path';

/**
 * Anexa uma linha JSON ao ficheiro (JSONL). Cria diretório se necessário.
 * @param {string} filePath
 * @param {unknown} record
 */
export async function appendJsonlLine(filePath, record) {
  const raw = String(filePath || '').trim();
  if (!raw) return;
  const p = path.resolve(raw);
  await fs.mkdir(path.dirname(p), { recursive: true });
  await fs.appendFile(p, `${JSON.stringify(record)}\n`, 'utf8');
}
