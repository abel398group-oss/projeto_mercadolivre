/**
 * Grava ficheiro “latest” e, opcionalmente, cópia em output/history/… com timestamp.
 */

import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { config } from '../config.js';

/** @returns {string} YYYY-MM-DD_HH-mm-ss (hora local) */
export function formatSnapshotTimestamp(date = new Date()) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}_${pad(date.getHours())}-${pad(
    date.getMinutes()
  )}-${pad(date.getSeconds())}`;
}

/**
 * @param {{
 *   latestPath: string,
 *   historySubdir: 'catalog' | 'pdp' | 'debug' | 'metrics',
 *   historyBaseName: string,
 *   content: string,
 *   saveHistory?: boolean,
 * }} opts
 */
export async function writeSnapshot(opts) {
  const latestPath = path.resolve(opts.latestPath);
  const saveHist = opts.saveHistory ?? config.saveHistoryOutputs;
  await fsPromises.mkdir(path.dirname(latestPath), { recursive: true });
  await fsPromises.writeFile(latestPath, opts.content, 'utf8');
  if (!saveHist) return;
  const histDir = path.resolve(path.join(config.historyOutputRoot, opts.historySubdir));
  await fsPromises.mkdir(histDir, { recursive: true });
  const histPath = path.join(histDir, `${opts.historyBaseName}_${formatSnapshotTimestamp()}.json`);
  await fsPromises.writeFile(histPath, opts.content, 'utf8');
}

/**
 * @param {{
 *   latestPath: string,
 *   historySubdir: 'catalog' | 'pdp' | 'debug' | 'metrics',
 *   historyBaseName: string,
 *   content: string,
 *   saveHistory?: boolean,
 * }} opts
 */
export function writeSnapshotSync(opts) {
  const latestPath = path.resolve(opts.latestPath);
  const saveHist = opts.saveHistory ?? config.saveHistoryOutputs;
  fs.mkdirSync(path.dirname(latestPath), { recursive: true });
  fs.writeFileSync(latestPath, opts.content, 'utf8');
  if (!saveHist) return;
  const histDir = path.resolve(path.join(config.historyOutputRoot, opts.historySubdir));
  fs.mkdirSync(histDir, { recursive: true });
  const histPath = path.join(histDir, `${opts.historyBaseName}_${formatSnapshotTimestamp()}.json`);
  fs.writeFileSync(histPath, opts.content, 'utf8');
}
