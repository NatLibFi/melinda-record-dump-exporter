
/**
*
* @licstart  The following is the entire license notice for the JavaScript code in this file.
*
* Melinda record dump exporter microservice
*
* Copyright (C) 2020 University Of Helsinki (The National Library Of Finland)
*
* This file is part of melinda-record-dump-exporter
*
* melinda-record-dump-exporter program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* melinda-record-dump-exporter is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* @licend  The above is the entire license notice
* for the JavaScript code in this file.
*
*/

import moment from 'moment';
import {promises as fsPromises} from 'fs';
import {join as joinPath} from 'path';
import {zip} from 'compressing';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {MARCXML} from '@natlibfi/marc-record-serializers';
import {MarcRecord} from '@natlibfi/marc-record';
import createStateInterface, {statuses} from '@natlibfi/melinda-record-harvest-commons';

export default async ({dumpDirectory, logLevel, maxFileSize, stateInterfaceOptions}) => {
  const logger = createLogger(logLevel);

  logger.log('info', `Starting melinda-record-dump-exporter`);

  const {readState, getPool, close, writeState} = await createStateInterface(stateInterfaceOptions);
  const {status} = await readState();

  const dbPool = getPool();

  await initializeDatabase();

  if (status === statuses.harvestPending) {
    logger.info('Starting to process records');
    await processRecords();
    logger.info('No more records to process');
    return close();
  }

  if (status === statuses.harvestDone) {
    logger.info('Starting to process records');
    await processRecords(true);

    logger.info('All records processed. Finalizing...');
    await finalizeProcessing();
    return close();
  }

  logger.info('Nothing to do. Exiting.');
  return close();

  async function initializeDatabase() {
    const connection = await dbPool.getConnection();
    await connection.query(`CREATE TABLE IF NOT EXISTS packages (id MEDIUMINT NOT NULL AUTO_INCREMENT, data LONGBLOB NOT NULL, PRIMARY KEY (id))`);
    await connection.end();
  }

  async function processRecords(harvestDone = false) {
    const connection = await dbPool.getConnection();
    await connection.beginTransaction();

    logger.log('info', 'Finding records to package...');
    const identifiers = await packageRecords();

    if (identifiers) {
      logger.log('info', identifiers.length === 0 ? 'No records to package' : `Created package with ${identifiers.length} records`);

      await removeRecords();

      await connection.commit();
      await connection.end();

      return processRecords();
    }

    logger.log('info', 'Harvesting pending and not enough records yet available for a package.');

    await connection.rollback();
    await connection.end();

    async function packageRecords() {
      const compressStream = new zip.Stream();
      const identifiers = await addRecords();

      if (identifiers && identifiers.length > 0) {
        await insertPackage();
        return identifiers;
      }

      function insertPackage() {
        return new Promise((resolve, reject) => {
          const stream = compressStream.on('error', reject);
          logger.log('debug', 'Inserting package into database');
          resolve(connection.query('INSERT INTO packages (data) VALUE (?)', stream));
        });
      }

      function addRecords() {
        return new Promise((resolve, reject) => {
          const identifiers = [];
          let size = 0; // eslint-disable-line functional/no-let

          const emitter = connection.queryStream('SELECT * FROM records');

          emitter
            .on('error', reject)
            .on('end', () => {
              if (harvestDone) {
                resolve(identifiers);
                return;
              }

              resolve();
            })
            .on('data', async ({id, record}) => {
              try {
                const recordBuffer = await convertRecord(record);

                if (size + recordBuffer.length > maxFileSize) {
                  // this message is repeated: destroy doesn't work?
                  logger.log('debug', 'Maximum file size reached');
                  emitter.destroy();
                  resolve(identifiers);
                  return;
                }

                const prefix = id.toString().padStart('0', 9);

                compressStream.addEntry(recordBuffer, {relativePath: `${prefix}.xml`});
                identifiers.push(id); // eslint-disable-line functional/immutable-data

                size += recordBuffer.length;
              } catch (err) {
                reject(new Error(`Converting record ${id} to MARCXML failed: ${err}`));
              }

              async function convertRecord(record) {
                // Disable validation because we just to want harvest everything and not comment on validity
                const marcRecord = new MarcRecord(record, {fields: false, subfields: false, subfieldValues: false});
                const str = await MARCXML.to(marcRecord, {indent: true});
                return Buffer.from(str);
              }
            });
        });
      }
    }

    async function removeRecords() {
      await connection.batch('DELETE FROM records WHERE id=?', identifiers.map(i => [i]));
    }
  }

  async function finalizeProcessing() { // eslint-disable-line no-empty-function
    const {readdir, unlink, writeFile} = fsPromises;

    logger.log('info', 'Removing old packages and exporting new');

    await clearOldFiles();
    await clearIncomplete();
    await exportPackages();
    await writeState({status: statuses.postProcessingDone});

    async function clearOldFiles() {
      const filenames = await readdir(dumpDirectory);
      const oldPrefix = getOldPrefix();

      if (oldPrefix) {
        await removeOldFiles(filenames);
        return;
      }

      logger.log('debug', 'No old packages to remove');

      function getOldPrefix() {
        const prefixes = filenames.map(v => v.split('-'));
        return prefixes.length > 1 ? prefixes.slice(-1)[0] : undefined;
      }

      async function removeOldFiles(filenames, count = 0) {
        const [filename] = filenames;

        if (filename) {
          if (filename.startsWith(oldPrefix)) {
            await unlink(joinPath(dumpDirectory, filename));
            return removeOldFiles(filenames.slice(1), count + 1);
          }

          return removeOldFiles(filenames.slice(1), count);
        }

        logger.log('debug', `Removed ${count} old packages`);
      }
    }

    async function clearIncomplete() {
      const identifiers = await getPackageIdentifiers();
      const removableFilenames = await getRemovableFilenames();
      return remove(removableFilenames);

      async function remove(filenames) {
        const [filename] = filenames;

        if (filename) {
          logger.log('info', `Removing incomplete package ${filename}`);
          await unlink(joinPath(dumpDirectory, filename));
          return remove(filenames.slice(1));
        }
      }

      async function getRemovableFilenames() {
        const filenames = await readdir(dumpDirectory);

        return filenames.filter(filename => {
          const [, identifier] = filename.match(/-(?<def>[0-9]+)\.zip$/u);
          return identifiers.includes(identifier);
        });
      }

      async function getPackageIdentifiers() {
        const connection = await dbPool.getConnection();
        const results = await connection.query('SELECT id FROM packages');
        await connection.close();
        return results.map(({id}) => id);
      }
    }

    async function exportPackages() {
      const prefix = await getPrefix();
      const connection = await dbPool.getConnection();

      return new Promise((resolve, reject) => {
        const promises = [];

        connection.queryStream('SELECT * FROM packages')
          .on('error', reject)
          .on('end', async () => {
            try {
              await Promise.all(promises);
              await connection.close();
              resolve();
            } catch (err) {
              reject(err);
            }
          })
          .on('data', ({id, data}) => {
            promises.push(processPackage()); // eslint-disable-line functional/immutable-data

            async function processPackage() {
              const suffix = id.toString().padStart(9, '0');
              const filename = `${prefix}-${suffix}.zip`;

              logger.log('info', `Writing package ${filename}`);
              await writeFile(joinPath(dumpDirectory, filename), data);
              await connection.query('DELETE FROM packages WHERE id=?', [id]);
            }
          });
      });

      async function getPrefix() {
        const filenames = await readdir(dumpDirectory);
        return filenames.length === 0 ? moment().format('YYYYMMDDTHHMMSS') : filenames[0].split('-')[0];
      }
    }
  }
};
