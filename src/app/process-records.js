
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

import {zip} from 'compressing';
import {MARCXML} from '@natlibfi/marc-record-serializers';
import {MarcRecord} from '@natlibfi/marc-record';

export default ({logger, dbPool, maxFileSize}) => {
  return processRecords;

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
};
