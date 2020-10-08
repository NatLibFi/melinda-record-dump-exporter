
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

import {ZipFile} from 'yazl';
import {MARCXML} from '@natlibfi/marc-record-serializers';
import {MarcRecord} from '@natlibfi/marc-record';

export default ({logger, dbPool, maxFileSize, packagingReportLimit}) => {
  return processRecords;

  async function processRecords(harvestDone = false) {
    
    const connection = await dbPool.getConnection();
    const zipFile = new ZipFile();

    try {
      await connection.beginTransaction();

      zipFile.outputStream.pipe();

    } catch (err) {
      await connection.rollback();
    }
    

    await connection.beginTransaction();

    zipFile.outputStream.pipe

    const addedIdentifiers = await addToPackage() || [];

    if (addedIdentifiers.length > 0) {
      await connection.beginTransaction();

      await createPackage();
      await removeRecords();

      await connection.commit();
      await connection.end();

      logger.log('info', `Created package with ${addedIdentifiers.length} records`);
      return processRecords();
    }

    logger.log('info', 'Not enough records for a package.');
    await connection.end();

    async function addToPackage({cursor, identifiers = [], currentSize = 0} = {}) {
      const records = await getRecords();

      if (records.length > 0) {
        const [{id: lastId}] = records.slice(-1);
        const {newIdentifiers, newSize, isDone} = await iterate({records, currentSize, identifiers});

        if (isDone) {
          return identifiers.concat(newIdentifiers);
        }

        return addToPackage({
          cursor: lastId,
          identifiers: identifiers.concat(newIdentifiers),
          currentSize: currentSize + newSize
        });
      }

      return harvestDone ? identifiers : undefined;

      function getRecords() {
        if (cursor) {
          return connection.query('SELECT * FROM records WHERE id > ? ORDER BY id ASC LIMIT 1000', [cursor]);
        }

        return connection.query('SELECT * FROM records ORDER BY id ASC LIMIT 1000');
      }

      async function iterate({currentSize, records = [], identifiers = [], lastReportedSize = 0}) {
        const [record] = records;

        if (record) {
          const recordBuffer = await convertRecord();

          if (recordBuffer.length + currentSize > maxFileSize) {
            logger.log('debug', `Max filesize reached: ${recordBuffer.length + currentSize}`);

            return {
              newIdentifiers: identifiers.concat(record.id),
              isDone: true
            };
          }

          const prefix = record.id.toString().padStart('0', 9);
          compressStream.addEntry(recordBuffer, {relativePath: `${prefix}.xml`});

          const newSize = currentSize + recordBuffer.length;
          const newIdentifiers = identifiers.concat(record.id);

          return iterate({
            records: records.slice(1),
            lastId: record.id,
            currentSize: newSize,
            identifiers: newIdentifiers,
            lastReportedSize: reportSizeChange(newSize, newIdentifiers)
          });
        }

        return {newIdentifiers: identifiers, newSize: currentSize};

        function reportSizeChange(currentSize, identifiers) {
          if (Math.abs(lastReportedSize - currentSize) > packagingReportLimit) {
            logger.log('info', `Package size is now ${currentSize} bytes. Number of records is ${identifiers.length}`);
            return currentSize;
          }

          return lastReportedSize;
        }

        async function convertRecord() {
          try {
            // Disable validation because we just to want harvest everything and not comment on validity
            const marcRecord = new MarcRecord(record.record, {fields: false, subfields: false, subfieldValues: false});
            const str = await MARCXML.to(marcRecord, {indent: true});
            return Buffer.from(str);
          } catch (err) {
            throw new Error(`Converting record ${record.id} to MARCXML failed: ${err}`);
          }
        }
      }
    }

    async function removeRecords() { // eslint-disable-line no-unused-vars
      await connection.batch('DELETE FROM records WHERE id=?', addedIdentifiers.map(i => [i]));
    }

    function createPackage() {
      return new Promise((resolve, reject) => {
        const stream = compressStream.on('error', ({stack}) => {
          reject(new Error(`Compression error: ${stack}`));
        });

        logger.log('debug', 'Inserting package into database');
        resolve(connection.query('INSERT INTO packages (data) VALUE (?)', stream));
      });
    }
  }
};
