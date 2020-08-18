
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

import {promises as fsPromises} from 'fs';
import {join as joinPath} from 'path';
import moment from 'moment';
import {statuses} from '@natlibfi/melinda-record-harvest-commons';

export default ({logger, dbPool, dumpDirectory, writeState}) => async () => {
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
};
