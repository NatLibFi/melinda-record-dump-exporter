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

import {readEnvironmentVariable} from '@natlibfi/melinda-backend-commons';

export const logLevel = readEnvironmentVariable('LOG_LEVEL', {defaultValue: 'info'});
export const dumpDirectory = readEnvironmentVariable('DUMP_DIRECTORY', {defaultValue: 'dump'});
// 10 megabytes
export const maxFileSize = readEnvironmentVariable('MAX_FILE_SIZE', {defaultValue: 10000000, format: v => Number(v)});

export const stateInterfaceOptions = {
  db: {
    host: readEnvironmentVariable('DATABASE_HOST', {defaultValue: 'localhost'}),
    port: readEnvironmentVariable('DATABASE_PORT', {defaultValue: 3306, format: v => Number(v)}),
    connectionLimit: readEnvironmentVariable('DATABASE_CONNECTION_LIMIT', {defaultValue: 5, format: v => Number(v)}),
    database: readEnvironmentVariable('DATABASE_NAME'),
    username: readEnvironmentVariable('DATABASE_USERNAME'),
    password: readEnvironmentVariable('DATABASE_PASSWORD')
  }
};
