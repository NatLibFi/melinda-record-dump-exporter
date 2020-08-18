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

import chai from 'chai';
import moment from 'moment';
import {readdirSync} from 'fs';
import {join as joinPath} from 'path';
import fixtureFactory, {READERS} from '@natlibfi/fixura';
import createConverter, {__RewireAPI__ as RewireAPI} from './convert';

describe('transform/convert', () => {
  const {expect} = chai;
  const fixturesPath = joinPath(__dirname, '..', '..', 'test-fixtures', 'transform', 'convert');

  beforeEach(() => {
    RewireAPI.__Rewire__('moment', () => moment('2020-01-01T00:00:00'));
  });

  afterEach(() => {
    RewireAPI.__ResetDependency__('moment');
  });


  readdirSync(fixturesPath).forEach(subDir => {
    it(subDir, () => {
      const {getFixture} = fixtureFactory({root: [fixturesPath, subDir], reader: READERS.JSON});
      const inputData = getFixture(['input.json']);
      const expectedRecord = getFixture(['output.json']);

      const convert = createConverter({
        harvestSource: 'FOOBAR',
        urnResolverUrl: 'http://foo.bar'
      });

      // Fixtures are lists so that they can be fed to the CLI when testing manually
      const record = convert(inputData[0]);
      expect(record.toObject()).to.eql(expectedRecord);
    });
  });
});
