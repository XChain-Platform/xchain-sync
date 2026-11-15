/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md.
 *
 **********************************************************************/
'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const CONFIGS = ['stryker.config.json', 'stryker.quick.config.json'];

describe('stryker report output location', () => {
  for (const name of CONFIGS) {
    describe(name, () => {
      const config = JSON.parse(fs.readFileSync(path.join(__dirname, name), 'utf8'));

      it('writes html and json reports under tmp/mutation', () => {
        const files = Object.values(config.htmlReporter || {})
          .concat(Object.values(config.jsonReporter || {}))
          .filter((v) => typeof v === 'string');
        assert.ok(files.length >= 2, 'expected html and json report file names');
        for (const f of files) assert.ok(f.startsWith('tmp/mutation/'), f);
      });

      it('keeps the incremental file under tmp/mutation', () => {
        assert.ok(config.incrementalFile.startsWith('tmp/mutation/'), config.incrementalFile);
      });

      it('does not reference the retired tracked directory', () => {
        assert.ok(!/reports[\\/]/.test(JSON.stringify(config)));
      });
    });
  }
});
