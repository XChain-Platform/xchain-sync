'use strict';

// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

const { expect } = require('chai');
const path = require('path');

const pkg = require(path.join('..', '..', 'package.json'));
const lock = require(path.join('..', '..', 'package-lock.json'));

describe('eslint ajv override wiring', () => {
    it('pins eslint and @eslint/eslintrc to ajv 6 in package.json overrides', () => {
        expect(pkg.overrides.eslint).to.deep.equal({ ajv: '^6.14.0' });
        expect(pkg.overrides['@eslint/eslintrc']).to.deep.equal({ ajv: '^6.14.0' });
    });

    it('keeps the top-level ajv override on the 8.x line', () => {
        expect(pkg.overrides.ajv).to.match(/^\^8\./);
    });

    it('resolves eslint and @eslint/eslintrc to an ajv 6 in the lockfile', () => {
        const eslintAjv = lock.packages['node_modules/eslint/node_modules/ajv'];
        const eslintrcAjv = lock.packages['node_modules/@eslint/eslintrc/node_modules/ajv'];
        expect(eslintAjv, 'node_modules/eslint/node_modules/ajv missing from lockfile').to.exist;
        expect(eslintrcAjv, 'node_modules/@eslint/eslintrc/node_modules/ajv missing from lockfile').to.exist;
        expect(eslintAjv.version).to.match(/^6\./);
        expect(eslintrcAjv.version).to.match(/^6\./);
    });

    it('resolves the top-level ajv to the 8.x line in the lockfile', () => {
        const topAjv = lock.packages['node_modules/ajv'];
        expect(topAjv, 'node_modules/ajv missing from lockfile').to.exist;
        expect(topAjv.version).to.match(/^8\./);
    });

    it('carries the transitive packages ajv 6 needs', () => {
        expect(lock.packages['node_modules/uri-js'], 'node_modules/uri-js missing from lockfile').to.exist;
        expect(
            lock.packages['node_modules/fast-json-stable-stringify'],
            'node_modules/fast-json-stable-stringify missing from lockfile'
        ).to.exist;
    });
});
