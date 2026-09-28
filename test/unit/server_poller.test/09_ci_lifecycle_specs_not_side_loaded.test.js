// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

const assert = require('assert');
const fs = require('node:fs');
const path = require('node:path');

describe('ServerPoller CI suite selection', function(){
    const repoRoot = path.resolve(__dirname, '../../..');

    it('keeps CI lifecycle detection out of the production poller module', function(){
        const source = fs.readFileSync(path.join(repoRoot, 'src/server/poller.js'), 'utf8');

        assert.doesNotMatch(source, /npm_lifecycle_event/);
        assert.doesNotMatch(source, /npmLifecycleEventFromEnv/);
    });

    it('selects the safety suites explicitly in the CI script', function(){
        const source = fs.readFileSync(path.join(repoRoot, 'package.json'), 'utf8');
        const packageJson = JSON.parse(source);

        assert.ok(packageJson.scripts.ci.includes('test/chaos/source_tip_read_failclosed.test.js'));
        assert.ok(packageJson.scripts.ci.includes('test/boundary/consensus_constants.test.js'));
    });
});
