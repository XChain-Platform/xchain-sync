// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Covers hash fixtures and hooks. One part of hash_continuity.test.js.
const HashVerifier = require('../../../../../src/client/hash_verifier');
const hashes = { ledger_hash: 'aaa', actions_hash: 'bbb', contract_hash: 'ccc' };

function registerHooks(setVerifier){
    beforeEach(function(){
        setVerifier(new HashVerifier());
    });
}

module.exports = { hashes, registerHooks };
