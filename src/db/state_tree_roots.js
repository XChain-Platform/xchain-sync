/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * The state_tree_roots table: one committed state root per chain, network and
 * block, which the replica compares against what it rebuilds.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/



module.exports = {

    // Light-client state-commitment roots for a block (SPV spec sec.4-5). Returns
    // { balances_root, stakes_root, state_root, block_merkle_root } or null. Used by
    // the follower's incremental SMT (reads block-1's balances_root) and by
    // ServerPoller to attach the committed roots to the outgoing block payload.
    //
    // STRICT (M-17): this is a consensus-input read for computeFollowerRoots, which
    // calls it with no conn. Fail-soft doQuery answers [] on a non-transactional
    // query fault, which this method turns into null and the follower reads as "no
    // prior row" - a wrong answer, not an error signal, sending it into a silent
    // full rebuild instead of a halt. ServerPoller always passes a conn, and doQuery
    // returns conn.query() before its own catch, so this is a no-op for that caller.
    async getStateRootsRow(chain, network, block_index, conn){
        let rows = await this.doQueryStrict(
            `SELECT balances_root, stakes_root, state_root, block_merkle_root
             FROM state_tree_roots
             WHERE chain=? AND network=? AND block_index=? LIMIT 1`,
            [chain, network, block_index], conn);
        return rows.length ? rows[0] : null;
    },

};
