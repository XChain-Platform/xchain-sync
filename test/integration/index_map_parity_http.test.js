// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.
//
// END-TO-END advisory index-map parity over real HTTP. A real Express server
// publishes index_map_checksum on /status (computed with the SHIPPED
// BlockHasher.computeIndexMapChecksum over a real source DB), and the SHIPPED
// ClientSync.verifyAgainstSource fetches /status over HTTP, recomputes over a
// real replica DB, and compares. Exercises the full transport + client consume
// path the pure-DB drill (index-map-parity.test.js) could not.
//
// The /status handler mirrors api.js buildStatusRow's server-mode shape (the
// fields the client reads: block_height, the three hashes, table_counts, and the
// index_map_checksum), using the exact shipped checksum call. buildStatusRow is
// not exported, so it is mirrored rather than imported; the CLIENT side here is
// 100% shipped code.

const assert       = require('assert');
const {
    PORT, H, STAMPED, stamp, registerIndexMapParityHttpHooks
} = require('./index_map_parity_http.test/helpers/index_map_parity_http_suite');

describe('Integration: index-map parity over HTTP (e2e)', function() {
    this.timeout(180000);

    const countKey = 'index_map_mismatch_count:indexer';
    const lastKey  = 'index_map_mismatch_last_block:indexer';
    const suite = registerIndexMapParityHttpHooks();

    it('faithful replica: /status checksum matches, no mismatch, no counter', async function() {
        await stamp(suite.replicaDb, STAMPED);                       // ensure faithful
        await suite.clientSync.verifyAgainstSource('http://127.0.0.1:' + PORT, H);
        assert.strictEqual(suite.sawParityWarn(), false, 'no parity warning on a faithful replica');
        let c = await suite.realReplica.getSyncState(countKey);
        assert.strictEqual(c, null, 'counter unset when everything agrees');
    });

    it('divergence: equal-count swapped identity fires advisory mismatch, no halt', async function() {
        // Replica id=1002 locally points elsewhere; SAME row count, different content.
        await suite.realReplica.doQuery("UPDATE index_addresses SET address=? WHERE id=1002",
            ['bc1qstamped0MALLORY00000000000000000mmmmmmm']);

        await suite.clientSync.verifyAgainstSource('http://127.0.0.1:' + PORT, H);

        assert.strictEqual(suite.sawParityWarn(), true, 'parity mismatch must be logged');
        assert.strictEqual(suite.clientSync.isHalted(), false, 'advisory: client must NOT halt');
        let c  = await suite.realReplica.getSyncState(countKey);
        let lb = await suite.realReplica.getSyncState(lastKey);
        assert.strictEqual(c, '1', 'mismatch counter incremented');
        assert.strictEqual(lb, String(H), 'last divergent block recorded');
    });

    it('recovery: faithful again raises no new mismatch (counter steady)', async function() {
        await suite.realReplica.doQuery("UPDATE index_addresses SET address=? WHERE id=1002",
            [STAMPED[1].address]);                              // restore
        // Add a benign NULL-block row the source lacks (API read-path seed): excluded.
        await suite.realReplica.doQuery("INSERT INTO index_addresses (`id`,`address`,`block_index`) VALUES (?,?,NULL)",
            [2001, 'bc1qapiseed00000000000000000000000000seed1']);

        await suite.clientSync.verifyAgainstSource('http://127.0.0.1:' + PORT, H);

        assert.strictEqual(suite.sawParityWarn(), false, 'no false alarm once faithful (NULL-block excluded)');
        let c = await suite.realReplica.getSyncState(countKey);
        assert.strictEqual(c, '1', 'counter unchanged from the single real divergence');
    });
});
