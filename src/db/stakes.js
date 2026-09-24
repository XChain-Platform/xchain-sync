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
 * The stake-weight queries behind the stake-weighted quorum. Consensus-critical:
 * these build the stakes_root, so their SQL is a byte-for-byte twin of the
 * indexer's and is guarded by a cross-repo drift test.
 *
 * A Database mixin: every method below is installed on Database.prototype by
 * db/index.js, so `this` is the Database instance and call sites are unchanged.
 *
 ********************************************************************/

const path       = require('path');
const swqCap = require('../consensus/gates/swq_source_cap_gate');
const stakeWeightCollation = require('../consensus/gates/stake_weight_collation_gate');
const { requireStakeWeight } = require('./shared.js');
const { getLogger } = require('../observability');
const logger = getLogger();

module.exports = {

    // Light-client stakes_root support (SPV spec sec.4.1, BTC-only). Source-deduped
    // capability stake-weight query, ported VERBATIM from
    // xchain-indexer/src/db/stakes/effective_set_sql.js
    // stakeWeightsSql: it MUST produce a byte-identical SQL string + arg order or
    // the follower's stakes_root diverges from the indexer's committed root and the
    // state-commitment check false-halts. The cross-repo drift guard in
    // test/unit/rollback_coverage.test.js locks the two together. Reads only tables
    // xchain-sync replicates (stakes, delegations, stake_key_revocations,
    // capability_slash_events, index_addresses, index_pubkeys).
    stakeWeightsSql(valid_id, blockIndex, minStake){
        // Precision: DECIMAL(30,8) (22 integer digits, 8 fractional) is sufficient because the
        // staking tick is XCHAIN at 8 decimals and total supply stays far below 10^22; every
        // same-version node truncates identically, so the stake-weight tally is deterministic.
        // If a >8-decimal staking tick is ever introduced, widen these casts to
        // DECIMAL(60, <tick-decimals>) AND pin a consistent sql_mode fleet-wide (an overflow at
        // >22 integer digits is otherwise sql_mode-dependent) before that tick can stake.
        // Permanent disqualification - see _effectiveCapabilitySetSql. Excludes equivocation-
        // slashed keys from the effective-key set (both stake-key and delegated-key branches)
        // so the source-deduped stake-weight tally matches the count-quorum set exactly.
        const slashExcl = (keyCol) =>
            `AND NOT EXISTS (SELECT 1 FROM capability_slash_events cse
                             WHERE cse.signing_pubkey_id = ${keyCol} AND cse.block_index <= ?)`;
        let sql = `SELECT ip.pubkey AS pubkey,
                          sa.address AS source,
                          q.total    AS weight
                   FROM (
                       SELECT s.source_id AS source_id,
                              SUM(CAST(s.amount AS DECIMAL(30,8))) AS total
                       FROM stakes s
                       WHERE s.status_id = ?
                         AND s.activation_block <= ?
                         AND (s.deactivation_block IS NULL OR s.deactivation_block > ?)
                       GROUP BY s.source_id
                       HAVING total >= CAST(? AS DECIMAL(30,8))
                   ) q
                   JOIN index_addresses sa ON sa.id = q.source_id
                   JOIN (
                       SELECT s2.source_id AS source_id, s2.signing_pubkey_id AS pubkey_id
                       FROM stakes s2
                       WHERE s2.status_id = ?
                         AND s2.activation_block <= ?
                         AND (s2.deactivation_block IS NULL OR s2.deactivation_block > ?)
                         AND NOT EXISTS (
                             SELECT 1 FROM stake_key_revocations r
                             WHERE r.source_id = s2.source_id
                               AND r.signing_pubkey_id = s2.signing_pubkey_id
                               AND r.status_id = ?
                               AND r.deactivation_block <= ?
                               AND r.action_index > s2.action_index)
                         ${slashExcl('s2.signing_pubkey_id')}
                       GROUP BY s2.source_id, s2.signing_pubkey_id
                       UNION
                       SELECT d.source_id AS source_id, d.signing_pubkey_id AS pubkey_id
                       FROM delegations d
                       WHERE d.status_id = ?
                         AND d.activation_block <= ?
                         AND (d.deactivation_block IS NULL OR d.deactivation_block > ?)
                         ${slashExcl('d.signing_pubkey_id')}
                   ) ek ON ek.source_id = q.source_id
                   JOIN index_pubkeys ip ON ip.id = ek.pubkey_id`;
        let args = [valid_id, blockIndex, blockIndex, minStake,
                    valid_id, blockIndex, blockIndex, valid_id, blockIndex, blockIndex,
                    valid_id, blockIndex, blockIndex, blockIndex];
        return { sql, args };
    },

    // SWQ source-cap wrapper (SWQ-TRUNC-1 liveness half). Wraps an inner source-keyed
    // stake-weight builder ({sql,args} from stakeWeightsSql or the sync AsOf variant)
    // and replaces the raw key-row LIMIT with a windowed cap on the consensus UNIT:
    // DISTINCT staking SOURCES (DENSE_RANK over source) plus a per-source key bound
    // (ROW_NUMBER per source). One source can no longer fill the window and evict
    // honest sources. Over-fetches one extra source (_sr <= maxSources + 1) so the
    // caller can flag a genuinely >maxSources federation as truncated (the primitive
    // then fails closed); the per-source key cap only bounds the row/leaf count and
    // never sets truncated (dropping a source's excess keys does not change its
    // weight). Row order is consensus-irrelevant (the stakes_root SMT keys on
    // pubkey+capability); only the returned SET is. CONSENSUS-CRITICAL: feeds the
    // hashed stakes_root at/after SWQ_SOURCE_CAP_ACTIVATION and MUST stay byte-identical
    // to the xchain-indexer twin (cross-repo drift guard in rollback-coverage.test.js).
    //
    // `binCollation` (stake_weight_collation_activation.js) pins the ordering to a
    // binary collation. `source` and `pubkey` resolve through index_addresses.address
    // and index_pubkeys.pubkey, both declared utf8_general_ci (folding), and every
    // other consensus read of those columns already pins utf8_bin. Order is a
    // consensus quantity HERE and only here: the two window ranks are what the caps
    // truncate on, so the collation decides which sources and which keys survive into
    // the hashed stakes_root. Below the height the emitted SQL is byte-identical to
    // what shipped before the gate; the suffix is '' and concatenates away.
    cappedStakeWeightsSql(inner, maxSources, maxKeys, binCollation){
        let c = stakeWeightCollation.stakeWeightCollate(binCollation);
        let sql = `SELECT r.pubkey AS pubkey, r.source AS source, r.weight AS weight, r._sr AS _sr
                   FROM (
                       SELECT b.pubkey AS pubkey, b.source AS source, b.weight AS weight,
                              DENSE_RANK() OVER (ORDER BY b.source${c})                        AS _sr,
                              ROW_NUMBER() OVER (PARTITION BY b.source${c} ORDER BY b.pubkey${c})  AS _kr
                       FROM (${inner.sql}) b
                   ) r
                   WHERE r._sr <= ? AND r._kr <= ?
                   ORDER BY r.source${c}, r.pubkey${c}`;
        let args = [...inner.args, maxSources + 1, maxKeys];
        return { sql, args };
    },

    // Apply the cap regime in force for `coin`/`network` at `blockIndex` to an inner
    // source-keyed stake-weight builder, returning { rows:[{pubkey,source,weight}],
    // truncated }. Twin of the indexer's stakeWeightsWithCap gate: at/after
    // SWQ_SOURCE_CAP_ACTIVATION the windowed source-cap (cappedStakeWeightsSql);
    // below it the legacy uncapped key-row LIMIT. The gate + caps + cappedStakeWeightsSql
    // are byte-mirrored to the indexer so the follower's stakes_root set is identical on
    // both sides of the height. Sync reads coin/network from the caller (it has no
    // per-chain config); a null coin/network stays inert (legacy uncapped path).
    async applyStakeWeightCap(inner, blockIndex, limit, coin, network, label){
        // Ordering collation for BOTH regimes (stake_weight_collation_activation.js);
        // the legacy LIMIT branch truncates on the same order the capped branch ranks on.
        // A null coin/network stays inert here exactly as it does for the source cap.
        let binCollation = stakeWeightCollation.isStakeWeightBinCollationActive(blockIndex, network, coin);
        let swc = stakeWeightCollation.stakeWeightCollate(binCollation);
        if(swqCap.isSwqSourceCapActive(blockIndex, network, coin)){
            let maxSources = swqCap.STAKE_WEIGHT_MAX_SOURCES;
            let maxKeys    = swqCap.STAKE_WEIGHT_MAX_KEYS_PER_SOURCE;
            let capped = this.cappedStakeWeightsSql(inner, maxSources, maxKeys, binCollation);
            // Strict for the M-17 reason getBlockLeafRows is: this row set IS the
            // stakes_root, and the SPV checkpoint forward-follow
            // (ClientSync.oraclePublishSetAt) reads it with NO transaction open, so a
            // swallowed error here would commit an empty stake set over a populated
            // stakes table. Only the execution wrapper changes; the SQL builders stay
            // byte-mirrored to the indexer.
            let raw = await this.doQueryStrict(capped.sql, capped.args);
            let truncated = raw.some(r => Number(r._sr) > maxSources);
            if(truncated)
                logger.warn(label + ' saw more than ' + maxSources + ' distinct staking sources at block ' + blockIndex + ' - stakes_root snapshot truncated; stake-weighted quorum fails closed. Raise STAKE_WEIGHT_MAX_SOURCES (coordinated flag-day upgrade) if the federation has grown.');
            let rows = (truncated ? raw.filter(r => Number(r._sr) <= maxSources) : raw).map(r => ({
                pubkey: String(r.pubkey),
                source: String(r.source),
                weight: requireStakeWeight(r.weight, label)
            }));
            return { rows, truncated };
        }
        let query = `${inner.sql} ORDER BY source${swc}, pubkey${swc} LIMIT ?`;
        let raw = await this.doQueryStrict(query, [...inner.args, limit]);
        let truncated = raw.length >= limit;
        if(truncated)
            logger.warn(label + ' hit the result cap of ' + limit + ' rows at block ' + blockIndex + ' - stakes_root set may be truncated vs the source. Raise the frozen VALIDATOR_QUERY_LIMIT (coordinated fleet upgrade) if the federation has grown.');
        let rows = raw.map(r => ({
            pubkey: String(r.pubkey),
            source: String(r.source),
            weight: requireStakeWeight(r.weight, label)
        }));
        return { rows, truncated };
    },

    // Source-keyed capability stake weights at a block, mirroring the indexer's
    // getStakeWeightsByCapability BTC path (the follower only builds the BTC
    // stakes_root, so the off-BTC hub-mirror branch is not needed here). minStake
    // is the capability's frozen MIN_STAKE floor; limit is the frozen
    // VALIDATOR_QUERY_LIMIT. Same ORDER BY + cap as the source so the selected set
    // is identical even on truncation.
    async getStakeWeightsByCapability(capability, blockIndex, minStake, limit, coin, network){
        // rethrow: a null here would silently return the empty stake set (M-17).
        let valid_id = await this.getStatusId('valid', { rethrow: true });
        if(valid_id === null) return [];
        let sw = this.stakeWeightsSql(valid_id, blockIndex, String(minStake));
        let { rows } = await this.applyStakeWeightCap(sw, blockIndex, limit, coin, network, 'getStakeWeightsByCapability(' + capability + ')');
        return rows;
    },

    // HISTORICAL stake weights at snapshotBlock S, reconstructing the amount that
    // stakes_root[S] committed IN ORDER, for the SPV checkpoint forward-follow
    // (ClientSync.oraclePublishSetAt / followCheckpointForward). getStakeWeightsByCapability
    // reads live SUM(stakes.amount), but a SLASH zeroes stakes.amount IN PLACE, so a
    // query for a past S run at the current tip understates the weight committed at S
    // (and via HAVING can false-drop a source below the floor -> false-halt on a
    // legitimate rotation). This method adds back capability_slash_debits whose slash
    // block_index > S (target_table='stakes' only; unstakes debits do not feed the
    // stakes weight), restoring each row's pre-slash amount: 0 + prev_amount = prev_amount.
    //
    // SYNC-ONLY / forward-follow-only. MUST NOT be added to or called from xchain-indexer:
    // the indexer's action handlers (anchor/xcall/xexec/cross_settle) and the in-order
    // stakes_root build call getStakeWeightsByCapability while S is the tip, BEFORE any
    // later slash mutates the row, so they already read the correct value; adding the
    // add-back there would double-count and fork the committed ledger. This is a NO-OP
    // when no slash with block_index > S exists (addback is NULL -> COALESCE 0), so it
    // returns a result byte-identical to getStakeWeightsByCapability(cap, S) computed
    // in order at S. stakeWeightsSql (the cross-repo byte-identical twin) is deliberately
    // NOT reused/modified here so the drift guard and the consensus query stay untouched.
    async getStakeWeightsByCapabilityAsOf(capability, snapshotBlock, minStake, limit, coin, network){
        // rethrow for the same M-17 reason, and this is the caller that runs with NO
        // transaction open (ClientSync.oraclePublishSetAt).
        let valid_id = await this.getStatusId('valid', { rethrow: true });
        if(valid_id === null) return [];
        // Membership exclusion is identical to stakeWeightsSql: a key slashed at
        // block > S has cse.block_index > S, so NOT EXISTS is TRUE and the key is
        // correctly KEPT in the set at S. Only the q-subquery AMOUNT is reconstructed.
        const slashExcl = (keyCol) =>
            `AND NOT EXISTS (SELECT 1 FROM capability_slash_events cse
                             WHERE cse.signing_pubkey_id = ${keyCol} AND cse.block_index <= ?)`;
        let sql = `SELECT ip.pubkey AS pubkey,
                          sa.address AS source,
                          q.total    AS weight
                   FROM (
                       SELECT s.source_id AS source_id,
                              SUM(CAST(s.amount AS DECIMAL(30,8))
                                  + COALESCE(CAST(addback.amt AS DECIMAL(30,8)), 0)) AS total
                       FROM stakes s
                       LEFT JOIN (
                           SELECT csd.stake_action_index AS stake_action_index,
                                  SUM(CAST(csd.amount AS DECIMAL(30,8))) AS amt
                           FROM capability_slash_debits csd
                           WHERE csd.target_table = 'stakes' AND csd.block_index > ?
                           GROUP BY csd.stake_action_index
                       ) addback ON addback.stake_action_index = s.action_index
                       WHERE s.status_id = ?
                         AND s.activation_block <= ?
                         AND (s.deactivation_block IS NULL OR s.deactivation_block > ?)
                       GROUP BY s.source_id
                       HAVING total >= CAST(? AS DECIMAL(30,8))
                   ) q
                   JOIN index_addresses sa ON sa.id = q.source_id
                   JOIN (
                       SELECT s2.source_id AS source_id, s2.signing_pubkey_id AS pubkey_id
                       FROM stakes s2
                       WHERE s2.status_id = ?
                         AND s2.activation_block <= ?
                         AND (s2.deactivation_block IS NULL OR s2.deactivation_block > ?)
                         AND NOT EXISTS (
                             SELECT 1 FROM stake_key_revocations r
                             WHERE r.source_id = s2.source_id
                               AND r.signing_pubkey_id = s2.signing_pubkey_id
                               AND r.status_id = ?
                               AND r.deactivation_block <= ?
                               AND r.action_index > s2.action_index)
                         ${slashExcl('s2.signing_pubkey_id')}
                       GROUP BY s2.source_id, s2.signing_pubkey_id
                       UNION
                       SELECT d.source_id AS source_id, d.signing_pubkey_id AS pubkey_id
                       FROM delegations d
                       WHERE d.status_id = ?
                         AND d.activation_block <= ?
                         AND (d.deactivation_block IS NULL OR d.deactivation_block > ?)
                         ${slashExcl('d.signing_pubkey_id')}
                   ) ek ON ek.source_id = q.source_id
                   JOIN index_pubkeys ip ON ip.id = ek.pubkey_id`;
        // Arg order tracks the placeholders left-to-right: the addback block bound
        // first, then the same sequence stakeWeightsSql uses (all historical-block
        // args bound to snapshotBlock), then the LIMIT.
        let args = [snapshotBlock,
                    valid_id, snapshotBlock, snapshotBlock, String(minStake),
                    valid_id, snapshotBlock, snapshotBlock, valid_id, snapshotBlock, snapshotBlock,
                    valid_id, snapshotBlock, snapshotBlock, snapshotBlock];
        let { rows } = await this.applyStakeWeightCap({ sql, args }, snapshotBlock, limit, coin, network, 'getStakeWeightsByCapabilityAsOf(' + capability + ')');
        return rows;
    },

};
