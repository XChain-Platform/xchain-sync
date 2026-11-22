// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

// The indexer REORG-marker cursor is kept in sync_state so a marker the indexer
// writes while this server is stopped is still consumed on the next start. A
// read-only replica box cannot write sync_state, so it keeps the in-memory seed.

function cursorKey(poller){
    return 'server_reorg_marker_cursor:' + poller.chain + ':' + poller.network + ':' + poller.dbType;
}

function isDurable(poller){
    if(poller.transparencyLog && poller.transparencyLog.readOnly) return false;
    return typeof poller.db.getSyncState === 'function' &&
        typeof poller.db.setSyncState === 'function';
}

// The write is fail-soft: the in-memory cursor still advances, and a lost write
// only replays markers on the next start, which the prune and rewind path tolerates.
async function writeCursor(poller, eventId){
    poller.lastEventId = eventId;
    await poller.db.setSyncState(cursorKey(poller), eventId === null ? 0 : eventId);
}

// With no usable stored cursor, a marker stamped at or after the newest sync_meta
// row was written after this server last recorded a block, because consuming a
// marker re-records the blocks from its fork. A same-second tie replays.
async function seedWithoutStoredCursor(poller, newestEventId, logger){
    if(newestEventId === null || !poller.transparencyLog ||
        typeof poller.db.findReorgMarkerSinceLastRecord !== 'function') return newestEventId;
    const markerId = await poller.db.findReorgMarkerSinceLastRecord();
    if(!Number.isSafeInteger(markerId) || markerId < 1 || markerId > newestEventId)
        return newestEventId;
    logger.warn('ServerPoller: replaying indexer REORG markers from events id ' + markerId +
        ' for ' + poller.chain + '/' + poller.network + ', stamped after the last recorded block');
    return markerId > 1 ? markerId - 1 : null;
}

async function resumeStoredCursor(poller, stored, newestEventId, logger){
    const storedEventId = stored === null ? null : Number(stored);
    const latest = newestEventId === null ? 0 : newestEventId;
    if(storedEventId !== null && Number.isSafeInteger(storedEventId) &&
        storedEventId >= 0 && storedEventId <= latest){
        poller.lastEventId = storedEventId === 0 ? null : storedEventId;
        return;
    }
    if(stored !== null)
        logger.warn('ServerPoller: stored events cursor ' + stored + ' is unusable against ' +
            latest + ' for ' + poller.chain + '/' + poller.network + '; reseeding');
    await writeCursor(poller, await seedWithoutStoredCursor(poller, newestEventId, logger));
}

async function initializeReorgMarkerCursor(poller, logger){
    if(poller.dbType === 'decoder' || typeof poller.db.getMaxRowId !== 'function') return;
    const newestEventId = await poller.readLatestEventId();
    if(isDurable(poller)){
        const stored = await poller.db.getSyncState(cursorKey(poller), { rethrow: true });
        await resumeStoredCursor(poller, stored, newestEventId, logger);
    } else {
        poller.lastEventId = newestEventId;
    }
    poller.reorgMarkerCursorInitialized = true;
}

async function acknowledgeReorgMarkerCursor(poller, eventId){
    if(eventId === poller.lastEventId) return;
    if(isDurable(poller)) await writeCursor(poller, eventId);
    else poller.lastEventId = eventId;
}

module.exports = { initializeReorgMarkerCursor, acknowledgeReorgMarkerCursor, cursorKey };
