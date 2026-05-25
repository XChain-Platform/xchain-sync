// Poll a condition function until it returns true or timeout expires
async function waitFor(fn, timeout = 15000, interval = 100) {
    let start = Date.now();
    while (Date.now() - start < timeout) {
        if (await fn()) return true;
        await new Promise(r => setTimeout(r, interval));
    }
    throw new Error('waitFor timeout after ' + timeout + 'ms');
}

// Wait until replica DB reaches a given block height
async function waitForReplicaBlock(replicaDb, expectedBlock, timeout = 15000) {
    await waitFor(async () => {
        let block = await replicaDb.getLastBlock();
        return block !== null && block >= expectedBlock;
    }, timeout);
}

// Wait until server status reports a given block height
async function waitForServerStatus(axios, serverUrl, chain, network, expectedBlock, timeout = 15000) {
    await waitFor(async () => {
        try {
            let res = await axios.get(serverUrl + '/status/indexer/' + chain + '/' + network, { timeout: 3000 });
            return res.data && res.data.block_height >= expectedBlock;
        } catch (e) {
            return false;
        }
    }, timeout);
}

// Wait until a table has a given row count
async function waitForTableCount(db, table, expectedCount, timeout = 15000) {
    await waitFor(async () => {
        let count = await db.getTableCount(table);
        return count >= expectedCount;
    }, timeout);
}

module.exports = {
    waitFor,
    waitForReplicaBlock,
    waitForServerStatus,
    waitForTableCount
};
