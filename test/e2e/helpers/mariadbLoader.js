// Wrapper to load ESM-only mariadb package from CommonJS
// Caches the module after first load
let _mariadb = null;

async function getMariadb() {
    if (!_mariadb) {
        _mariadb = await import('mariadb');
        if (_mariadb.default) _mariadb = _mariadb.default;
    }
    return _mariadb;
}

module.exports = { getMariadb };
