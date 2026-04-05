const express = require('express');

class MockHub {

    constructor() {
        this.app     = null;
        this.server  = null;
        this.configs = {};
    }

    // Set the configs the hub will return
    // configs: array of { coin, network, db_host, db_port, db_name, db_user, db_pass }
    setConfigs(configs) {
        this.configs = {};
        for (let cfg of configs) {
            if (!this.configs[cfg.coin]) this.configs[cfg.coin] = {};
            this.configs[cfg.coin][cfg.network] = {
                'xchain-indexer': {
                    db_host: cfg.db_host,
                    db_port: cfg.db_port,
                    name:    cfg.db_name,
                    user:    cfg.db_user,
                    pass:    cfg.db_pass
                }
            };
        }
    }

    start(port) {
        return new Promise((resolve) => {
            this.app = express();
            this.app.use(express.json());

            this.app.post('/', (req, res) => {
                let method = req.body.method;
                if (method === 'ping') {
                    return res.json({ jsonrpc: '2.0', result: true, id: req.body.id });
                }
                if (method === 'getallconfigs') {
                    return res.json({ jsonrpc: '2.0', result: this.configs, id: req.body.id });
                }
                res.json({ jsonrpc: '2.0', error: { message: 'Unknown method' }, id: req.body.id });
            });

            this.server = this.app.listen(port, () => resolve());
        });
    }

    stop() {
        return new Promise((resolve) => {
            if (this.server) {
                this.server.close(() => resolve());
            } else {
                resolve();
            }
        });
    }
}

module.exports = MockHub;
