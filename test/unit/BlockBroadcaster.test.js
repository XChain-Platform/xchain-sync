const assert = require('assert');
const sinon  = require('sinon');
const WebSocket = require('ws');
const BlockBroadcaster = require('../../src/BlockBroadcaster');

function mockWs(ip){
    let ws = {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null,
        _syncNetwork: null,
        _syncIp: null,
        send: sinon.stub(),
        close: sinon.stub(),
        on: sinon.stub()
    };
    return ws;
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

describe('BlockBroadcaster', function(){

    let broadcaster, config;

    beforeEach(function(){
        config = { WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 };
        broadcaster = new BlockBroadcaster(config);
        sinon.stub(console, 'log');
    });

    afterEach(function(){
        sinon.restore();
    });

    describe('addSubscription', function(){
        it('adds ws to subscribers set', function(){
            let ws = mockWs();
            let req = mockReq('1.1.1.1');
            let result = broadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, true);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
        });

        it('sets metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('1.1.1.1'), 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncChain, 'bitcoin');
            assert.strictEqual(ws._syncNetwork, 'mainnet');
            assert.strictEqual(ws._syncIp, '1.1.1.1');
        });

        it('rejects when per-IP limit exceeded', function(){
            let req = mockReq('2.2.2.2');
            for(let i = 0; i < 3; i++){
                broadcaster.addSubscription(mockWs(), req, 'bitcoin', 'mainnet');
            }
            let ws4 = mockWs();
            let result = broadcaster.addSubscription(ws4, req, 'bitcoin', 'mainnet');
            assert.strictEqual(result, false);
            assert.strictEqual(ws4.close.calledOnce, true);
        });

        it('sends initial status if available', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 100 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
            assert.strictEqual(sent.block_height, 100);
        });

        it('does not send status if none available', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });

        it('registers close and error handlers', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            assert.strictEqual(ws.on.calledWith('close'), true);
            assert.strictEqual(ws.on.calledWith('error'), true);
        });

        it('uses x-forwarded-for when present', function(){
            let ws = mockWs();
            let req = { headers: { 'x-forwarded-for': '9.9.9.9' }, socket: { remoteAddress: '1.1.1.1' } };
            broadcaster.addSubscription(ws, req, 'bitcoin', 'mainnet');
            assert.strictEqual(ws._syncIp, '9.9.9.9');
        });
    });

    describe('removeSubscription', function(){
        it('removes from subscribers and ipConnections', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('3.3.3.3'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 1);
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 0);
        });

        it('clears metadata on ws', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(ws._syncChain, null);
            assert.strictEqual(ws._syncNetwork, null);
        });

        it('handles ws with no metadata gracefully', function(){
            let ws = mockWs();
            broadcaster.removeSubscription(ws); // should not throw
        });

        it('cleans up empty sets from maps', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq('4.4.4.4'), 'bitcoin', 'mainnet');
            broadcaster.removeSubscription(ws);
            assert.strictEqual(broadcaster.subscribers.has('bitcoin:mainnet'), false);
            assert.strictEqual(broadcaster.ipConnections.has('4.4.4.4'), false);
        });
    });

    describe('broadcast', function(){
        it('sends to all subscribers of a chain/network', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('6.6.6.6'), 'bitcoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block', data: 'x' });
            assert.strictEqual(ws1.send.calledOnce, true);
            assert.strictEqual(ws2.send.calledOnce, true);
        });

        it('does not send to other chain/network', function(){
            let ws1 = mockWs(), ws2 = mockWs();
            broadcaster.addSubscription(ws1, mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(ws2, mockReq('6.6.6.6'), 'litecoin', 'mainnet');
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block' });
            assert.strictEqual(ws1.send.calledOnce, true);
            assert.strictEqual(ws2.send.called, false);
        });

        it('does nothing when no subscribers', function(){
            broadcaster.broadcast('bitcoin', 'mainnet', { type: 'block' }); // should not throw
        });
    });

    describe('broadcastStatus', function(){
        it('sends status to all subscribers', function(){
            broadcaster.updateStatus('bitcoin', 'mainnet', { block_height: 200 });
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.calledOnce, true);
            let sent = JSON.parse(ws.send.firstCall.args[0]);
            assert.strictEqual(sent.type, 'status');
        });

        it('does nothing when no status data', function(){
            let ws = mockWs();
            broadcaster.addSubscription(ws, mockReq(), 'bitcoin', 'mainnet');
            ws.send.resetHistory();
            broadcaster.broadcastStatus('bitcoin', 'mainnet');
            assert.strictEqual(ws.send.called, false);
        });
    });

    describe('_send', function(){
        it('skips non-OPEN WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.send.called, false);
        });

        it('closes ws when backpressure limit exceeded', function(){
            let ws = mockWs();
            ws._syncIp = 'test';
            ws._syncChain = 'bitcoin';
            ws._syncNetwork = 'mainnet';
            ws.bufferedAmount = 100;
            ws._syncBuffered = config.WS_BACKPRESSURE_LIMIT + 1;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.close.calledOnce, true);
        });

        it('resets backpressure counter when buffer is clear', function(){
            let ws = mockWs();
            ws.bufferedAmount = 0;
            ws._syncBuffered = 10;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws._syncBuffered, 0);
        });
    });

    describe('getSubscriberCount', function(){
        it('returns count for specific chain/network', function(){
            broadcaster.addSubscription(mockWs(), mockReq('a.a.a.1'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(mockWs(), mockReq('a.a.a.2'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 2);
        });

        it('returns 0 for unknown chain/network', function(){
            assert.strictEqual(broadcaster.getSubscriberCount('unknown', 'test'), 0);
        });

        it('returns total across all chains when no args', function(){
            broadcaster.addSubscription(mockWs(), mockReq('b.b.b.1'), 'bitcoin', 'mainnet');
            broadcaster.addSubscription(mockWs(), mockReq('b.b.b.2'), 'litecoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount(), 2);
        });
    });
});
