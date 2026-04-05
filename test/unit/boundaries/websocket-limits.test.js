const assert = require('assert');
const sinon  = require('sinon');
const WebSocket = require('ws');
const BlockBroadcaster = require('../../../src/BlockBroadcaster');

function mockWs(){
    return {
        readyState: WebSocket.OPEN,
        bufferedAmount: 0,
        _syncBuffered: 0,
        _syncChain: null, _syncNetwork: null, _syncIp: null,
        send: sinon.stub(), close: sinon.stub(), on: sinon.stub()
    };
}

function mockReq(ip){
    return { headers: {}, socket: { remoteAddress: ip || '127.0.0.1' } };
}

describe('Boundary: WebSocket Limits', function(){

    let broadcaster;

    afterEach(function(){ sinon.restore(); });

    describe('per-IP connection limit', function(){
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 3, WS_BACKPRESSURE_LIMIT: 50 });
            sinon.stub(console, 'log');
        });

        it('accepts connections 1, 2, 3 from same IP', function(){
            let ip = '1.1.1.1';
            for(let i = 0; i < 3; i++){
                let result = broadcaster.addSubscription(mockWs(), mockReq(ip), 'bitcoin', 'mainnet');
                assert.strictEqual(result, true);
            }
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 3);
        });

        it('rejects 4th connection from same IP', function(){
            let ip = '2.2.2.2';
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq(ip), 'bitcoin', 'mainnet');

            let ws4 = mockWs();
            let result = broadcaster.addSubscription(ws4, mockReq(ip), 'bitcoin', 'mainnet');
            assert.strictEqual(result, false);
            assert.strictEqual(ws4.close.calledOnce, true);
            assert.strictEqual(ws4.close.firstCall.args[0], 1008);
        });

        it('accepts after one closes (3 → close 1 → add new)', function(){
            let ip = '3.3.3.3';
            let sockets = [];
            for(let i = 0; i < 3; i++){
                let ws = mockWs();
                broadcaster.addSubscription(ws, mockReq(ip), 'bitcoin', 'mainnet');
                sockets.push(ws);
            }
            broadcaster.removeSubscription(sockets[0]);
            let wsNew = mockWs();
            let result = broadcaster.addSubscription(wsNew, mockReq(ip), 'bitcoin', 'mainnet');
            assert.strictEqual(result, true);
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 3);
        });

        it('different IPs tracked independently', function(){
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq('4.4.4.4'), 'bitcoin', 'mainnet');
            for(let i = 0; i < 3; i++)
                broadcaster.addSubscription(mockWs(), mockReq('5.5.5.5'), 'bitcoin', 'mainnet');
            assert.strictEqual(broadcaster.getSubscriberCount('bitcoin', 'mainnet'), 6);
        });
    });

    describe('per-IP limit = 1', function(){
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 1, WS_BACKPRESSURE_LIMIT: 50 });
            sinon.stub(console, 'log');
        });

        it('accepts first, rejects second', function(){
            let ip = '6.6.6.6';
            assert.strictEqual(broadcaster.addSubscription(mockWs(), mockReq(ip), 'b', 'm'), true);
            let ws2 = mockWs();
            assert.strictEqual(broadcaster.addSubscription(ws2, mockReq(ip), 'b', 'm'), false);
            assert.strictEqual(ws2.close.calledOnce, true);
        });
    });

    describe('backpressure limit (50)', function(){
        beforeEach(function(){
            broadcaster = new BlockBroadcaster({ WS_MAX_PER_IP: 10, WS_BACKPRESSURE_LIMIT: 50 });
            sinon.stub(console, 'log');
        });

        it('49 buffered sends — connection maintained', function(){
            let ws = mockWs();
            ws.bufferedAmount = 100;
            ws._syncBuffered = 49;
            ws._syncIp = 'test'; ws._syncChain = 'b'; ws._syncNetwork = 'm';
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.close.called, false);
            assert.strictEqual(ws._syncBuffered, 50);
        });

        it('50 buffered sends — connection maintained (> not >=)', function(){
            let ws = mockWs();
            ws.bufferedAmount = 100;
            ws._syncBuffered = 50;
            ws._syncIp = 'test'; ws._syncChain = 'b'; ws._syncNetwork = 'm';
            broadcaster._send(ws, 'test');
            // 50 + 1 = 51 > 50 → dropped
            assert.strictEqual(ws.close.calledOnce, true);
        });

        it('51 buffered sends — connection dropped', function(){
            let ws = mockWs();
            ws.bufferedAmount = 100;
            ws._syncBuffered = 51;
            ws._syncIp = 'test'; ws._syncChain = 'b'; ws._syncNetwork = 'm';
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.close.calledOnce, true);
        });

        it('counter resets when buffer clears', function(){
            let ws = mockWs();
            ws.bufferedAmount = 0;
            ws._syncBuffered = 45;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws._syncBuffered, 0);
            assert.strictEqual(ws.close.called, false);
        });

        it('counter increments when buffer not clear', function(){
            let ws = mockWs();
            ws.bufferedAmount = 100;
            ws._syncBuffered = 0;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws._syncBuffered, 1);
        });

        it('skips closed WebSocket', function(){
            let ws = mockWs();
            ws.readyState = WebSocket.CLOSED;
            broadcaster._send(ws, 'test');
            assert.strictEqual(ws.send.called, false);
        });
    });
});
