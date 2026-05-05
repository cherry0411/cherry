'use strict';

var P2PSpider = require('./lib');
var http = require('http');
var os = require('os');

var CONFIG = {
    port: parseInt(process.env.SPIDER_PORT) || 6881,
    address: process.env.SPIDER_ADDRESS || '0.0.0.0',
    apiUrl: process.env.SPIDER_API_URL || 'http://127.0.0.1:5070',
    maxConnections: parseInt(process.env.SPIDER_MAX_CONN) || 600,
    nodesMaxSize: parseInt(process.env.SPIDER_MAX_NODES) || 8000,
    timeout: parseInt(process.env.SPIDER_TIMEOUT) || 5000,
    batchSize: parseInt(process.env.SPIDER_BATCH) || 32,
    flushInterval: parseInt(process.env.SPIDER_FLUSH_MS) || 3000,
    dedupSize: parseInt(process.env.SPIDER_DEDUP_SIZE) || 200000,
};

var API_BATCH_URL = CONFIG.apiUrl + '/api/v1/torrents/batch';
var API_CHECK_URL = CONFIG.apiUrl + '/api/v1/torrents/check';

// Use Set for O(1) dedup
var processed = new Set();
var remoteKnown = new Set();  // exists on main API — skip download entirely
var dedupMax = CONFIG.dedupSize;
var batch = [];
var checkQueue = [];          // pending API checks
var checkTimer = 0;
var httpAgent = new http.Agent({ keepAlive: true, maxSockets: 4 });

var stats = {
    metadata_ok: 0, metadata_fail: 0, exported: 0, export_fail: 0,
    dedup_hits: 0, remote_hits: 0, remote_checks: 0, startTime: Date.now(),
};

function log(msg) {
    console.log('[spider:' + CONFIG.port + '] ' + new Date().toISOString() + ' ' + msg);
}

function flushCheckQueue() {
    if (checkQueue.length === 0) return;
    var hashes = checkQueue.splice(0, 100).map(function (h) { return h.hash; });
    var url = API_CHECK_URL + '?hashes=' + hashes.join(',');
    http.get(url, { agent: httpAgent, timeout: 5000 }, function (res) {
        var body = '';
        res.on('data', function (d) { body += d; });
        res.on('end', function () {
            try {
                var existing = JSON.parse(body);
                existing.forEach(function (h) { remoteKnown.add(h); });
                stats.remote_hits += existing.length;
                stats.remote_checks += hashes.length;
            } catch (e) {}
        });
    }).on('error', function () {});
}

function sendBatch() {
    if (batch.length === 0) return;
    var payload = JSON.stringify({ events: batch });
    var current = batch;
    batch = [];

    var url = new URL(API_BATCH_URL);
    var req = http.request({
        hostname: url.hostname, port: url.port, path: url.pathname,
        method: 'POST',
        agent: httpAgent,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
            'Connection': 'keep-alive'
        },
        timeout: 30000
    }, function (res) {
        var body = '';
        res.on('data', function (d) { body += d; });
        res.on('end', function () {
            if (res.statusCode < 300) {
                try {
                    var r = JSON.parse(body);
                    stats.exported += r.accepted || 0;
                    if (r.accepted > 0 || r.duplicates > 0) {
                        log('api: +' + (r.accepted || 0) + ' / ' + (r.duplicates || 0) + ' dup / ' + current.length + ' sent');
                    }
                } catch (e) {
                    stats.exported += current.length;
                }
            } else {
                stats.export_fail += current.length;
                log('api error: HTTP ' + res.statusCode);
            }
        });
    });
    req.on('error', function (e) { stats.export_fail += current.length; log('api fail: ' + e.message); });
    req.on('timeout', function () { stats.export_fail += current.length; req.destroy(); log('api timeout'); });
    req.write(payload);
    req.end();
}

function logStats() {
    var elapsed = (Date.now() - stats.startTime) / 1000;
    var rate = elapsed > 0 ? Math.floor(stats.metadata_ok / elapsed) : 0;
    var dhr = elapsed > 0 ? Math.floor(stats.dedup_hits / elapsed) : 0;
    var bw = elapsed > 0 ? Math.floor(stats.metadata_ok / 60) : 0;
    log('rate ok=' + stats.metadata_ok + ' fail=' + stats.metadata_fail +
        ' export=' + stats.exported + ' dedup=' + processed.size + '/' + dedupMax +
        ' remote=' + stats.remote_hits + '/' + stats.remote_checks +
        ' dh/s=' + dhr + ' cpu=' + os.loadavg()[0].toFixed(1));
}

var p2p = P2PSpider({
    nodesMaxSize: CONFIG.nodesMaxSize,
    maxConnections: CONFIG.maxConnections,
    timeout: CONFIG.timeout
});

p2p.ignore(function (infohash, rinfo, callback) {
    if (processed.has(infohash) || remoteKnown.has(infohash)) {
        stats.dedup_hits++;
        callback(true);
        return;
    }
    callback(false);
    processed.add(infohash);
    // Queue for remote check — if API already has it, future hits skip download
    checkQueue.push({ hash: infohash });
    if (checkQueue.length >= 50) flushCheckQueue();

    if (processed.size > dedupMax) {
        var keys = Array.from(processed).slice(0, Math.floor(dedupMax / 2));
        keys.forEach(function (k) { processed.delete(k); });
    }
    if (remoteKnown.size > dedupMax) {
        var rkeys = Array.from(remoteKnown).slice(0, Math.floor(dedupMax / 2));
        rkeys.forEach(function (k) { remoteKnown.delete(k); });
    }
});

p2p.on('metadata', function (metadata) {
    if (!metadata || !metadata.info) { stats.metadata_fail++; return; }

    var name = (metadata.info['name.utf-8'] || metadata.info.name || '').toString('utf-8');
    if (!name) { stats.metadata_fail++; return; }

    var pieceLength = metadata.info['piece length'] || 0;
    var isPrivate = !!metadata.info.private;
    var files = [];
    var totalLength = 0;

    if (metadata.info.files) {
        for (var i = 0; i < metadata.info.files.length; i++) {
            var f = metadata.info.files[i];
            var fpath = (f['path.utf-8'] || f.path || '').toString('utf-8');
            if (!fpath) continue;
            if (fpath.indexOf('_____padding_file_') !== -1) continue;
            var flen = f.length || 0;
            files.push({ path_text: fpath, length: flen });
            totalLength += flen;
        }
    } else if (metadata.info.length) {
        totalLength = metadata.info.length;
        files.push({ path_text: name, length: totalLength });
    }
    if (files.length === 0 || files.length > 1000) { stats.metadata_fail++; return; }

    processed.add(metadata.infohash);
    stats.metadata_ok++;

    batch.push({
        type: 'metadata_fetched',
        timestamp: new Date().toISOString(),
        instance_id: process.env.SPIDER_INSTANCE_ID || 'spider-' + CONFIG.port,
        info_hash: metadata.infohash,
        metadata: {
            name: name,
            piece_length: pieceLength,
            length: totalLength,
            file_count: files.length,
            private: isPrivate,
            files: files
        }
    });

    if (batch.length >= CONFIG.batchSize) sendBatch();
});

setInterval(sendBatch, CONFIG.flushInterval);
setInterval(flushCheckQueue, 2000);   // flush pending checks every 2s
setInterval(logStats, 30000);

p2p.listen(CONFIG.port, CONFIG.address);
log('started maxConn=' + CONFIG.maxConnections + ' maxNodes=' + CONFIG.nodesMaxSize + ' dedup=' + dedupMax + ' api=' + CONFIG.apiUrl);
