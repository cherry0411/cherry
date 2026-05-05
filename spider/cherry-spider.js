'use strict';

// Cherry DHT Spider — production entry point
// Reads config from env vars, downloads metadata via BT protocol, POSTs to Cherry API

var P2PSpider = require('./lib');
var http = require('http');

var CONFIG = {
    port: parseInt(process.env.SPIDER_PORT) || 6881,
    address: process.env.SPIDER_ADDRESS || '0.0.0.0',
    apiUrl: process.env.SPIDER_API_URL || 'http://127.0.0.1:5070',
    maxConnections: parseInt(process.env.SPIDER_MAX_CONN) || 400,
    nodesMaxSize: parseInt(process.env.SPIDER_MAX_NODES) || 5000,
    timeout: parseInt(process.env.SPIDER_TIMEOUT) || 5000,
    batchSize: parseInt(process.env.SPIDER_BATCH) || 32,
    flushInterval: parseInt(process.env.SPIDER_FLUSH_MS) || 3000,
    dedupSize: parseInt(process.env.SPIDER_DEDUP_SIZE) || 100000,
};

var API_BATCH_URL = CONFIG.apiUrl + '/api/v1/torrents/batch';

var processed = [];
var batch = [];
var stats = {
    metadata_ok: 0,
    metadata_fail: 0,
    exported: 0,
    export_fail: 0,
    startTime: Date.now(),
};

function log(msg) {
    console.log('[spider] ' + new Date().toISOString() + ' ' + msg);
}

function sendBatch() {
    if (batch.length === 0) return;
    var payload = JSON.stringify({ events: batch });
    var current = batch;
    batch = [];

    var url = new URL(API_BATCH_URL);
    var options = {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload)
        },
        timeout: 30000
    };

    var req = http.request(options, function (res) {
        var body = '';
        res.on('data', function (d) { body += d; });
        res.on('end', function () {
            if (res.statusCode < 300) {
                try {
                    var r = JSON.parse(body);
                    stats.exported += r.accepted || 0;
                    log('api: ' + (r.accepted || 0) + ' accepted, ' + (r.duplicates || 0) + ' dup, ' + (r.errors || 0) + ' err (sent ' + current.length + ')');
                } catch (e) {
                    stats.exported += current.length;
                    log('export: ' + current.length + ' events sent');
                }
            } else {
                stats.export_fail += current.length;
                log('api error: HTTP ' + res.statusCode);
            }
        });
    });
    req.on('error', function (e) {
        stats.export_fail += current.length;
        log('api connection failed: ' + e.message);
    });
    req.on('timeout', function () {
        stats.export_fail += current.length;
        req.destroy();
        log('api timeout');
    });
    req.write(payload);
    req.end();
}

function logStats() {
    var elapsed = Math.floor((Date.now() - stats.startTime) / 1000);
    var rate = elapsed > 0 ? Math.floor(stats.metadata_ok / elapsed) : 0;
    log('stats: ok=' + stats.metadata_ok + ' fail=' + stats.metadata_fail +
        ' exported=' + stats.exported + ' rate=' + rate + '/s dedup=' + processed.length +
        ' nodes=' + (global.nodeCount || '?'));
}

var p2p = P2PSpider({
    nodesMaxSize: CONFIG.nodesMaxSize,
    maxConnections: CONFIG.maxConnections,
    timeout: CONFIG.timeout
});

p2p.ignore(function (infohash, rinfo, callback) {
    if (processed.indexOf(infohash) !== -1) {
        callback(true);
    } else {
        callback(false);
    }
    if (processed.length > CONFIG.dedupSize) {
        processed = processed.slice(-Math.floor(CONFIG.dedupSize / 2));
    }
});

p2p.on('metadata', function (metadata) {
    if (!metadata || !metadata.info) {
        stats.metadata_fail++;
        return;
    }

    var name;
    if (metadata.info['name.utf-8']) {
        name = metadata.info['name.utf-8'].toString('utf-8');
    } else if (metadata.info.name) {
        name = metadata.info.name.toString('utf-8');
    }
    if (!name) { stats.metadata_fail++; return; }

    var pieceLength = metadata.info['piece length'] || 0;
    var isPrivate = metadata.info.private ? true : false;
    var files = [];
    var totalLength = 0;

    if (metadata.info.files) {
        for (var i = 0; i < metadata.info.files.length; i++) {
            var f = metadata.info.files[i];
            var fpath;
            if (f['path.utf-8']) {
                fpath = f['path.utf-8'].toString('utf-8');
            } else if (f.path) {
                fpath = f.path.toString('utf-8');
            }
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

    if (files.length === 0) { stats.metadata_fail++; return; }

    processed.push(metadata.infohash);
    stats.metadata_ok++;

    batch.push({
        type: 'metadata_fetched',
        timestamp: new Date().toISOString(),
        instance_id: process.env.SPIDER_INSTANCE_ID || 'spider-1',
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

    if (batch.length >= CONFIG.batchSize) {
        sendBatch();
    }
});

// Flush
setInterval(sendBatch, CONFIG.flushInterval);

// Stats
setInterval(logStats, 30000);

// Start
p2p.listen(CONFIG.port, CONFIG.address);
log('started on UDP :' + CONFIG.port + ' api=' + CONFIG.apiUrl + ' maxConn=' + CONFIG.maxConnections + ' maxNodes=' + CONFIG.nodesMaxSize);
