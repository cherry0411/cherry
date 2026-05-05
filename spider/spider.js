'use strict';

var P2PSpider = require('./lib');
var mysql = require('mysql');

var p2p = P2PSpider({
    nodesMaxSize: 200,
    maxConnections: 400,
    timeout: 5000
});

// var pool = mysql.createPool({
//     host: '138.197.196.147',
//     user: 'root',
//     password: 'DJMix261389pwd!',
//     connectionLimit: 200,
//     waitForConnections: true,
// 	database: 'crawlerdb'
// });

var processed = [];

p2p.ignore(function (infohash, rinfo, callback) {
    if (processed.indexOf(infohash) != -1) {
        callback(true);
    //} else {
    //    hashExists(infohash, function (exists) {
    //        callback(exists);
    //    });
    } else { callback(false); }

    if (processed.length > 1000000) {
        processed = [];
    }
});

p2p.on('metadata', function (metadata) {
    if (metadata == null || metadata.info == null) return;
    var torrent = new Object();

    if (metadata.info['name.utf-8'] != null)
        torrent.title = metadata.info['name.utf-8'].toString('utf-8');
    else
        torrent.title = metadata.info.name.toString('utf-8');
    torrent.hash = metadata.infohash.toUpperCase();
    //torrent.included_date = new Date();
    torrent.files = 0;
    torrent.length = 0;
    torrent.analyzed = 0;
    torrent.clicks = 0;
    torrent.file_list = [];
    if (metadata.info.files != null) {
        metadata.info.files.forEach(function (info) {
            var file = new Object();
            if (info['path.utf-8'] != null)
                file.path = info['path.utf-8'].toString('utf-8');
            else
                file.path = info.path.toString('utf-8');
            if (file.path.indexOf('_____padding_file_') != -1) return;
            file.length = info.length;
            torrent.files++;
            torrent.length += file.length;
            torrent.file_list.push(file);
        });
    }

    processed.push(metadata.infohash);
    if (torrent.files == 0 || torrent.files > 1000) return;
    console.log('[metadata]' + torrent.title);
    // saveTorrent(torrent);
});

function hashExists(hash, callback) {
    if (err) {
        console.log(err);
        callback(true);
    } else {
        var sql = 'SELECT COUNT(hash) AS count FROM torrent WHERE hash = \'' + hash + '\';';
        connection.query(sql, function (err, results) {
            if (err) {
                console.log(err);
                callback(true);
            } else {
                callback(parseInt(results[0]['count']) == 1);
            }
        });
    }
}

function saveTorrent(torrent) {
    pool.getConnection(function (err, connection) {
        if (err) {
            console.log(err);
        } else {
            connection.charset = 'UTF8';
            connection.beginTransaction(function (err) {
                if (err) {
                    console.log(err);
                } else {
                    var sql = 'INSERT INTO torrent SET included_date=now(),?';
                    var post = {
                        hash: torrent.hash,
                        title: torrent.title,
                        size: torrent.length,
                        files: torrent.files,
			analyzed: 0,
			clicks: 0
                    };
                    connection.query(sql, post, function (error, results) {
                        if (error) {
                            connection.rollback();
                            connection.release();
			    // console.log(error);
                        } else {
                            var count = 0;
                            torrent.file_list.forEach(function (file) {
                                sql = "INSERT INTO torrent_file SET ?";
                                post = {
                                    hash: torrent.hash,
                                    path: file.path,
                                    size: file.length
                                };
                                connection.query(sql, post, function (error, results) {
                                    if (error) {
                                        connection.rollback();
                                        connection.release();
					connection = undefined;
					console.log(err);
                                        return;
                                    } else {
                                    	count = count + 1;
				    }
                                });
                            });
				if (connection) {
                                connection.commit();
                                console.log('[DB]' + torrent.title);
                            	connection.release();
				}
                        }
                    });
                }
            });
        }
    });
}

p2p.listen(6881, '0.0.0.0');

process.on('uncaughtException', function (e) {
    console.log(e);
});