var http = require('http');
var qs = require('querystring');
var cp = require('child_process');
var os = require('os');

var checkInterval = 1000 * 60;
var restarting = false;
var spiderMaximumMem = 6000;
var spider = cp.exec('nodejs spider.js');
var secondEllapsed = 0;

setInterval(function () {
    if (!restarting) {
        (function () {
            secondEllapsed++;

            if (secondEllapsed >= 30) {
                secondEllapsed = 0;

                spider.kill();
                spider = cp.exec('nodejs spider.js');
				spider.stdout.on('data', function(data) {
					console.log('Spider: ' + data);
				});
                console.log('[MSG] spider restarted.');
            }
        })();
    }
}, checkInterval);
console.log('Monitor started. HTTP Check Interval: ' + checkInterval / 1000 + "s");

function sendRestart() {
    restarting = true;
    
    console.log('[' + getLocaleTime() + ']Restarting tomcat process...');
    var restart = cp.exec('../../etc/init.d/tomcat7 restart');
    restart.on('exit', function (code) {
        restarting = false;
        console.log('[' + getLocaleTime() + ']tomcat process restarted.');
    });
    restart.stdout.on('data', function (data) {
        console.log('Restart: ' + data);
    });
}

function getLocaleTime() {
    return new Date().toLocaleTimeString();
}

process.on('uncaughtException', function (e) {
    console.log(e);
});

spider.stdout.on('data', function (data) {
    console.log('Spider: ' + data);
});