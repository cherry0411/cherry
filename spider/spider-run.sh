#!/usr/bin/env bash
# Cherry JS Spider — PM2 launcher
set -e
cd "$(dirname "$0")"
mkdir -p logs 2>/dev/null || true

case "${1:-start}" in
    install-pm2)
        npm install -g pm2
        ;;

    start)
        pm2 start ecosystem.config.js
        pm2 save
        echo "Spider started. Commands: pm2 status, pm2 logs, pm2 monit"
        ;;

    stop)
        pm2 stop spider
        ;;

    restart)
        pm2 reload ecosystem.config.js
        ;;

    delete)
        pm2 delete spider
        ;;

    status)
        pm2 status
        pm2 jlist 2>/dev/null | node -e "var d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); d.forEach(function(p){console.log(p.name+':'+p.pm2_env.SPIDER_PORT+' pid='+p.pid+' status='+p.pm2_env.status+' uptime='+Math.floor(p.pm2_env.pm_uptime/1000)+'s mem='+Math.floor(p.monit.memory/1024/1024)+'MB cpu='+p.monit.cpu+'%');})" 2>/dev/null || true
        ;;

    logs)
        pm2 logs spider ${2:---lines 50}
        ;;

    flush)
        pm2 flush
        ;;

    startup)
        pm2 startup
        pm2 save
        echo "PM2 will auto-start on boot. Run 'pm2 unstartup' to disable."
        ;;

    *)
        echo "Usage: $0 {install-pm2|start|stop|restart|delete|status|logs|flush|startup}"
        ;;
esac
