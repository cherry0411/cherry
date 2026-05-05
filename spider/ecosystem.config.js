module.exports = {
    apps: [{
        name: 'spider',
        script: './cherry-spider.js',
        instances: parseInt(process.env.SPIDER_INSTANCES || '6'),
        exec_mode: 'fork',
        increment_var: 'SPIDER_PORT',
        env: {
            SPIDER_API_URL: process.env.SPIDER_API_URL || 'http://127.0.0.1:5070',
            SPIDER_INSTANCE_PREFIX: process.env.SPIDER_INSTANCE_PREFIX || 'js',
            SPIDER_MAX_CONN: '600',
            SPIDER_MAX_NODES: '5000',
            SPIDER_TIMEOUT: '5000',
            SPIDER_DEDUP_SIZE: '200000',
            SPIDER_PORT: 20003,
            SPIDER_ADDRESS: '0.0.0.0',
        },
        max_memory_restart: '512M',
        log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
        error_file: './logs/pm2-error.log',
        out_file: './logs/pm2-out.log',
        merge_logs: true,
        autorestart: true,
        max_restarts: 10,
        restart_delay: 5000,
    }]
};
