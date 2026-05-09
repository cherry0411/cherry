// Sync PostgreSQL torrents → Meilisearch
// Usage: node scripts/sync-meilisearch.js
const { Client } = require('pg');
const MEILI = 'http://localhost:7700';
const BATCH = 1000;

const pg = new Client({
    host: process.env.PG_HOST || 'localhost',
    port: process.env.PG_PORT || 5432,
    database: process.env.PG_DB || 'cherry',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || 'cindy131120',
});
pg.connect();

async function main() {
    // 1. Check total
    const { rows: [{ count }] } = await pg.query('SELECT COUNT(*) FROM torrents');
    console.log(`Total: ${count} torrents`);

    // 2. Create Meilisearch index with searchable fields (delete if exists then recreate)
    await fetch(`${MEILI}/indexes/torrents`, { method: 'DELETE' }).catch(() => {});
    await fetch(`${MEILI}/indexes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            uid: 'torrents',
            primaryKey: 'infoHash'
        })
    });

    // 3. Configure searchable + sortable fields
    await fetch(`${MEILI}/indexes/torrents/settings`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            searchableAttributes: ['name'],
            sortableAttributes: ['peerCount', 'totalLength', 'fileCount', 'createdAt'],
            filterableAttributes: ['fileCount', 'totalLength', 'isPrivate', 'peerCount'],
            typoTolerance: { minWordSizeForTypos: { oneTypo: 5, twoTypos: 8 }, disableOnWords: [], disableOnAttributes: [] },
            rankingRules: [
                'sort',
                'createdAt:desc',
                'words',
                'exactness'
            ]
        })
    });

    // 4. Batch import using cursor pagination on info_hash (avoids relying on numeric id column)
    let last = '';
    let processed = 0;
    while (true) {
        const { rows } = await pg.query(
            `SELECT info_hash, name, total_length, file_count, is_private, peer_count, created_at
             FROM torrents
             WHERE info_hash > $1
             ORDER BY info_hash
             LIMIT $2`,
            [last, BATCH]
        );

        if (rows.length === 0) break;

        const docs = rows.map(r => ({
            infoHash: r.info_hash,
            name: r.name,
            totalLength: parseInt(r.total_length),
            fileCount: parseInt(r.file_count),
            isPrivate: r.is_private,
            peerCount: r.peer_count || 0,
            createdAt: r.created_at ? new Date(r.created_at).getTime() : 0
        }));

        await fetch(`${MEILI}/indexes/torrents/documents`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(docs)
        });

        processed += rows.length;
        last = rows[rows.length - 1].info_hash;
        console.log(`${processed} / ${count}`);
    }

    console.log('Done. Try: curl http://localhost:7700/indexes/torrents/search?q=test');
    process.exit(0);
}
main().catch(e => { console.error(e); process.exit(1); });
