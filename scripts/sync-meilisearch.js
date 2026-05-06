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
    password: process.env.PG_PASSWORD || 'postgres',
});
pg.connect();

async function main() {
    // 1. Check total
    const { rows: [{ count }] } = await pg.query('SELECT COUNT(*) FROM torrents');
    console.log(`Total: ${count} torrents`);

    // 2. Create Meilisearch index with searchable fields
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
            rankingRules: [
                'words',
                'typo',
                'proximity',
                'attribute',
                'sort',
                'exactness',
                'createdAt:desc'
            ]
        })
    });

    // 4. Batch import
    for (let offset = 0; offset < count; offset += BATCH) {
        const { rows } = await pg.query(
            `SELECT info_hash, name, total_length, file_count, is_private, peer_count, created_at
             FROM torrents ORDER BY id LIMIT ${BATCH} OFFSET ${offset}`
        );
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
        console.log(`${offset + rows.length} / ${count}`);
    }

    console.log('Done. Try: curl http://localhost:7700/indexes/torrents/search?q=test');
    process.exit(0);
}
main().catch(e => { console.error(e); process.exit(1); });
