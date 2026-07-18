#!/usr/bin/env node
// Refresh the thin Meilisearch metadata projection through Cherry's durable
// outbox. The explicit recovery mode recreates a verified-empty physical index
// and coordinates metadata + heat replay from PostgreSQL.
//
// Usage:
//   CHERRY_API_KEY=... node scripts/sync-meilisearch.js
//   CHERRY_API_KEY=... node scripts/sync-meilisearch.js --recover-empty-index
//
// Optional:
//   CHERRY_API_URL=http://127.0.0.1:5070
//   CHERRY_REBUILD_TIMEOUT_SECONDS=1800
//   CHERRY_REBUILD_POLL_MILLISECONDS=2000
//   --no-wait

'use strict';

const apiUrl = (process.env.CHERRY_API_URL || 'http://127.0.0.1:5070').replace(/\/+$/, '');
const apiKey = process.env.CHERRY_API_KEY || process.env.API_KEY;
const noWait = process.argv.includes('--no-wait');
const recoverEmptyIndex = process.argv.includes('--recover-empty-index');
const timeoutSeconds = positiveNumber(
    process.env.CHERRY_REBUILD_TIMEOUT_SECONDS,
    1800,
    'CHERRY_REBUILD_TIMEOUT_SECONDS');
const pollMilliseconds = positiveNumber(
    process.env.CHERRY_REBUILD_POLL_MILLISECONDS,
    2000,
    'CHERRY_REBUILD_POLL_MILLISECONDS');

if (!apiKey) {
    fail('CHERRY_API_KEY (or API_KEY) is required; direct PostgreSQL/Meili rebuild is intentionally unsupported');
}

function positiveNumber(raw, fallback, name) {
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0) fail(`${name} must be a positive number`);
    return value;
}

function fail(message) {
    console.error(`Error: ${message}`);
    process.exit(1);
}

async function api(path, options = {}) {
    const response = await fetch(`${apiUrl}${path}`, {
        ...options,
        headers: {
            Accept: 'application/json',
            'X-API-Key': apiKey,
            ...(options.headers || {})
        }
    });
    const body = await response.text();
    let json = null;
    if (body) {
        try {
            json = JSON.parse(body);
        } catch {
            throw new Error(`${options.method || 'GET'} ${path} returned non-JSON HTTP ${response.status}: ${body.slice(0, 512)}`);
        }
    }
    if (!response.ok) {
        throw new Error(`${options.method || 'GET'} ${path} returned HTTP ${response.status}: ${JSON.stringify(json)}`);
    }
    return json;
}

async function main() {
    const rebuild = recoverEmptyIndex
        ? await api('/api/v1/search/outbox/recover-empty-index', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ confirmation: 'DELETE_AND_REBUILD_TORRENTS_INDEX' })
        })
        : await api('/api/v1/search/outbox/rebuild', { method: 'POST' });
    const enqueued = recoverEmptyIndex
        ? rebuild?.metadataRowsEnqueued
        : rebuild?.enqueued;
    if (recoverEmptyIndex) {
        console.log(
            `Verified a fresh index with ${rebuild?.verifiedEmptyDocuments ?? 'unknown'} documents; ` +
            `enqueued ${enqueued ?? 'unknown'} metadata rows; ` +
            `heat rebuild=${Boolean(rebuild?.heatRebuildRequested)}` +
            `${rebuild?.heatTargetDay ? ` target=${rebuild.heatTargetDay}` : ''}; ` +
            `rolling heat rebuild=${Boolean(rebuild?.rollingHeatRebuildRequested)}` +
            `${rebuild?.rollingTargetThroughUtc ? ` target=${rebuild.rollingTargetThroughUtc}` : ''}.`);
    } else {
        console.log(`Metadata refresh enqueued for ${enqueued ?? 'unknown'} catalog rows.`);
    }
    if (noWait) return;

    const deadline = Date.now() + timeoutSeconds * 1000;
    let lastLine = '';
    const heatRequested = recoverEmptyIndex && Boolean(rebuild?.heatRebuildRequested);
    const heatTargetDay = rebuild?.heatTargetDay || null;
    const rollingRequested = recoverEmptyIndex && Boolean(rebuild?.rollingHeatRebuildRequested);
    const rollingTarget = rebuild?.rollingTargetThroughUtc || null;
    while (true) {
        const status = await api('/api/v1/search/outbox/stats');
        const backlog = status?.backlog || {};
        const recovery = status?.recovery || {};
        const heat = recovery?.heat || {};
        const depth = Number(backlog.depth);
        const due = Number(backlog.due);
        const retrying = Number(backlog.retrying);
        const heatPending = Number(heat.pendingTasks);
        const heatCaughtUp = !heatRequested || (
            heat.rebuildRequired === false &&
            heatPending === 0 &&
            (!heatTargetDay || heat.projectedThrough === heatTargetDay));
        const rollingProjected = heat.rollingProjectedThroughUtc || null;
        const rollingCaughtUp = !rollingRequested || (
            heat.rollingRebuildRequired === false &&
            rollingProjected &&
            (!rollingTarget || Date.parse(rollingProjected) >= Date.parse(rollingTarget)));
        const line = recoverEmptyIndex
            ? `depth=${depth} due=${due} retrying=${retrying} ` +
              `heatTarget=${heat.projectedThrough || '-'} heatPending=${heatPending} heatRebuild=${String(heat.rebuildRequired)}`
              + ` rollingTarget=${rollingProjected || '-'} rollingCoverage=${Number(heat.rollingCoverageHours || 0)}`
              + ` rollingRebuild=${String(heat.rollingRebuildRequired)}`
            : `depth=${depth} due=${due} retrying=${retrying} oldest=${Number(backlog.oldestAgeSeconds || 0).toFixed(1)}s`;
        if (line !== lastLine) {
            console.log(line);
            lastLine = line;
        }
        if (depth === 0 && heatCaughtUp && rollingCaughtUp) {
            if (!recoverEmptyIndex) {
                console.log('Meilisearch metadata projection is caught up.');
                return;
            }
            // COUNT(*) is deliberately deferred until projection catch-up; doing
            // it on every poll is expensive for a large authoritative catalog.
            const verified = await api('/api/v1/search/outbox/stats?verifyDocuments=true');
            const authoritativeDocuments = Number(verified?.recovery?.authoritativeDocuments);
            const meiliDocuments = Number(verified?.recovery?.meiliDocuments);
            if (!Number.isFinite(authoritativeDocuments) || !Number.isFinite(meiliDocuments)) {
                throw new Error(`unexpected recovery counts: ${JSON.stringify(verified)}`);
            }
            if (authoritativeDocuments === meiliDocuments) {
                console.log(
                    `Meilisearch metadata and heat recovery is caught up; ` +
                    `${meiliDocuments} documents match PostgreSQL.`);
                return;
            }
            console.log(
                `Projection drained but document counts differ: ` +
                `Meili=${meiliDocuments}, PostgreSQL=${authoritativeDocuments}; waiting for replay.`);
        }
        if (!Number.isFinite(depth)) throw new Error(`unexpected outbox stats: ${JSON.stringify(status)}`);
        if (Date.now() >= deadline) {
            throw new Error(`projection did not recover within ${timeoutSeconds}s (${line})`);
        }
        await new Promise(resolve => setTimeout(resolve, pollMilliseconds));
    }
}

main().catch(error => fail(error instanceof Error ? error.message : String(error)));
