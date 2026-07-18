var API = window.CHERRY_API || '';

function fmtSize(bytes) {
    if (bytes == null || bytes <= 0) return '0 B';
    var u = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), u.length - 1);
    return (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + ' ' + u[i];
}
function fmtDate(iso) {
    if (!iso) return '-';
    var d = new Date(iso);
    return d.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' }) + ' ' + d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}
function fmtRelative(iso) {
    if (!iso) return ''; var diff = Date.now() - new Date(iso).getTime(); if (diff < 0) return '';
    var m = Math.floor(diff / 60000); if (m < 1) return 'just now'; if (m < 60) return m + 'm ago';
    var h = Math.floor(m / 60); if (h < 24) return h + 'h ago'; var d = Math.floor(h / 24);
    if (d < 30) return d + 'd ago'; return Math.floor(d / 30) + 'mo ago';
}
function fmtNumber(n) { return n == null ? '0' : Number(n).toLocaleString('zh-CN'); }
function heatValue(item, windowName) {
    var key = 'heat' + String(windowName || '7d');
    return Number(item && item[key]) || 0;
}
function magnetLink(h, n) { return 'magnet:?xt=urn:btih:' + h + '&dn=' + encodeURIComponent(n || ''); }
function escapeHtml(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
function highlightTerm(text, query) {
    if (!query || !text) return escapeHtml(text);
    var e = escapeHtml(text), q = escapeHtml(query).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return e.replace(new RegExp('(' + q + ')', 'gi'), '<mark>$1</mark>');
}
function detectCategory(name) {
    if (!name) return { icon: '📄', cat: '' };
    var n = name.toLowerCase();
    if (/s\d{1,2}e\d|season|episode/i.test(n)) return { icon: '📺', cat: 'Series' };
    if (/\.(flac|mp3|m4a|aac|ogg|wav)/i.test(n)) return { icon: '🎵', cat: 'Music' };
    if (/\.(iso|dmg|exe|msi|rar|zip|7z|deb|rpm)/i.test(n) || /(setup|install|crack|keygen)/i.test(n)) return { icon: '💿', cat: 'Software' };
    if (/game|gog|steam|epic/i.test(n)) return { icon: '🎮', cat: 'Game' };
    if (/\.(pdf|epub|mobi)/i.test(n)) return { icon: '📚', cat: 'Books' };
    if (/\.(mkv|mp4|avi|mov|bluray|web-dl|bdrip|1080p|2160p|x265|x264)/i.test(n)) return { icon: '🎬', cat: 'Movie' };
    return { icon: '📦', cat: '' };
}
function fileIcon(filename) {
    if (!filename) return '📄';
    var m = { mkv: '🎬', mp4: '🎬', avi: '🎬', mov: '🎬', flac: '🎵', mp3: '🎵', m4a: '🎵', iso: '💿', exe: '⚙️', rar: '📦', zip: '📦', '7z': '📦', pdf: '📕', epub: '📚', jpg: '🖼️', png: '🖼️', txt: '📝', srt: '💬', sfv: '✅' };
    var e = filename.split('.').pop().toLowerCase();
    return m[e] || '📄';
}

// ---- i18n ----
var lang = (function () { try { var v = localStorage.getItem('cherry_lang'); if (v) return v; } catch (e) { } return (navigator.language || '').startsWith('zh') ? 'zh' : 'en'; })();
var dict = {
    zh: { site_title: 'Cherry - 种子搜索', search_placeholder: '搜索种子...', search_btn: '搜索', hero_title: 'Cherry 种子搜索', hero_desc: '搜索千万级 DHT 网络种子', hero_placeholder: '电影、剧集、软件、音乐...', stat_total: '总量', stat_today: '今日新增', recent_searches: '最近搜索', results_for: '结果', found: '条', no_results: '未找到结果', no_results_hint: '试试其他关键词', retry: '重试', prev_page: '上一页', next_page: '下一页', copy_magnet: '复制', back: '返回结果', torrent_not_found: '种子未找到', server_error: '服务器错误', info_hash: 'Info Hash', total_size: '大小', file_count: '文件数', discovered: '发现时间', download_magnet: '磁力链接', copy_link: '复制链接', copied: '已复制', file_list: '文件列表', filter_files: '筛选文件...', hash_copied: '已复制', link_copied: '已复制', private_torrent: '私有种子', just_now: '刚刚', min_ago: '分钟前', h_ago: '小时前', d_ago: '天前', mo_ago: '个月前', sort_name: '文件名', sort_size: '大小', lang_label: 'En', lookup: '查询', hot: '热榜', network_activity: '网络活跃度', heat_as_of: '统计截至', heat_coverage: '覆盖小时', status_crawling: '抓取中', status_online: 'API 在线', status_offline: '离线', metadata_rate: 'Metadata 速率', heat_records: 'Heat 记录', heat_rate: 'Heat 速率', rolling_heat: '24h Heat 容量', last_update: '更新于' },
    en: { site_title: 'Cherry - DHT Search', search_placeholder: 'Search torrents...', search_btn: 'Search', hero_title: 'Cherry Torrent Search', hero_desc: 'Search millions of torrents from DHT', hero_placeholder: 'Movies, TV, software, music...', stat_total: 'Total', stat_today: 'Today', recent_searches: 'Recent', results_for: 'Results for', found: 'found', no_results: 'No results', no_results_hint: 'Try different keywords', retry: 'Retry', prev_page: 'Prev', next_page: 'Next', copy_magnet: 'Copy', back: 'Back to results', torrent_not_found: 'Torrent not found', server_error: 'Server error', info_hash: 'Info Hash', total_size: 'Size', file_count: 'Files', discovered: 'Discovered', download_magnet: 'Magnet', copy_link: 'Copy Link', copied: 'Copied', file_list: 'File List', filter_files: 'Filter files...', hash_copied: 'Copied', link_copied: 'Copied', private_torrent: 'Private', just_now: 'just now', min_ago: 'm ago', h_ago: 'h ago', d_ago: 'd ago', mo_ago: 'mo ago', sort_name: 'Name', sort_size: 'Size', lang_label: '中文', lookup: 'Lookup', hot: 'Hot', network_activity: 'Network activity', heat_as_of: 'As of', heat_coverage: 'coverage hours', status_crawling: 'Crawling', status_online: 'API online', status_offline: 'Offline', metadata_rate: 'Metadata rate', heat_records: 'Heat records', heat_rate: 'Heat rate', rolling_heat: '24h heat capacity', last_update: 'Updated' }
};
function T(k) { return (dict[lang] && dict[lang][k]) || dict.en[k] || k; }
function switchLang() { lang = lang === 'zh' ? 'en' : 'zh'; try { localStorage.setItem('cherry_lang', lang); } catch (e) { } window.location.reload(); }

// ---- History ----
function loadHistory() { try { return JSON.parse(localStorage.getItem('cherry_history')) || []; } catch (e) { return []; } }
function saveHistory(q) { var h = loadHistory().filter(function (x) { return x !== q; }); h.unshift(q); if (h.length > 10) h.length = 10; try { localStorage.setItem('cherry_history', JSON.stringify(h)); } catch (e) { } }

// ---- Toast ----
var tt; function showToast(m) { var e = document.getElementById('toast'); if (!e) { e = document.createElement('div'); e.id = 'toast'; e.className = 'toast'; document.body.appendChild(e); } e.textContent = m; e.classList.add('show'); clearTimeout(tt); tt = setTimeout(function () { e.classList.remove('show'); }, 2000); }
function copyText(t) { try { navigator.clipboard.writeText(t); showToast('Copied'); } catch (e) { showToast('Copied'); } }

// ---- Components ----
var HomePage = {
    template: '<div class="hero"><div class="hero-logo">🍒</div><h1>{{ T("hero_title") }}</h1><div class="hero-search"><input class="search-input" :placeholder="T(\'hero_placeholder\')" v-model="q" @keydown.enter="goSearch" ref="inp" /><button class="search-btn" @click="goSearch">{{ T("search_btn") }}</button></div><div class="hero-hash"><input placeholder="info_hash..." v-model="hq" @keydown.enter="lookupHash" /><button class="hash-btn" @click="lookupHash">{{ T("lookup") }}</button></div><div v-if="history.length" class="search-history"><span v-for="h in history" class="history-chip" @click="goHistory(h)">{{ h }}</span></div><section class="status-board" aria-live="polite"><div class="status-card status-state"><span class="status-label">Status</span><strong><i class="status-dot" :class="{active:online,crawling:isCrawling}"></i>{{ stateText }}</strong><small v-if="lastUpdated">{{ T("last_update") }} {{ lastUpdated }}</small></div><div class="status-card"><span class="status-label">{{ T("stat_total") }}</span><strong>{{ fmtNumber(st.totalTorrents) }}</strong><small>+{{ fmtNumber(st.todayNew) }} {{ T("stat_today") }}</small></div><div class="status-card"><span class="status-label">{{ T("metadata_rate") }}</span><strong>{{ fmtRate(metadataRate) }}</strong><small>metadata/s</small></div><div class="status-card"><span class="status-label">{{ T("heat_records") }}</span><strong>{{ fmtNumber(acceptedHeat) }}</strong><small>{{ T("heat_rate") }} {{ fmtRate(heatRate) }}/s</small></div><div class="status-card"><span class="status-label">{{ T("rolling_heat") }}</span><strong>{{ capacityPercent }}</strong><small>{{ capacityText }}</small></div></section><div class="footer">Ctrl+K to search</div></div>',
    data: function () { return { st: {}, health: {}, q: '', hq: '', history: loadHistory(), online: false, metadataRate: null, heatRate: null, lastUpdated: '', pollTimer: null, pollBusy: false, previousMetadata: null, previousHeat: null }; },
    computed: {
        acceptedHeat: function () { return Number(this.health.heat && this.health.heat.acceptedRecords) || 0; },
        rolling: function () { return this.health.rolling_heat_capacity || {}; },
        capacityPercent: function () { var max = Number(this.rolling.maxBytes) || 0; return max ? ((Number(this.rolling.usedBytes) || 0) / max * 100).toFixed(1) + '%' : '—'; },
        capacityText: function () { var max = Number(this.rolling.maxBytes) || 0; return max ? fmtSize(this.rolling.usedBytes) + ' / ' + fmtSize(max) : '—'; },
        isCrawling: function () { var t = this.health.heat && this.health.heat.lastCommitAt; return this.online && t && Date.now() - new Date(t).getTime() < 30000; },
        stateText: function () { return !this.online ? T('status_offline') : (this.isCrawling ? T('status_crawling') : T('status_online')); }
    },
    mounted: function () { this.$refs.inp && this.$refs.inp.focus(); document.title = T('site_title'); this.poll(); },
    beforeUnmount: function () { clearTimeout(this.pollTimer); },
    methods: {
        T: T, fmtNumber: fmtNumber,
        fmtRate: function (n) { return n == null || !isFinite(n) ? '—' : Number(n).toFixed(n < 10 ? 1 : 0); },
        fetchJson: function (path) { return fetch(API + path, { cache: 'no-store' }).then(function (r) { if (!r.ok) throw new Error(String(r.status)); return r.json(); }); },
        poll: function () {
            var s = this;
            if (s.pollBusy) return;
            s.pollBusy = true;
            Promise.allSettled([s.fetchJson('/api/v1/stats'), s.fetchJson('/health')]).then(function (results) {
                var now = Date.now();
                if (results[0].status === 'fulfilled') {
                    var nextStats = results[0].value, total = Number(nextStats.totalTorrents) || 0;
                    s.st = nextStats;
                    if (s.previousMetadata && total < s.previousMetadata.value) s.metadataRate = null;
                    if (!s.previousMetadata || total !== s.previousMetadata.value) {
                        if (s.previousMetadata && total >= s.previousMetadata.value) s.metadataRate = (total - s.previousMetadata.value) / ((now - s.previousMetadata.at) / 1000);
                        s.previousMetadata = { value: total, at: now };
                    } else if (now - s.previousMetadata.at >= 15000) s.metadataRate = 0;
                }
                s.online = results[1].status === 'fulfilled' && results[1].value.status === 'healthy';
                if (results[1].status === 'fulfilled') {
                    var nextHealth = results[1].value, heat = Number(nextHealth.heat && nextHealth.heat.acceptedRecords) || 0;
                    s.health = nextHealth;
                    if (s.previousHeat) s.heatRate = heat >= s.previousHeat.value ? (heat - s.previousHeat.value) / ((now - s.previousHeat.at) / 1000) : null;
                    s.previousHeat = { value: heat, at: now };
                }
                s.lastUpdated = new Date(now).toLocaleTimeString(lang === 'zh' ? 'zh-CN' : 'en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
            }).finally(function () { s.pollBusy = false; s.pollTimer = setTimeout(function () { s.poll(); }, 3000); });
        },
        goSearch: function () { var q = this.q.trim(); if (q) { saveHistory(q); this.$router.push({ path: '/search', query: { q: q } }); } },
        goHistory: function (q) { this.q = q; saveHistory(q); this.$router.push({ path: '/search', query: { q: q } }); },
        lookupHash: function () { var h = this.hq.trim().toLowerCase(); if (h.length === 40 && /^[a-f0-9]{40}$/.test(h)) { this.$router.push('/torrent/' + h); } else if (h) { this.$router.push('/search?q=' + encodeURIComponent(h)); } }
    }
};

var SearchPage = {
    template: '<div class="result-header"><span class="result-count">{{ fmtNumber(total) }}</span> {{ T("found") }} — <template v-if="q">{{ T("results_for") }} <b>{{ q }}</b></template><b v-else>{{ T("hot") }}</b><div class="heat-meta" v-if="heatAsOfUtc">{{ T("heat_as_of") }} {{ fmtDate(heatAsOfUtc) }}<span v-if="heatCoverageHours!=null"> · {{ T("heat_coverage") }} {{ heatCoverageHours }}/{{ heatWindowHours }}h</span></div><div class="result-filters heat-windows"><span v-for="w in heatWindows" class="filter-chip" :class="{active:heatWindow===w}" @click="setHeat(w)">{{ w }}</span></div><div class="result-filters" v-if="q"><span v-for="f in filters" class="filter-chip" :class="{active:af===f.v}" @click="setF(f.v)">{{ f.l }}</span></div></div><div v-if="loading"><div class="skeleton skel-line long"></div><div class="skeleton skel-line long"></div><div class="skeleton skel-line mid"></div></div><div v-else-if="err" class="error-state">{{ err }} <button class="retry-btn" @click="fetch">{{ T("retry") }}</button></div><div v-else-if="items.length===0" class="empty-state">{{ T("no_results") }}<span v-if="q"> "{{ q }}"</span><br><span style="font-size:.82rem;color:var(--text-muted);">{{ T("no_results_hint") }}</span></div><div v-else><div v-for="item in items" :key="item.infoHash" class="result-item" @click="$router.push(\'/torrent/\'+item.infoHash)"><div class="result-title" v-html="highlightTerm(item.name,q)"></div><div class="result-tags"><span v-if="catInfo(item.name).cat" class="tag tag-cat">{{ catInfo(item.name).cat }}</span><span class="tag tag-size">{{ fmtSize(item.totalLength) }}</span><span v-if="item.fileCount>1" class="tag tag-files">{{ item.fileCount }} files</span><span v-if="heat(item)>0" class="tag tag-health">{{ fmtNumber(heat(item)) }} {{ T("network_activity") }} / {{ heatWindow }}</span></div><div class="result-footer"><span>{{ fmtRelative(item.createdAt) }}</span><span>{{ item.infoHash.slice(0,12) }}...</span><button class="result-copy-btn" @click.stop="cp(item)">🧲 {{ T("copy_magnet") }}</button></div></div></div><div v-if="total>pageSize" class="pagination"><button :disabled="page<=1" @click="goPage(page-1)">{{ T("prev_page") }}</button><span class="page-info">{{ page }} / {{ tp }}</span><button :disabled="page>=tp" @click="goPage(page+1)">{{ T("next_page") }}</button></div></div>',
    data: function () {
        return {
            items: [], total: 0, loading: true, err: '', af: '', heatAsOfUtc: '', heatCoverageHours: null,
            heatWindows: ['24h', '3d', '7d', '15d'],
            filters: [{ l: 'All', v: '' }, { l: '🎬 Video', v: 'mkv' }, { l: '🎵 Audio', v: 'mp3' }, { l: '📚 Books', v: 'pdf' }, { l: '📦 Archives', v: 'zip' }, { l: '🖼️ Images', v: 'jpg' }]
        };
    },
    computed: { q: function () { return (this.$route.query.q || '').trim(); }, heatWindow: function () { var w = String(this.$route.query.heatWindow || '7d'); return this.heatWindows.indexOf(w) >= 0 ? w : '7d'; }, heatWindowHours: function () { return this.heatWindow === '24h' ? 24 : parseInt(this.heatWindow, 10) * 24; }, page: function () { return Math.max(1, parseInt(this.$route.query.page) || 1); }, pageSize: function () { return 20; }, tp: function () { return Math.max(1, Math.ceil(this.total / this.pageSize)); } },
    watch: { '$route.fullPath': { immediate: true, handler: function () { this.fetch(); } } },
    methods: {
        T: T, fmtSize: fmtSize, fmtDate: fmtDate, fmtNumber: fmtNumber, fmtRelative: fmtRelative, catInfo: detectCategory, highlightTerm: highlightTerm,
        heat: function (item) { return heatValue(item, this.heatWindow); },
        setF: function (f) { this.af = this.af === f ? '' : f; this.fetch(); },
        setHeat: function (w) { this.$router.push({ path: this.q ? '/search' : '/hot', query: { q: this.q || undefined, heatWindow: w } }); },
        fetch: function () {
            var s = this; s.loading = true; s.err = '';
            if (s.q) saveHistory(s.q); document.title = (s.q || T('hot')) + ' - Cherry';
            var p = new URLSearchParams({ q: s.q, page: s.page, size: s.pageSize, heatWindow: s.heatWindow }); if (s.af && s.q) p.set('fileType', s.af);
            fetch(API + '/api/v1/torrents/search?' + p).then(function (r) { if (!r.ok) throw new Error('Search failed'); return r.json(); }).then(function (d) { s.items = d.items || []; s.total = d.total || 0; s.heatAsOfUtc = d.heatAsOfUtc || ''; s.heatCoverageHours = d.heatCoverageHours == null ? null : d.heatCoverageHours; }).catch(function (e) { s.err = e.message; }).finally(function () { s.loading = false; });
        },
        goPage: function (p) { this.$router.push({ path: this.q ? '/search' : '/hot', query: { q: this.q || undefined, page: p, heatWindow: this.heatWindow } }); },
        cp: function (item) { copyText(magnetLink(item.infoHash, item.name)); }
    }
};

var DetailPage = {
    template: '<div><a class="back-link" @click="$router.back()">← {{ T("back") }}</a><div v-if="loading"><div class="skeleton skel-line long"></div><div class="skeleton skel-line mid"></div></div><div v-else-if="err" class="error-state">{{ err }} <button class="retry-btn" @click="load">{{ T("retry") }}</button></div><div v-else><div class="detail-title">{{ t.name }}</div><div class="detail-meta"><span>{{ fmtSize(t.totalLength) }}</span><span>{{ t.fileCount }} files</span><span>{{ fmtDate(t.createdAt) }}</span><span v-if="catInfo(t.name).cat">{{ catInfo(t.name).cat }}</span></div><div class="detail-actions"><a :href="magnet" class="btn-primary">🧲 {{ T("download_magnet") }}</a><button class="btn-secondary" :class="{copied:cpd}" @click="cp">{{ cpd?"✓ "+T("copied"):T("copy_link") }}</button></div><div class="hash-row"><span>{{ t.infoHash }}</span><button class="hash-copy-btn" @click="cph">{{ T("copy_link") }}</button></div><div v-if="t.files&&t.files.length" class="file-section"><div class="file-header"><h3>{{ T("file_list") }} <span style="color:var(--text-muted);font-weight:400;">({{ t.files.length }})</span></h3><input class="file-search" :placeholder="T(\'filter_files\')" v-model="ff" /></div><table class="file-table"><thead><tr><th class="file-num-col">#</th><th class="sortable" @click="sf(\'name\')">{{ T("sort_name") }}{{ sb==="name"?(sa?" ↑":" ↓"):"" }}</th><th class="sortable file-size-col" @click="sf(\'size\')">{{ T("sort_size") }}{{ sb==="size"?(sa?" ↑":" ↓"):"" }}</th></tr></thead><tbody><tr v-for="(f,idx) in pf" :key="f.pathText"><td class="file-num-col">{{ idx+1+(fp-1)*50 }}</td><td><span class="file-type-icon">{{ fileIcon(f.pathText) }}</span>{{ f.pathText }}</td><td class="file-size-col">{{ fmtSize(f.length) }}</td></tr></tbody></table><div v-if="fi.length>50" class="pagination"><button :disabled="fp<=1" @click="fp--">{{ T("prev_page") }}</button><span class="page-info">{{ fp }}/{{ ftp }}</span><button :disabled="fp>=ftp" @click="fp++">{{ T("next_page") }}</button></div></div></div></div>',
    data: function () { return { t: {}, loading: true, err: '', cpd: false, ff: '', fp: 1, sb: '', sa: true }; },
    computed: {
        magnet: function () { return magnetLink(this.t.infoHash, this.t.name); },
        fi: function () {
            var s = this, l = (s.t.files || []).slice(0, 500);
            if (s.ff) { var q = s.ff.toLowerCase(); l = l.filter(function (f) { return f.pathText.toLowerCase().indexOf(q) >= 0; }); }
            if (s.sb === 'name') l = l.slice().sort(function (a, b) { return s.sa ? a.pathText.localeCompare(b.pathText) : b.pathText.localeCompare(a.pathText); });
            else if (s.sb === 'size') l = l.slice().sort(function (a, b) { return s.sa ? a.length - b.length : b.length - a.length; });
            return l;
        },
        pf: function () { var s = (this.fp - 1) * 50; return this.fi.slice(s, s + 50); },
        ftp: function () { return Math.max(1, Math.ceil(this.fi.length / 50)); }
    },
    watch: { ff: function () { this.fp = 1; } },
    mounted: function () { this.load(); },
    methods: {
        T: T, fmtSize: fmtSize, fmtDate: fmtDate, fileIcon: fileIcon, catInfo: detectCategory,
        load: function () {
            var s = this; s.loading = true; s.err = '';
            fetch(API + '/api/v1/torrents/' + this.$route.params.infoHash).then(function (r) { if (!r.ok) throw new Error(r.status === 404 ? T('torrent_not_found') : T('server_error')); return r.json(); }).then(function (d) { s.t = d; document.title = d.name + ' - Cherry'; }).catch(function (e) { s.err = e.message; }).finally(function () { s.loading = false; });
        },
        cp: function () { var s = this; copyText(s.magnet); s.cpd = true; setTimeout(function () { s.cpd = false; }, 2000); },
        cph: function () { copyText(this.t.infoHash); },
        sf: function (f) { if (this.sb === f) { this.sa = !this.sa; } else { this.sb = f; this.sa = true; } }
    }
};

var RecentPage = {
    template: '<div class="result-header"><span class="result-count">{{ items.length }}</span> recent torrents</div><div v-if="loading"><div class="skeleton skel-line long"></div><div class="skeleton skel-line mid"></div></div><div v-else-if="err" class="error-state">{{ err }}</div><div v-else><div v-for="item in items" :key="item.infoHash" class="result-item" @click="$router.push(\'/torrent/\'+item.infoHash)"><div class="result-title">{{ item.name }}</div><div class="result-tags"><span v-if="catInfo(item.name).cat" class="tag tag-cat">{{ catInfo(item.name).cat }}</span><span class="tag tag-size">{{ fmtSize(item.totalLength) }}</span><span v-if="item.fileCount>1" class="tag tag-files">{{ item.fileCount }} files</span></div><div class="result-footer"><span>{{ fmtRelative(item.createdAt) }}</span><span>{{ item.infoHash.slice(0,12) }}...</span><button class="result-copy-btn" @click.stop="cp(item)">🧲 {{ T("copy_magnet") }}</button></div></div></div></div>',
    data: function () { return { items: [], loading: true, err: '' }; },
    mounted: function () { var s = this; document.title = 'Recent - Cherry'; fetch(API + '/api/v1/torrents/recent').then(function (r) { return r.json(); }).then(function (d) { s.items = d || []; }).catch(function (e) { s.err = e.message; }).finally(function () { s.loading = false; }); },
    methods: { T: T, fmtSize: fmtSize, fmtRelative: fmtRelative, catInfo: detectCategory, cp: function (i) { copyText(magnetLink(i.infoHash, i.name)); } }
};

// ---- Router ----
var router = VueRouter.createRouter({ history: VueRouter.createWebHistory(), routes: [{ path: '/', component: HomePage }, { path: '/search', component: SearchPage }, { path: '/hot', component: SearchPage }, { path: '/recent', component: RecentPage }, { path: '/torrent/:infoHash', component: DetailPage }] });

// ---- Root App ----
var App = {
    template: '<nav class="topbar"><a href="/" class="logo">🍒 Cherry</a><div class="search-wrap" v-if="showSearch"><input type="text" class="search-input" :placeholder="T(\'search_placeholder\')" v-model="q" @keydown.enter="doSearch" ref="navInp" /><button class="search-btn" @click="doSearch">{{ T("search_btn") }}</button></div><div class="nav-links"><a href="/hot">{{ T("hot") }}</a><a href="/recent">Recent</a></div><span class="lang-switch" @click="switchLang">{{ T("lang_label") }}</span></nav><main class="main"><router-view /></main>',
    data: function () { return { q: '', lang: lang }; },
    computed: { showSearch: function () { return this.$route.path !== '/'; } },
    watch: { '$route.query.q': function (v) { if (v) this.q = v; } },
    mounted: function () { var s = this; document.addEventListener('keydown', function (e) { if ((e.ctrlKey || e.metaKey) && e.key === 'k') { e.preventDefault(); s.$refs.navInp && s.$refs.navInp.focus(); } }); },
    methods: { T: T, switchLang: switchLang, doSearch: function () { var q = this.q.trim(); if (!q) return; saveHistory(q); this.$router.push({ path: '/search', query: { q: q } }); } }
};

var app = Vue.createApp(App);
app.use(router);
app.mount('#app');
