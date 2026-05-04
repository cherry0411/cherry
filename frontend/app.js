var API = window.CHERRY_API || '';

// ---- i18n ----
var LANG_KEY = 'cherry_lang';
var langs = ['zh','en'];
var t = {};
var lang = (function() {
    try { var v = localStorage.getItem(LANG_KEY); if (langs.indexOf(v) >= 0) return v; }
    catch(e) {}
    return (navigator.language || '').startsWith('zh') ? 'zh' : 'en';
})();

var dict = {
    zh: {
        site_title: 'Cherry - DHT 搜索引擎',
        search_placeholder: '搜索种子...',
        search_btn: '搜索',
        hero_title: 'Cherry 种子搜索',
        hero_desc: '实时搜索 DHT 网络中发现的数千万个种子',
        hero_input_placeholder: '电影、剧集、软件、音乐...',
        stats_total: '种子总数',
        stats_today: '今日新增',
        stats_cache: '内存缓存',
        stats_date: '服务器日期',
        nav_recent: '最新',
        recent_title: '最近新增',
        recent_searches: '最近搜索',
        results_for: '结果',
        found: '找到',
        searching: '搜索中',
        no_results: '未找到结果',
        no_results_hint: '尝试换一个关键词',
        retry: '重试',
        prev_page: '上一页',
        next_page: '下一页',
        copy_magnet: '复制',
        back: '返回结果',
        torrent_not_found: '种子未找到',
        server_error: '服务器错误',
        info_hash: 'Info Hash',
        total_size: '大小',
        file_count: '文件数',
        discovered: '发现时间',
        download_magnet: '下载磁力链接',
        copy_link: '复制链接',
        copied: '已复制',
        file_list: '文件列表',
        files_shown: '个文件，仅展示前',
        more_files: '... 还有',
        no_more: '个文件未展示',
        filter_files: '筛选文件...',
        hash_copied: 'Hash 已复制',
        link_copied: '链接已复制',
        press_ctrl_k: '按 Ctrl+K 快速搜索',
        network_stats: '网络统计',
        filters_all: '全部',
        filters_video: '视频',
        filters_audio: '音频',
        filters_books: '图书',
        filters_archives: '压缩包',
        filters_images: '图片',
        file_col_name: '文件名',
        file_col_size: '大小',
        private_torrent: '私有种子',
        just_now: '刚刚',
        min_ago: '分钟前',
        h_ago: '小时前',
        d_ago: '天前',
        mo_ago: '个月前',
        sort_name: '文件名',
        sort_size: '大小',
        lang_label: 'En'
    },
    en: {
        site_title: 'Cherry - DHT Search Engine',
        search_placeholder: 'Search torrents...',
        search_btn: 'Search',
        hero_title: 'Cherry Torrent Search',
        hero_desc: 'Search millions of torrents discovered from the DHT network in real-time',
        hero_input_placeholder: 'Movie, TV show, software, music...',
        stats_total: 'Total Torrents',
        stats_today: 'Discovered Today',
        stats_cache: 'Memory Cache',
        stats_date: 'Server Date',
        nav_recent: 'Recent',
        recent_title: 'Recently Added',
        recent_searches: 'Recent Searches',
        results_for: 'Results for',
        found: 'found',
        searching: 'Searching',
        no_results: 'No results found',
        no_results_hint: 'Try different keywords',
        retry: 'Retry',
        prev_page: 'Prev',
        next_page: 'Next',
        copy_magnet: 'Copy',
        back: 'Back to results',
        torrent_not_found: 'Torrent not found',
        server_error: 'Server error',
        info_hash: 'Info Hash',
        total_size: 'Size',
        file_count: 'Files',
        discovered: 'Discovered',
        download_magnet: 'Download Magnet',
        copy_link: 'Copy Link',
        copied: 'Copied',
        file_list: 'File List',
        files_shown: 'files, showing first',
        more_files: '... and',
        no_more: 'more files not shown',
        filter_files: 'Filter files...',
        hash_copied: 'Hash copied',
        link_copied: 'Link copied',
        press_ctrl_k: 'Press Ctrl+K to search anytime',
        network_stats: 'Network Stats',
        filters_all: 'All',
        filters_video: 'Video',
        filters_audio: 'Audio',
        filters_books: 'Books',
        filters_archives: 'Archives',
        filters_images: 'Images',
        file_col_name: 'Name',
        file_col_size: 'Size',
        private_torrent: 'Private',
        just_now: 'just now',
        min_ago: 'm ago',
        h_ago: 'h ago',
        d_ago: 'd ago',
        mo_ago: 'mo ago',
        sort_name: 'Name',
        sort_size: 'Size',
        lang_label: '中文'
    }
};

function T(key) { return (dict[lang] && dict[lang][key]) || (dict.en[key]) || key; }

function switchLang() {
    lang = lang === 'zh' ? 'en' : 'zh';
    try { localStorage.setItem(LANG_KEY, lang); } catch(e) {}
    window.location.reload();
}

// ---- utility ----
function fmtSize(bytes) {
    if (bytes == null || bytes <= 0) return '0 B';
    var u = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), u.length - 1);
    return (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + ' ' + u[i];
}

function fmtDate(iso) {
    if (!iso) return '-';
    var d = new Date(iso);
    return d.toLocaleDateString('zh-CN',{year:'numeric',month:'2-digit',day:'2-digit'})
        + ' ' + d.toLocaleTimeString('zh-CN',{hour:'2-digit',minute:'2-digit'});
}

function fmtRelative(iso) {
    if (!iso) return '';
    var diff = Date.now() - new Date(iso).getTime();
    if (diff < 0) return '';
    var mins = Math.floor(diff / 60000);
    if (mins < 1) return T('just_now');
    if (mins < 60) return mins + T('min_ago');
    var h = Math.floor(mins / 60);
    if (h < 24) return h + T('h_ago');
    var d = Math.floor(h / 24);
    if (d < 30) return d + T('d_ago');
    return Math.floor(d / 30) + T('mo_ago');
}

function fmtNumber(n) {
    if (n == null) return '0';
    return Number(n).toLocaleString('zh-CN');
}

function magnetLink(hash, name) {
    return 'magnet:?xt=urn:btih:' + hash + '&dn=' + encodeURIComponent(name || '');
}

function escapeHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function highlightTerm(text, query) {
    if (!query || !text) return escapeHtml(text);
    var escaped = escapeHtml(text);
    var q = escapeHtml(query).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return escaped.replace(new RegExp('(' + q + ')', 'gi'), '<mark>$1</mark>');
}

function detectCategory(name) {
    if (!name) return { icon: '📄', cat: '' };
    var n = name.toLowerCase();
    if (/s\d{1,2}e\d|season|episode|ova|oad/i.test(n)) return { icon: '📺', cat: 'Series' };
    if (/\.(flac|mp3|m4a|aac|ogg|wav|24bit|96khz|album|discography)/i.test(n)) return { icon: '🎵', cat: 'Music' };
    if (/\.(iso|dmg|exe|msi|appx|rar|zip|7z|deb|rpm|apk)/i.test(n) || /(windows|macos|linux|ubuntu|debian|fedora|android|ios|setup|install|portable|crack|patch|keygen)/i.test(n)) return { icon: '💿', cat: 'Software' };
    if (/game|gog|steam|epic|repack|fitgirl|dodi/i.test(n)) return { icon: '🎮', cat: 'Game' };
    if (/\.(epub|pdf|mobi|azw|djvu|cbz|cbr|chm)/i.test(n) || /ebook|book|magazine/i.test(n)) return { icon: '📚', cat: 'Books' };
    if (/anime|s\d{2}e\d/i.test(n)) return { icon: '🎌', cat: 'Anime' };
    if (/\.(mkv|mp4|avi|mov|wmv|webm|bluray|web-dl|webrip|bdrip|brrip|hdtv|dvdrip|1080p|2160p|720p|2160|hdr|x265|x264|hevc|avc)/i.test(n)) return { icon: '🎬', cat: 'Movie' };
    return { icon: '📦', cat: '' };
}

function fileIcon(filename) {
    if (!filename) return '📄';
    var ext = filename.split('.').pop().toLowerCase();
    var map = {
        mkv:'🎬',mp4:'🎬',avi:'🎬',mov:'🎬',wmv:'🎬',webm:'🎬',
        flac:'🎵',mp3:'🎵',m4a:'🎵',aac:'🎵',ogg:'🎵',wav:'🎵',
        iso:'💿',dmg:'💿',exe:'⚙️',msi:'⚙️',rar:'📦',zip:'📦','7z':'📦',tar:'📦',gz:'📦',
        pdf:'📕',epub:'📚',mobi:'📚',djvu:'📚',
        jpg:'🖼️',jpeg:'🖼️',png:'🖼️',gif:'🖼️',webp:'🖼️',bmp:'🖼️',
        txt:'📝',nfo:'📝',srt:'💬',ass:'💬',sub:'💬',sfv:'✅',md5:'✅',sha1:'✅'
    };
    return map[ext] || '📄';
}

// ---- Search history ----
var HISTORY_KEY = 'cherry_search_history';
function loadHistory() { try { return JSON.parse(localStorage.getItem(HISTORY_KEY))||[]; } catch(e) { return []; } }
function saveHistory(q) {
    var h = loadHistory().filter(function(x){return x!==q;});
    h.unshift(q); if (h.length > 10) h.length = 10;
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(h)); } catch(e) {}
}

// ---- Toast ----
var toastTimer;
function showToast(msg) {
    var el = document.getElementById('toast');
    if (!el) { el = document.createElement('div'); el.id='toast'; el.className='toast'; document.body.appendChild(el); }
    el.textContent = msg; el.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function(){el.classList.remove('show');}, 2000);
}
function copyText(text, msg) { try { navigator.clipboard.writeText(text); showToast(msg||'Copied'); } catch(e) { showToast(msg||'Copied'); } }

// ---- Components ----

var HomePage = {
    template: '\
<div>\
    <div class="hero">\
        <div class="hero-icon">🍒</div>\
        <h1>{{ T("hero_title") }}</h1>\
        <p>{{ T("hero_desc") }}</p>\
        <div class="hero-search">\
            <input class="search-input" :placeholder="T(\'hero_input_placeholder\')" v-model="q" @keydown.enter="goSearch" ref="heroInput" />\
            <button class="btn-search" @click="goSearch">{{ T("search_btn") }}</button>\
        </div>\
        <div v-if="history.length" class="search-history">\
            <span v-for="h in history" :key="h" class="history-chip" @click="goHistory(h)">{{ h }}</span>\
        </div>\
    </div>\
    <div class="stats-section">\
        <h2>{{ T("network_stats") }}</h2>\
        <div v-if="!stats.totalTorrents && !done" class="skel-loading">\
            <div class="stats-grid">\
                <div class="stat-card" v-for="_ in 4"><div class="skeleton" style="height:48px;"></div></div>\
            </div>\
        </div>\
        <div v-else class="stats-grid">\
            <div class="stat-card"><div class="stat-value counting">{{ fmtNumber(stats.totalTorrents) }}</div><div class="stat-label">{{ T("stats_total") }}</div></div>\
            <div class="stat-card"><div class="stat-value counting">+{{ fmtNumber(stats.todayNew) }}</div><div class="stat-label">{{ T("stats_today") }}</div></div>\
            <div class="stat-card"><div class="stat-value counting">{{ fmtNumber(stats.dedupFilterSize) }}</div><div class="stat-label">{{ T("stats_cache") }}</div></div>\
            <div class="stat-card"><div class="stat-value">{{ (stats.serverTime||"").slice(0,10) }}</div><div class="stat-label">{{ T("stats_date") }}</div></div>\
        </div>\
    </div>\
    <div class="footer">{{ T("press_ctrl_k") }}</div>\
</div>',
    data: function(){ return { stats:{}, done:false, q:'', history:loadHistory() }; },
    mounted: function(){
        var self = this;
        this.$refs.heroInput && this.$refs.heroInput.focus();
        fetch(API + '/api/v1/stats').then(function(r){return r.ok?r.json():null;}).then(function(s){if(s)self.stats=s;}).catch(function(){}).finally(function(){self.done=true;});
    },
    methods: {
        T:T, fmtNumber:fmtNumber,
        goSearch: function(){ var q=this.q.trim(); if(q){saveHistory(q);this.$router.push({path:'/search',query:{q:q}});} },
        goHistory: function(q){ this.q=q; saveHistory(q); this.$router.push({path:'/search',query:{q:q}}); }
    }
};

var SearchPage = {
    template: '\
<div>\
    <div class="search-header">\
        <h2>{{ T("results_for") }} <span class="highlight">"{{ q }}"</span></h2>\
        <div class="search-meta">{{ fmtNumber(total) }} {{ T("found") }}</div>\
        <div class="search-actions">\
            <span v-for="ft in filters" :key="ft.value" class="filter-chip" :class="{active:activeFilter===ft.value}" @click="setFilter(ft.value)">{{ ft.label }}</span>\
        </div>\
    </div>\
    <div v-if="loading"><div class="skel-card" v-for="_ in 5"><div class="skeleton skel-line long"></div><div class="skeleton skel-line mid"></div><div class="skeleton skel-line short"></div></div></div>\
    <div v-else-if="error" class="error-state"><p>{{ error }}</p><button class="btn-retry" @click="fetchData">{{ T("retry") }}</button></div>\
    <div v-else-if="items.length===0" class="empty-state"><div class="empty-icon">🔍</div><p>{{ T("no_results") }} "{{ q }}"</p><p style="font-size:.82rem;color:var(--text-muted);">{{ T("no_results_hint") }}</p></div>\
    <div v-else class="result-list">\
        <div v-for="item in items" :key="item.infoHash" class="result-card" @click="$router.push(\'/torrent/\' + item.infoHash)">\
            <div class="result-top">\
                <div class="result-icon">{{ catInfo(item.name).icon }}</div>\
                <div class="result-body">\
                    <div class="result-name" v-html="highlightTerm(item.name,q)"></div>\
                    <div class="result-tags">\
                        <span v-if="catInfo(item.name).cat" class="tag tag-cat">{{ catInfo(item.name).cat }}</span>\
                        <span class="tag tag-size">{{ fmtSize(item.totalLength) }}</span>\
                        <span v-if="item.fileCount>1" class="tag tag-files">{{ item.fileCount }} files</span>\
                        <span v-if="item.isPrivate" class="tag tag-private">{{ T("private_torrent") }}</span>\
                    </div>\
                </div>\
            </div>\
            <div class="result-footer">\
                <span>{{ fmtRelative(item.createdAt) }} &middot; <span class="result-hash">{{ item.infoHash.slice(0,12) }}...</span></span>\
                <button class="result-copy" @click.stop="copyMagnet(item)">🧲 {{ T("copy_magnet") }}</button>\
            </div>\
        </div>\
    </div>\
    <div v-if="total>pageSize" class="pagination">\
        <button :disabled="page<=1" @click="goPage(page-1)">&larr; {{ T("prev_page") }}</button>\
        <template v-for="p in pageNumbers">\
            <button v-if="p===\'...\'" class="page-dots" disabled>...</button>\
            <button v-else :class="{current:p===page}" @click="goPage(p)">{{ p }}</button>\
        </template>\
        <button :disabled="page>=totalPages" @click="goPage(page+1)">{{ T("next_page") }} &rarr;</button>\
    </div>\
</div>',
    data: function(){
        return {
            items:[], total:0, loading:true, error:'', activeFilter:'',
            filters:[
                {label:T('filters_all'),value:''},
                {label:'🎬 '+T('filters_video'),value:'mkv'},
                {label:'🎵 '+T('filters_audio'),value:'mp3'},
                {label:'📚 '+T('filters_books'),value:'pdf'},
                {label:'📦 '+T('filters_archives'),value:'zip'},
                {label:'🖼️ '+T('filters_images'),value:'jpg'}
            ]
        };
    },
    computed: {
        q: function(){ return (this.$route.query.q||'').trim(); },
        page: function(){ return Math.max(1,parseInt(this.$route.query.page)||1); },
        pageSize: function(){ return 20; },
        totalPages: function(){ return Math.max(1,Math.ceil(this.total/this.pageSize)); },
        pageNumbers: function(){
            var p=[],tp=this.totalPages,cp=this.page;
            var s=Math.max(1,cp-2),e=Math.min(tp,cp+2);
            if(s>1){p.push(1);if(s>2)p.push('...');}
            for(var i=s;i<=e;i++)p.push(i);
            if(e<tp){if(e<tp-1)p.push('...');p.push(tp);}
            return p;
        }
    },
    watch: {
        '$route.query.q':{immediate:true,handler:function(){this.fetchData();}},
        '$route.query.page':{handler:function(){this.fetchData();}}
    },
    methods: {
        T:T, fmtSize:fmtSize, fmtDate:fmtDate, fmtNumber:fmtNumber, fmtRelative:fmtRelative,
        catInfo:detectCategory, highlightTerm:highlightTerm,
        setFilter: function(ft){ this.activeFilter = this.activeFilter===ft?'':ft; this.fetchData(); },
        fetchData: function(){
            var self=this; self.loading=true; self.error='';
            if(!self.q){self.loading=false;return;}
            saveHistory(self.q);
            var p=new URLSearchParams({q:self.q,page:self.page,size:self.pageSize});
            if(self.activeFilter)p.set('fileType',self.activeFilter);
            fetch(API+'/api/v1/torrents/search?'+p)
                .then(function(r){if(!r.ok)throw new Error('Search failed ('+r.status+')');return r.json();})
                .then(function(d){self.items=d.items||[];self.total=d.total||0;})
                .catch(function(e){self.error=e.message;})
                .finally(function(){self.loading=false;});
        },
        goPage: function(p){ this.$router.push({path:'/search',query:{q:this.q,page:p}}); window.scrollTo({top:0,behavior:'smooth'}); },
        copyMagnet: function(item){ copyText(magnetLink(item.infoHash,item.name), T('link_copied')); }
    }
};

var DetailPage = {
    template: '\
<div>\
    <a class="back-link" @click="$router.back()">← {{ T("back") }}</a>\
\
    <div v-if="loading">\
        <div class="skel-card"><div class="skeleton skel-line" style="width:70%;height:22px;margin-bottom:12px;"></div><div class="skeleton skel-line mid"></div><div class="skeleton skel-line short"></div></div>\
    </div>\
\
    <div v-else-if="error" class="error-state"><p>{{ error }}</p><button class="btn-retry" @click="loadDetail">{{ T("retry") }}</button></div>\
\
    <div v-else>\
        <div class="detail-hero">\
            <div class="detail-icon">{{ catInfo(torrent.name).icon }}</div>\
            <div class="detail-title-area">\
                <div class="detail-title">{{ torrent.name }}</div>\
                <div class="detail-subtitle">\
                    <span>{{ fmtRelative(torrent.createdAt) }}</span>\
                    <span v-if="torrent.isPrivate" class="tag tag-private">{{ T("private_torrent") }}</span>\
                    <span v-if="catInfo(torrent.name).cat" class="tag tag-cat">{{ catInfo(torrent.name).cat }}</span>\
                </div>\
            </div>\
        </div>\
\
        <div class="detail-stats-row">\
            <div class="detail-stat"><span class="ds-icon">📦</span><span class="ds-value size">{{ fmtSize(torrent.totalLength) }}</span></div>\
            <div class="detail-stat"><span class="ds-icon">📄</span><span class="ds-value">{{ torrent.fileCount }}</span><span class="ds-label">files</span></div>\
            <div class="detail-stat"><span class="ds-icon">🕐</span><span class="ds-value">{{ fmtDate(torrent.createdAt) }}</span></div>\
        </div>\
\
        <div class="detail-actions-row">\
            <a :href="magnet" class="btn-magnet">🧲 {{ T("download_magnet") }}</a>\
            <button class="btn-copy" :class="{copied:copied}" @click="copyMagnet">{{ copied?\'✓ \'+T("copied"):\'📋 \'+T("copy_link") }}</button>\
        </div>\
\
        <div class="hash-box">\
            <span class="hash-text">{{ torrent.infoHash }}</span>\
            <button class="hash-copy" @click="copyHash">{{ T("copy_link") }}</button>\
        </div>\
\
        <div v-if="torrent.files && torrent.files.length" class="file-section">\
            <div class="file-section-header">\
                <h3>📂 {{ T("file_list") }} <span class="file-count-badge">({{ torrent.files.length }})</span></h3>\
                <input class="file-search" :placeholder="T(\'filter_files\')" v-model="fileFilter" />\
            </div>\
            <div class="file-table-wrapper">\
                <table class="file-table">\
                    <thead>\
                        <tr>\
                            <th style="width:40px;text-align:right;">#</th>\
                            <th class="sortable" @click="sortFiles(\'name\')">{{ T("sort_name") }}{{ sortBy==="name"?(sortAsc?" ↑":" ↓"):"" }}</th>\
                            <th class="sortable" @click="sortFiles(\'size\')" style="text-align:right;width:100px;">{{ T("sort_size") }}{{ sortBy==="size"?(sortAsc?" ↑":" ↓"):"" }}</th>\
                        </tr>\
                    </thead>\
                    <tbody>\
                        <tr v-for="(f,idx) in pagedFiles" :key="f.pathText">\
                            <td class="file-row-num">{{ idx + 1 + (filePage-1)*filePageSize }}</td>\
                            <td><span class="file-row-icon">{{ fileIcon(f.pathText) }}</span>{{ f.pathText }}</td>\
                            <td class="file-row-size">{{ fmtSize(f.length) }}</td>\
                        </tr>\
                    </tbody>\
                </table>\
            </div>\
            <div v-if="filteredFiles.length > filePageSize" class="pagination">\
                <button :disabled="filePage<=1" @click="filePage--">&larr;</button>\
                <span style="padding:0 12px;font-size:.85rem;color:var(--text-dim);">{{ filePage }} / {{ fileTotalPages }}</span>\
                <button :disabled="filePage>=fileTotalPages" @click="filePage++">&rarr;</button>\
            </div>\
            <div v-if="filteredFiles.length > 500" class="file-truncated" style="margin-top:8px;">{{ T("files_shown") }} 500 {{ T("no_more") }}</div>\
        </div>\
    </div>\
</div>',
    data: function(){ return { torrent:{}, loading:true, error:'', copied:false, fileFilter:'', filePage:1, filePageSize:50, sortBy:'', sortAsc:true }; },
    computed: {
        magnet: function(){ return magnetLink(this.torrent.infoHash, this.torrent.name); },
        allFiles: function(){ return (this.torrent.files||[]).slice(0,500); },
        filteredFiles: function(){
            var self=this;
            var list = self.allFiles;
            if (self.fileFilter) {
                var q = self.fileFilter.toLowerCase();
                list = list.filter(function(f){ return f.pathText.toLowerCase().indexOf(q)>=0; });
            }
            if (self.sortBy === 'name') {
                list = list.slice().sort(function(a,b){ return self.sortAsc ? a.pathText.localeCompare(b.pathText) : b.pathText.localeCompare(a.pathText); });
            } else if (self.sortBy === 'size') {
                list = list.slice().sort(function(a,b){ return self.sortAsc ? a.length - b.length : b.length - a.length; });
            }
            return list;
        },
        pagedFiles: function(){
            var s = (this.filePage-1) * this.filePageSize;
            return this.filteredFiles.slice(s, s + this.filePageSize);
        },
        fileTotalPages: function(){ return Math.max(1, Math.ceil(this.filteredFiles.length / this.filePageSize)); }
    },
    watch: { fileFilter: function(){ this.filePage = 1; } },
    mounted: function(){ this.loadDetail(); },
    methods: {
        T:T, fmtSize:fmtSize, fmtDate:fmtDate, fmtRelative:fmtRelative, fileIcon:fileIcon, catInfo:detectCategory,
        loadDetail: function(){
            var self=this; self.loading=true; self.error='';
            fetch(API + '/api/v1/torrents/' + this.$route.params.infoHash)
                .then(function(r){if(!r.ok)throw new Error(r.status===404?T('torrent_not_found'):T('server_error'));return r.json();})
                .then(function(d){self.torrent=d;})
                .catch(function(e){self.error=e.message;})
                .finally(function(){self.loading=false;});
        },
        copyMagnet: function(){ var self=this; copyText(this.magnet, T('link_copied')); self.copied=true; setTimeout(function(){self.copied=false;},2000); },
        copyHash: function(){ copyText(this.torrent.infoHash, T('hash_copied')); },
        sortFiles: function(field){
            if (this.sortBy === field) { this.sortAsc = !this.sortAsc; }
            else { this.sortBy = field; this.sortAsc = true; }
        }
    }
};

var RecentPage = {
    template: '\
<div>\
    <h2 style="font-size:1.15rem;font-weight:600;margin-bottom:14px;">{{ T("recent_title") }}</h2>\
    <div v-if="loading">\
        <div class="skel-card" v-for="_ in 5"><div class="skeleton skel-line long"></div><div class="skeleton skel-line mid"></div><div class="skeleton skel-line short"></div></div>\
    </div>\
    <div v-else-if="error" class="error-state"><p>{{ error }}</p><button class="btn-retry" @click="fetchData">{{ T("retry") }}</button></div>\
    <div v-else class="result-list">\
        <div v-for="item in items" :key="item.infoHash" class="result-card" @click="$router.push(\'/torrent/\' + item.infoHash)">\
            <div class="result-top">\
                <div class="result-icon">{{ catInfo(item.name).icon }}</div>\
                <div class="result-body">\
                    <div class="result-name">{{ item.name }}</div>\
                    <div class="result-tags">\
                        <span v-if="catInfo(item.name).cat" class="tag tag-cat">{{ catInfo(item.name).cat }}</span>\
                        <span class="tag tag-size">{{ fmtSize(item.totalLength) }}</span>\
                        <span v-if="item.fileCount>1" class="tag tag-files">{{ item.fileCount }} files</span>\
                        <span v-if="item.isPrivate" class="tag tag-private">{{ T("private_torrent") }}</span>\
                    </div>\
                </div>\
            </div>\
            <div class="result-footer">\
                <span>{{ fmtRelative(item.createdAt) }} &middot; <span class="result-hash">{{ item.infoHash.slice(0,12) }}...</span></span>\
                <button class="result-copy" @click.stop="copyMagnet(item)">🧲 {{ T("copy_magnet") }}</button>\
            </div>\
        </div>\
    </div>\
</div>',
    data: function(){ return { items:[], loading:true, error:'' }; },
    mounted: function(){ this.fetchData(); },
    methods: {
        T:T, fmtSize:fmtSize, fmtRelative:fmtRelative, catInfo:detectCategory,
        fetchData: function(){
            var self=this; self.loading=true;
            fetch(API+'/api/v1/torrents/recent')
                .then(function(r){if(!r.ok)throw new Error('Failed');return r.json();})
                .then(function(d){self.items=d||[];})
                .catch(function(e){self.error=e.message;})
                .finally(function(){self.loading=false;});
        },
        copyMagnet: function(item){ copyText(magnetLink(item.infoHash,item.name), T('link_copied')); }
    }
};

// ---- Router ----
var router = VueRouter.createRouter({
    history: VueRouter.createWebHistory(),
    routes:[
        { path:'/', component:HomePage },
        { path:'/search', component:SearchPage },
        { path:'/recent', component:RecentPage },
        { path:'/torrent/:infoHash', component:DetailPage }
    ]
});

// ---- Root App ----
var App = {
    template: '\
<nav class="topbar">\
    <a href="/" class="logo">🍒 Cherry</a>\
    <a href="/recent" class="nav-link">{{ T("nav_recent") }}</a>\
    <span style="flex:1;"></span>\
    <span class="lang-switch" @click="switchLang" :title="lang==\'zh\'?\'Switch to English\':\'切换到中文\'">{{ T("lang_label") }}</span>\
</nav>\
<main class="main"><router-view /></main>',
    data: function(){ return { lang:lang }; },
    methods: {
        T:T, switchLang:switchLang
    }
};

var app = Vue.createApp(App);
app.use(router);
app.mount('#app');
