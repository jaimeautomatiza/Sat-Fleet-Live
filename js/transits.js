/* ============================================================
   SatFleet Live - Sun & Moon transit finder (page module)
   Satellites: ISS, Tiangong (CSS) and Hubble. Free for everyone.
   ------------------------------------------------------------
   Load it in <head>:
     <script src="/js/transits.js" data-hide=".selectors, .to-hide"></script>
   Then, at the end of <body>:
     SatFleetTransits.init({ getLocation, getSatellites, changeLocation })
   The heavy maths lives in /js/transits-worker.js (Web Worker).
   ============================================================ */
(function (global) {
  'use strict';

  var CACHE_KEY = 'satfleet_transits_cache_v3';
  var ORIENT_KEY = 'satfleet_transits_orient';
  var RADIUS_KEY = 'satfleet_transits_radius';
  var SATS_KEY = 'satfleet_transits_sats';
  var OMM_KEY = 'satfleet_transit_omms_v2';

  // Satellites big enough to show up as a silhouette.
  // sizeKm = longest dimension, used for the apparent size.
  var SATS = [
    { id: 'iss', label: 'ISS',      full: 'International Space Station', norad: '25544', re: /^ISS \(ZARYA\)/i,  sizeKm: 0.109 },
    { id: 'css', label: 'Tiangong', full: 'Tiangong, the Chinese space station', norad: '48274', re: /^CSS \(TIANHE\)/i, sizeKm: 0.055 },
    { id: 'hst', label: 'Hubble',   full: 'Hubble Space Telescope', norad: '20580', re: /^HST$/i,           sizeKm: 0.0132 }
  ];
  function satInfo(id) { for (var i = 0; i < SATS.length; i++) if (SATS[i].id === id) return SATS[i]; return SATS[0]; }
  var RADII = [25, 50, 100, 200];
  var DEFAULT_RADIUS = 50;
  var DAYS = 7;
  var CACHE_MAX_AGE = 12 * 3600 * 1000;      // recalculate at least twice a day
  var RECOMPUTE_KM = 1;                       // moving further than this recalculates ("from you" numbers)
  var LESS_RELIABLE_MS = 48 * 3600 * 1000;
  var WORKER_URL = '/js/transits-worker.js';
  var TLE_URL = 'https://worker.satfleetlive.com/api/tle';
  var TILE_URL = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}';
  var DEFAULT_LOC = { lat: 40.4168, lng: -3.7038 };
  var DEG = Math.PI / 180;
  var EARTH_R = 6371.0088;
  var SUN_WARNING = 'Never look at the Sun, even briefly, without a certified solar filter. Serious eye damage can happen instantly.';

  // ---------- Icons (SVG only) ----------
  var ICON = {
    sun: '<circle cx="12" cy="12" r="4.2"/><path d="M12 2.5v2.2M12 19.3v2.2M2.5 12h2.2M19.3 12h2.2M5.3 5.3l1.6 1.6M17.1 17.1l1.6 1.6M5.3 18.7l1.6-1.6M17.1 6.9l1.6-1.6"/>',
    moon: '<path d="M20.5 14.2A8.5 8.5 0 1 1 9.8 3.5a6.8 6.8 0 0 0 10.7 10.7z"/>',
    sat: '<rect x="9.5" y="9.5" width="5" height="5" rx="1" transform="rotate(45 12 12)"/><path d="M8.2 8.2 4.6 4.6M15.8 15.8l3.6 3.6M3 8l5-5M16 21l5-5"/>',
    warn: '<path d="M10.3 3.9 1.8 18.2A2 2 0 0 0 3.5 21h17a2 2 0 0 0 1.7-2.8L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4.5"/><path d="M12 17.2h.01"/>',
    lock: '<rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7.5a4 4 0 0 1 8 0V11"/>',
    pin: '<path d="M12 21s-6.5-5.9-6.5-11.2a6.5 6.5 0 0 1 13 0C18.5 15.1 12 21 12 21z"/><circle cx="12" cy="9.8" r="2.4"/>',
    refresh: '<path d="M20.5 12a8.5 8.5 0 1 1-2.5-6"/><path d="M20.5 3.5V8H16"/>',
    nav: '<path d="M3.5 11 20.5 3.5 13 20.5l-2-7.5z"/>',
    clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>'
  };
  function svg(name, cls) {
    return '<svg class="' + (cls || 'sft-ic') + '" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' + ICON[name] + '</svg>';
  }

  // ---------- Styles (injected once, as early as possible) ----------
  var CSS = [
    '.sft-tabs{position:relative;z-index:1;display:flex;gap:4px;width:max-content;max-width:100%;padding:4px;margin:0 0 var(--sp-5,1.25rem);border:1px solid var(--border-hover,rgba(255,255,255,.13));border-radius:999px;background:var(--np-panel,rgba(19,21,26,.88));-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}',
    '.sft-tab{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:40px;padding:0 16px;border:0;border-radius:999px;background:transparent;color:var(--text-muted,#8a8f9e);font:inherit;font-size:var(--text-sm,.875rem);font-weight:600;line-height:1.1;cursor:pointer;white-space:nowrap;transition:background-color .16s,color .16s}',
    '.sft-tab .sft-ic{width:16px;height:16px}',
    '.sft-tab:hover{color:var(--text,#e8e9ec)}',
    '.sft-tab:focus-visible,.sft-panel :focus-visible{outline:2px solid var(--np-focus,#4da6ff);outline-offset:2px}',
    'html:not(.sft-show-transits) .sft-tab[data-sft-tab="passes"],html.sft-show-transits .sft-tab[data-sft-tab="transits"]{background:linear-gradient(135deg,#a333bc,#7b1fa2);color:#fff;box-shadow:0 4px 14px rgba(156,39,176,.35)}',
    '@media (max-width:600px){.sft-tabs{width:100%}.sft-tab{flex:1;padding:0 10px}}',

    '.sft-ic{width:18px;height:18px;flex-shrink:0;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}',
    '.sft-panel{display:none;position:relative;z-index:1;color:var(--text,#e8e9ec)}',
    '.sft-panel .sr-only{position:absolute!important;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}',
    'html.sft-show-transits .sft-panel{display:block}',

    '.sft-head{margin-bottom:var(--sp-5,1.25rem)}',
    '.sft-title{font-size:clamp(1.6rem,1.1rem + 2vw,2.5rem);font-weight:700;line-height:1.08;letter-spacing:-.02em;color:var(--text,#e8e9ec);max-width:18ch}',
    '.sft-sub{margin-top:10px;max-width:58ch;font-size:var(--text-base,1rem);line-height:1.55;color:var(--text-muted,#8a8f9e)}',
    '.sft-loc{display:inline-flex;align-items:center;gap:8px;max-width:100%;min-height:44px;margin-top:14px;padding:4px 4px 4px 12px;border-radius:999px;border:1px solid var(--np-blue-border,rgba(33,150,243,.38));background:var(--np-blue-dim,rgba(33,150,243,.1));font-size:var(--text-sm,.875rem)}',
    '.sft-loc .sft-ic{width:17px;height:17px;color:var(--np-blue,#2196f3)}',
    '.sft-loc strong{font-weight:600;font-variant-numeric:tabular-nums}',
    '.sft-loc.is-default{border-color:var(--np-amber-border,rgba(255,181,71,.5));background:var(--np-amber-dim,rgba(255,181,71,.1))}',
    '.sft-loc.is-default .sft-ic{color:var(--np-amber,#ffb547)}',
    '.sft-loc.is-pending{padding-right:16px;color:var(--text-muted,#8a8f9e);border-color:var(--border-hover,rgba(255,255,255,.13));background:var(--surface-2,#181b22)}',
    '.sft-loc-btn{height:36px;padding:0 16px;border:0;border-radius:999px;background:var(--np-blue-solid,#1976d2);color:#fff;font:inherit;font-size:var(--text-xs,.8125rem);font-weight:600;cursor:pointer;white-space:nowrap}',
    '.sft-loc.is-default .sft-loc-btn{background:var(--np-amber-solid,#ffb547);color:#2a1800}',

    '.sft-controls{display:flex;flex-wrap:wrap;align-items:flex-end;row-gap:16px;justify-content:space-between;gap:12px;padding:14px 16px;margin-bottom:14px;border:1px solid var(--border-hover,rgba(255,255,255,.13));border-radius:var(--r-xl,16px);background:var(--np-panel,rgba(19,21,26,.88))}',
    '.sft-radius{border:0;min-width:0}',
    '.sft-radius legend{margin-bottom:9px;font-size:.8125rem;font-weight:600;color:var(--text-muted,#8a8f9e)}',
    '.sft-seg{display:inline-flex;gap:2px;padding:3px;border:1px solid var(--border,rgba(255,255,255,.07));border-radius:12px;background:var(--surface-2,#181b22)}',
    '.sft-seg label{position:relative;display:block}',
    '.sft-seg input{position:absolute;opacity:0;inset:0;margin:0;cursor:pointer}',
    '.sft-seg span{display:flex;align-items:center;justify-content:center;min-width:64px;height:38px;padding:0 12px;border-radius:9px;font-size:var(--text-sm,.875rem);font-weight:600;font-variant-numeric:tabular-nums;color:var(--text-muted,#8a8f9e);cursor:pointer}',
    '.sft-seg input:checked + span{background:var(--purple-dim,rgba(156,39,176,.12));color:var(--text,#e8e9ec);box-shadow:inset 0 0 0 1px var(--purple-border,rgba(156,39,176,.45))}',
    '.sft-seg input:focus-visible + span{outline:2px solid var(--np-focus,#4da6ff);outline-offset:1px}',
    '.sft-recalc{display:inline-flex;align-items:center;gap:8px;height:44px;padding:0 16px;border:1px solid var(--border-hover,rgba(255,255,255,.13));border-radius:10px;background:transparent;color:var(--text,#e8e9ec);font:inherit;font-size:var(--text-sm,.875rem);font-weight:600;cursor:pointer}',
    '.sft-recalc:hover:not(:disabled){border-color:var(--purple-border,rgba(156,39,176,.45))}',
    '.sft-recalc:disabled{opacity:.5;cursor:progress}',
    '@media (max-width:600px){.sft-controls{flex-direction:column;align-items:stretch}.sft-seg{display:flex}.sft-seg label{flex:1}.sft-seg span{min-width:0;padding:0 6px}.sft-recalc{justify-content:center}}',

    '.sft-meta{display:grid;gap:10px;margin-bottom:18px}',
    '.sft-orbit{display:flex;align-items:center;gap:8px;font-size:var(--text-sm,.875rem);color:var(--text-muted,#8a8f9e)}',
    '.sft-orbit strong{color:var(--text,#e8e9ec);font-weight:600}',
    '.sft-note{display:flex;gap:10px;padding:12px 14px;border-radius:12px;border:1px solid var(--np-amber-border,rgba(255,181,71,.5));background:var(--np-amber-dim,rgba(255,181,71,.1));font-size:var(--text-sm,.875rem);line-height:1.5}',
    '.sft-note .sft-ic{color:var(--np-amber,#ffb547);margin-top:1px}',

    '.sft-progress{padding:20px;border:1px solid var(--border,rgba(255,255,255,.07));border-radius:var(--r-xl,16px);background:var(--surface,#13151a)}',
    '.sft-progress p{margin-bottom:12px;font-size:var(--text-sm,.875rem);color:var(--text-muted,#8a8f9e)}',
    '.sft-progress p strong{color:var(--text,#e8e9ec);font-variant-numeric:tabular-nums}',
    '.sft-bar{height:6px;border-radius:999px;background:var(--surface-3,#1e2029);overflow:hidden}',
    '.sft-bar i{display:block;height:100%;width:0;border-radius:inherit;background:linear-gradient(90deg,#7b1fa2,#c084fc);transition:width .2s}',

    '.sft-list{display:grid;gap:16px}',
    '.sft-card{display:grid;gap:16px;padding:18px;border:1px solid var(--border,rgba(255,255,255,.07));border-radius:var(--r-xl,16px);background:var(--surface,#13151a)}',
    '.sft-card-top{display:flex;flex-wrap:wrap;align-items:center;gap:8px 12px}',
    '.sft-kind{display:inline-flex;align-items:center;gap:8px;font-size:var(--text-lg,1.15rem);font-weight:700}',
    '.sft-kind .sft-ic{width:22px;height:22px}',
    '.sft-card.is-sun .sft-kind .sft-ic{color:var(--gold,#f5c842)}',
    '.sft-card.is-moon .sft-kind .sft-ic{color:#c9ced8}',
    '.sft-rel{font-size:var(--text-sm,.875rem);color:var(--text-muted,#8a8f9e)}',
    '.sft-tag{display:inline-flex;align-items:center;gap:6px;height:26px;padding:0 10px;border-radius:999px;border:1px solid var(--np-amber-border,rgba(255,181,71,.5));color:var(--np-amber,#ffb547);font-size:var(--text-xs,.8125rem);font-weight:600}',
    '.sft-tag .sft-ic{width:14px;height:14px}',

    '.sft-sunwarn{display:flex;align-items:flex-start;gap:12px;padding:14px 16px;border-radius:12px;border:2px solid #f44336;background:rgba(244,67,54,.14);color:var(--text,#e8e9ec);font-size:clamp(.95rem,.9rem + .2vw,1.05rem);font-weight:700;line-height:1.45}',
    '.sft-sunwarn .sft-ic{width:26px;height:26px;color:#f44336;stroke-width:2.2}',
    'body.light-mode .sft-sunwarn{background:rgba(244,67,54,.08);color:#5c0b05}',

    '.sft-when{display:grid;grid-template-columns:132px 1fr;gap:18px;align-items:center}',
    '.sft-sky{display:grid;justify-items:center;gap:4px;margin:0}',
    '.sft-sky svg{width:180px;height:180px;display:block;overflow:visible;color:var(--text,#e8e9ec)}',
    '.sft-sky figcaption{font-size:.75rem;color:var(--text-muted,#8a8f9e);text-align:center;line-height:1.3}',
    '.sft-time-local{font-size:clamp(1.6rem,1.2rem + 1.6vw,2.2rem);font-weight:700;letter-spacing:-.01em;line-height:1.05;font-variant-numeric:tabular-nums}',
    '.sft-time-date{margin-top:6px;font-size:var(--text-base,1rem);font-weight:600}',
    '.sft-time-utc{margin-top:4px;font-size:var(--text-sm,.875rem);color:var(--text-muted,#8a8f9e);font-variant-numeric:tabular-nums}',
    '.sft-dur{display:inline-flex;align-items:center;gap:6px;margin-top:10px;font-size:var(--text-sm,.875rem);font-weight:600}',
    '.sft-dur .sft-ic{width:16px;height:16px;color:var(--np-icon,#c084fc)}',
    '@media (max-width:440px){.sft-when{grid-template-columns:1fr;justify-items:start}.sft-sky{justify-self:center}}',

    '.sft-facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px 20px;margin:0}',
    '.sft-fact dt{font-size:.8125rem;font-weight:600;color:var(--text-muted,#8a8f9e);margin-bottom:3px}',
    '.sft-fact dd{margin:0;font-size:var(--text-sm,.9rem);line-height:1.45;font-variant-numeric:tabular-nums}',
    '.sft-fact dd strong{font-weight:700}',
    '.sft-inside{color:var(--green,#00d26a);font-weight:700}',
    '.sft-moonwarn{display:flex;gap:10px;padding:10px 12px;border-radius:10px;background:var(--surface-2,#181b22);border:1px solid var(--border-hover,rgba(255,255,255,.13));font-size:var(--text-sm,.875rem);line-height:1.45}',
    '.sft-moonwarn .sft-ic{color:var(--np-amber,#ffb547)}',

    '.sft-mapwrap{display:grid;gap:8px}',
    '.sft-map{height:230px;border-radius:12px;overflow:hidden;border:1px solid var(--border,rgba(255,255,255,.07));background:#1b1c1f}',
    '.sft-legend{display:flex;flex-wrap:wrap;gap:6px 16px;font-size:.75rem;color:var(--text-muted,#8a8f9e)}',
    '.sft-legend span{display:inline-flex;align-items:center;gap:6px}',
    '.sft-legend i{display:inline-block;width:12px;height:12px;border-radius:50%}',
    '.sft-legend i.l-you{background:#2196f3;box-shadow:0 0 0 2px #fff inset}',
    '.sft-legend i.l-near{background:#ffb547}',
    '.sft-legend i.l-line{width:16px;height:3px;border-radius:2px;background:#e0aaff}',
    '.sft-legend i.l-path{width:16px;height:10px;border-radius:2px;background:rgba(192,132,252,.35);border:1px dashed rgba(192,132,252,.8)}',
    '.sft-actions{display:flex;flex-wrap:wrap;gap:10px}',
    '.sft-btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:44px;padding:0 18px;border-radius:10px;background:linear-gradient(135deg,#a333bc,#7b1fa2);color:#fff;font-size:var(--text-sm,.9rem);font-weight:700;text-decoration:none;box-shadow:0 6px 18px rgba(156,39,176,.3)}',
    '.sft-btn:hover{background:linear-gradient(135deg,#b23fcc,#8722b4)}',
    '.sft-foot{font-size:.8125rem;line-height:1.5;color:var(--text-muted,#8a8f9e)}',

    '.sft-empty{padding:24px;border:1px solid var(--border,rgba(255,255,255,.07));border-radius:var(--r-xl,16px);background:var(--surface,#13151a);text-align:center}',
    '.sft-empty p{max-width:46ch;margin:0 auto;line-height:1.55}',
    '.sft-empty p + p{margin-top:8px;color:var(--text-muted,#8a8f9e);font-size:var(--text-sm,.875rem)}',
    '.sft-empty button{margin-top:14px}',

    '.sft-chips{display:flex;flex-wrap:wrap;gap:6px}',
    '.sft-chip{position:relative;display:block}',
    '.sft-chip input{position:absolute;opacity:0;inset:0;margin:0;cursor:pointer}',
    '.sft-chip span{display:inline-flex;align-items:center;gap:7px;height:38px;padding:0 14px;border-radius:999px;border:1px solid var(--border-hover,rgba(255,255,255,.13));font-size:var(--text-sm,.875rem);font-weight:600;color:var(--text-muted,#8a8f9e);cursor:pointer;transition:background-color .16s,color .16s}',
    '.sft-chip span::before{content:"";width:8px;height:8px;border-radius:50%;border:1.5px solid currentColor}',
    '.sft-chip input:checked + span{background:var(--purple-dim,rgba(156,39,176,.12));border-color:var(--purple-border,rgba(156,39,176,.45));color:var(--text,#e8e9ec)}',
    '.sft-chip input:checked + span::before{background:#c084fc;border-color:#c084fc}',
    '.sft-chip input:focus-visible + span{outline:2px solid var(--np-focus,#4da6ff);outline-offset:2px}',
    '.sft-err{padding:16px;border-radius:12px;border:1px solid rgba(244,67,54,.5);background:rgba(244,67,54,.08);line-height:1.5}',
    '.sft-err button{margin-top:10px}',
    '.sft-sec{display:grid;gap:14px;padding-top:16px;border-top:1px solid var(--border,rgba(255,255,255,.07))}',
    '.sft-h3{font-size:var(--text-base,1rem);font-weight:700;color:var(--text,#e8e9ec)}',
    '.sft-timebox{display:grid;gap:2px}',
    '.sft-dec{font-size:.6em;font-weight:600;color:var(--text-muted,#8a8f9e)}',
    '.sft-cross{display:grid;grid-template-columns:200px 1fr;gap:18px;align-items:center}',
    '@media (max-width:560px){.sft-cross{grid-template-columns:1fr}.sft-cross .sft-sky{justify-self:center}}',
    '.sft-chords{display:grid;gap:8px;margin:0;padding:0;list-style:none;font-size:var(--text-sm,.875rem);line-height:1.45;font-variant-numeric:tabular-nums}',
    '.sft-chords b{font-weight:700}',
    '.sft-chords .sft-you b{color:#1e88e5}',
    '.sft-small{margin-top:10px;font-size:.8125rem;line-height:1.5;color:var(--text-muted,#8a8f9e)}',
    '.sft-details{border:1px solid var(--border-hover,rgba(255,255,255,.13));border-radius:12px;background:var(--surface-2,#181b22)}',
    '.sft-details summary{display:flex;align-items:center;gap:8px;min-height:48px;padding:0 16px;font-weight:700;cursor:pointer;list-style:none}',
    '.sft-details summary::-webkit-details-marker{display:none}',
    '.sft-details summary::after{content:"";margin-left:auto;width:8px;height:8px;border-right:2px solid currentColor;border-bottom:2px solid currentColor;transform:rotate(45deg);transition:transform .16s}',
    '.sft-details[open] summary::after{transform:rotate(-135deg)}',
    '.sft-details summary .sft-ic{color:var(--np-icon,#c084fc)}',
    '.sft-details > .sft-small{margin:0;padding:0 16px 12px}',
    '.sft-tblwrap{overflow-x:auto;padding:0 8px 12px}',
    '.sft-tbl{width:100%;border-collapse:collapse;font-size:.8125rem;font-variant-numeric:tabular-nums}',
    '.sft-tbl th,.sft-tbl td{padding:7px 8px;text-align:left;vertical-align:top;border-top:1px solid var(--border,rgba(255,255,255,.07))}',
    '.sft-tbl thead th{border-top:0;color:var(--text-muted,#8a8f9e);font-weight:600}',
    '.sft-tbl tbody th{font-weight:600;color:var(--text-muted,#8a8f9e);min-width:120px}',
    '.sft-tbl td{color:var(--text,#e8e9ec);white-space:nowrap}',
    '.sft-tbl td[colspan]{white-space:normal}',
    '@media (prefers-reduced-motion:reduce){.sft-bar i,.sft-tab,.sft-chip span{transition:none}}'
  ].join('\n');

  function injectCss(extraHide) {
    if (document.getElementById('sft-styles')) return;
    var s = document.createElement('style');
    s.id = 'sft-styles';
    var hide = '';
    if (extraHide) {
      hide = extraHide.split(',').map(function (x) { return x.trim(); }).filter(Boolean)
        .map(function (x) { return 'html.sft-show-transits ' + x; }).join(',') + '{display:none !important}\n';
    }
    s.textContent = hide + CSS;
    (document.head || document.documentElement).appendChild(s);
  }

  // Early setup, while the page is still loading: avoids a flash of the wrong tab
  var currentScript = document.currentScript;
  var hideSelector = currentScript ? (currentScript.getAttribute('data-hide') || '') : '';
  injectCss(hideSelector);
  function tabFromUrl() {
    try { return new URL(location.href).searchParams.get('tab') === 'transits' ? 'transits' : 'passes'; }
    catch (e) { return 'passes'; }
  }
  if (tabFromUrl() === 'transits') document.documentElement.classList.add('sft-show-transits');

  // ---------- Helpers ----------
  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function lsGet(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
  function lsSet(k, v) { try { localStorage.setItem(k, v); return true; } catch (e) { return false; } }
  function track(name, params) { try { if (typeof gtag === 'function') gtag('event', name, params || {}); } catch (e) {} }

  function haversineKm(lat1, lon1, lat2, lon2) {
    var p1 = lat1 * DEG, p2 = lat2 * DEG, dp = (lat2 - lat1) * DEG, dl = (lon2 - lon1) * DEG;
    var a = Math.sin(dp / 2) * Math.sin(dp / 2) + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
    return 2 * EARTH_R * Math.asin(Math.min(1, Math.sqrt(a)));
  }
  function bearing(lat1, lon1, lat2, lon2) {
    var p1 = lat1 * DEG, p2 = lat2 * DEG, dl = (lon2 - lon1) * DEG;
    var b = Math.atan2(Math.sin(dl) * Math.cos(p2), Math.cos(p1) * Math.sin(p2) - Math.sin(p1) * Math.cos(p2) * Math.cos(dl)) / DEG;
    return (b + 360) % 360;
  }
  function destination(lat, lon, brg, km) {
    var d = km / EARTH_R, p1 = lat * DEG, l1 = lon * DEG, b = brg * DEG;
    var p2 = Math.asin(Math.sin(p1) * Math.cos(d) + Math.cos(p1) * Math.sin(d) * Math.cos(b));
    var l2 = l1 + Math.atan2(Math.sin(b) * Math.sin(d) * Math.cos(p1), Math.cos(d) - Math.sin(p1) * Math.sin(p2));
    return [p2 / DEG, ((l2 / DEG + 540) % 360) - 180];
  }
  var COMPASS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  function compass(deg) { return COMPASS[Math.round(((deg % 360) + 360) % 360 / 22.5) % 16]; }
  function km(d) { return d < 1 ? Math.round(d * 1000) + ' m' : (d < 10 ? d.toFixed(1) : Math.round(d)) + ' km'; }

  function roundSec(t) { return new Date(Math.round(t / 1000) * 1000); }
  function tzName(d) {
    try {
      var p = new Intl.DateTimeFormat('en-GB', { timeZoneName: 'short' }).formatToParts(d);
      for (var i = 0; i < p.length; i++) if (p[i].type === 'timeZoneName') return p[i].value;
    } catch (e) {}
    return '';
  }
  function fmtTime(d, utc) {
    var o = { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
    if (utc) o.timeZone = 'UTC';
    return d.toLocaleTimeString('en-GB', o);
  }
  function fmtDate(d, utc) {
    var o = { weekday: 'short', day: 'numeric', month: 'short' };
    if (utc) o.timeZone = 'UTC';
    return d.toLocaleDateString('en-GB', o);
  }
  function relTime(ms) {
    if (ms < 60000) return 'in less than a minute';
    var m = Math.round(ms / 60000);
    if (m < 60) return 'in ' + m + ' min';
    var h = Math.floor(m / 60), mm = m % 60;
    if (h < 48) return 'in ' + h + ' h' + (mm ? ' ' + mm + ' min' : '');
    var dd = Math.floor(h / 24), hh = h % 24;
    return 'in ' + dd + ' days' + (hh ? ' ' + hh + ' h' : '');
  }
  function ageText(ms) {
    var h = ms / 3600000;
    if (h < 1) return 'less than an hour old';
    if (h < 48) return Math.round(h) + ' h old';
    return Math.round(h / 24) + ' days old';
  }

  // Closest point to the user on the stored centre line (fast, no astronomy needed)
  function localize(r, loc) {
    var L = r.line || [];
    if (L.length < 2) {
      var dd = haversineKm(loc.lat, loc.lng, r.lat, r.lon);
      return { lat: r.lat, lon: r.lon, t: r.t, distKm: dd, bearing: bearing(loc.lat, loc.lng, r.lat, r.lon), side: 1, s: dd };
    }
    var k = Math.cos(loc.lat * DEG) * DEG * EARTH_R, ky = DEG * EARTH_R;
    function xy(p) {
      var dl = p[1] - loc.lng;
      if (dl > 180) dl -= 360; if (dl < -180) dl += 360;
      return [dl * k, (p[0] - loc.lat) * ky];
    }
    var best = null;
    var a = xy(L[0]);
    for (var i = 0; i < L.length - 1; i++) {
      var b = xy(L[i + 1]);
      var abx = b[0] - a[0], aby = b[1] - a[1];
      var len2 = abx * abx + aby * aby || 1e-12;
      var u = Math.max(0, Math.min(1, -(a[0] * abx + a[1] * aby) / len2));
      var cx = a[0] + u * abx, cy = a[1] + u * aby;
      var d2 = cx * cx + cy * cy;
      if (!best || d2 < best.d2) best = { d2: d2, i: i, u: u, cx: cx, cy: cy, abx: abx, aby: aby };
      a = b;
    }
    var p = L[best.i], q = L[best.i + 1];
    var lat = p[0] + (q[0] - p[0]) * best.u;
    var lon = p[1] + (q[1] - p[1]) * best.u;
    var t = r.t + p[2] + (q[2] - p[2]) * best.u;
    var dist = haversineKm(loc.lat, loc.lng, lat, lon);
    // Which side of the path is the user on? (right of the direction of travel = +)
    var cr = best.abx * (-best.cy) - best.aby * (-best.cx);
    var side = cr > 0 ? -1 : 1;
    return { lat: lat, lon: lon, t: t, distKm: dist, bearing: bearing(loc.lat, loc.lng, lat, lon), side: side, s: side * dist };
  }

  // ---------- Sky drawing: the satellite crossing the disc ----------
  // Simple outlines, centred on 0,0 (not to scale; the real ones are tinier)
  var SILHOUETTE = {
    iss: '<rect x="-6" y="-0.45" width="12" height="0.9"/>' +
         '<rect x="-5.6" y="-2.6" width="1.6" height="5.2"/><rect x="-3.5" y="-2.6" width="1.6" height="5.2"/>' +
         '<rect x="1.9" y="-2.6" width="1.6" height="5.2"/><rect x="4" y="-2.6" width="1.6" height="5.2"/>' +
         '<rect x="-0.8" y="-1.6" width="1.6" height="3.2"/>',
    css: '<rect x="-3.6" y="-0.6" width="7.2" height="1.2"/><rect x="-0.7" y="-0.6" width="1.4" height="4.2"/>' +
         '<rect x="-5.8" y="-2.4" width="2.2" height="4.8"/><rect x="3.6" y="-2.4" width="2.2" height="4.8"/>',
    hst: '<rect x="-3" y="-1" width="6" height="2" rx="0.6"/>' +
         '<rect x="-1.8" y="-3.4" width="3.2" height="1.4"/><rect x="-1.8" y="2" width="3.2" height="1.4"/>'
  };
  // Vectors are (right, up) as seen facing the Sun/Moon with the zenith up.
  function rot2(v, a) { var c = Math.cos(a), s = Math.sin(a); return [v[0] * c - v[1] * s, v[0] * s + v[1] * c]; }
  function paOf(r, v) {
    // Position angle (from north through east) of a sky vector
    var a = r.sky.na, b = r.sky.nb;
    return (Math.atan2(-b * v[0] + a * v[1], a * v[0] + b * v[1]) / DEG + 360) % 360;
  }
  // Offsets across the path, as a fraction of the half-width (+ = right of the track on the ground)
  function chordOf(r, frac) {
    var m = [r.sky.mx, r.sky.my], n = [-r.sky.my, r.sky.mx];
    var off = frac * r.sky.rightSign;
    var c = [off * n[0], off * n[1]];
    var half = Math.sqrt(Math.max(0, 1 - off * off));
    return {
      c: c, half: half,
      entry: [c[0] - m[0] * half, c[1] - m[1] * half],
      exit: [c[0] + m[0] * half, c[1] + m[1] * half]
    };
  }

  function skySvg(r, userFrac, idx, orient) {
    var C = 90, R = 60;
    var a = 0;
    if (orient === 'north') a = Math.PI / 2 - Math.atan2(r.sky.nb, r.sky.na);
    var m = rot2([r.sky.mx, r.sky.my], a);
    var isSun = r.body === 'Sun';
    var ink = isSun ? '#5a2a00' : '#2b2f38';
    function P(v) { return [(C + v[0] * R).toFixed(1), (C - v[1] * R).toFixed(1)]; }
    function line(chord, ext, attrs) {
      var c = rot2(chord.c, a), h = chord.half + ext;
      var p1 = P([c[0] - m[0] * h, c[1] - m[1] * h]), p2 = P([c[0] + m[0] * h, c[1] + m[1] * h]);
      return '<line x1="' + p1[0] + '" y1="' + p1[1] + '" x2="' + p2[0] + '" y2="' + p2[1] + '" ' + attrs + '/>';
    }
    function label(chord, txt, color) {
      var c = rot2(chord.c, a), h = chord.half + 0.24;
      var p = P([c[0] - m[0] * h, c[1] - m[1] * h]);
      return '<text x="' + p[0] + '" y="' + p[1] + '" dy="3.5" text-anchor="middle" font-size="10" font-weight="700" fill="' + color + '">' + txt + '</text>';
    }
    var g = 'sftg' + idx;
    var out = '<svg viewBox="0 0 180 180" role="img" aria-label="How ' + esc(satInfo(r.sat).label) + ' crosses the ' + (isSun ? 'Sun' : 'Moon') + '">';
    out += '<defs><radialGradient id="' + g + '" cx="45%" cy="40%" r="65%">' +
      (isSun ? '<stop offset="0" stop-color="#fff1b8"/><stop offset=".55" stop-color="#ffc93c"/><stop offset="1" stop-color="#f08c00"/>'
             : '<stop offset="0" stop-color="#f1f2f5"/><stop offset=".7" stop-color="#c3c7d0"/><stop offset="1" stop-color="#8d939f"/>') +
      '</radialGradient></defs>';
    if (isSun) out += '<circle cx="90" cy="90" r="68" fill="#ffc93c" opacity=".12"/>';
    out += '<circle cx="90" cy="90" r="' + R + '" fill="url(#' + g + ')"/>';

    // Side chords: 60% of the way to each edge of the path
    var A = chordOf(r, 0.6), B = chordOf(r, -0.6), Z = chordOf(r, 0);
    out += line(A, 0.06, 'stroke="' + ink + '" stroke-width="1" stroke-dasharray="1.5 3" opacity=".7"');
    out += line(B, 0.06, 'stroke="' + ink + '" stroke-width="1" stroke-dasharray="1.5 3" opacity=".7"');
    out += label(A, 'A', 'var(--text-muted,#8a8f9e)') + label(B, 'B', 'var(--text-muted,#8a8f9e)');

    // Centre chord with the arrow and the silhouettes
    out += line(Z, 0.2, 'stroke="' + ink + '" stroke-width="1.3" stroke-dasharray="3 3" opacity=".85"');
    var tip = P([m[0] * (Z.half + 0.2), m[1] * (Z.half + 0.2)]);
    var bx = m[0] * (Z.half + 0.2) * R, by = m[1] * (Z.half + 0.2) * R, nx = -m[1], ny = m[0], ah = 8;
    out += '<polygon points="' + tip[0] + ',' + tip[1] + ' ' +
      (C + bx - m[0] * ah + nx * ah * 0.5).toFixed(1) + ',' + (C - (by - m[1] * ah + ny * ah * 0.5)).toFixed(1) + ' ' +
      (C + bx - m[0] * ah - nx * ah * 0.5).toFixed(1) + ',' + (C - (by - m[1] * ah - ny * ah * 0.5)).toFixed(1) + '" fill="' + ink + '"/>';
    var ang = Math.atan2(-m[1], m[0]) / DEG;
    [-0.55, 0, 0.55].forEach(function (k) {
      var px = C + m[0] * k * R, py = C - m[1] * k * R;
      out += '<g transform="translate(' + px.toFixed(1) + ' ' + py.toFixed(1) + ') rotate(' + ang.toFixed(1) + ') scale(1.4)" fill="#14161c">' +
        (SILHOUETTE[r.sat] || SILHOUETTE.iss) + '</g>';
    });

    // Your own chord, when you are inside the path
    if (userFrac != null && Math.abs(userFrac) < 1) {
      var U = chordOf(r, userFrac);
      out += line(U, 0.06, 'stroke="#1e88e5" stroke-width="2.2" stroke-linecap="round"');
      var uc = rot2(U.c, a), up = P([uc[0] + m[0] * (U.half + 0.24), uc[1] + m[1] * (U.half + 0.24)]);
      out += '<text x="' + up[0] + '" y="' + up[1] + '" dy="3.5" text-anchor="middle" font-size="9.5" font-weight="700" fill="#1e88e5">You</text>';
    }

    // Orientation marks on the rim
    function rimMark(v, txt) {
      var vv = rot2(v, a), l = Math.sqrt(vv[0] * vv[0] + vv[1] * vv[1]) || 1;
      vv = [vv[0] / l, vv[1] / l];
      var p1 = P([vv[0] * 1.02, vv[1] * 1.02]), p2 = P([vv[0] * 1.14, vv[1] * 1.14]), pt = P([vv[0] * 1.3, vv[1] * 1.3]);
      return '<line x1="' + p1[0] + '" y1="' + p1[1] + '" x2="' + p2[0] + '" y2="' + p2[1] + '" stroke="currentColor" stroke-width="1.5"/>' +
        '<text x="' + pt[0] + '" y="' + pt[1] + '" dy="3.5" text-anchor="middle" font-size="10" font-weight="700" fill="currentColor">' + txt + '</text>';
    }
    var north = [r.sky.na, r.sky.nb], east = [-r.sky.nb, r.sky.na];
    out += '<g class="sft-rim">';
    if (orient === 'north') { out += rimMark(north, 'N') + rimMark(east, 'E') + rimMark([0, 1], 'Z'); }
    else { out += rimMark([0, 1], 'Up') + rimMark(north, 'N'); }
    out += '</g></svg>';
    return out;
  }

  // ---------- Module state ----------
  var opts = null;
  var panel = null;
  var state = {
    tab: 'passes',
    radius: DEFAULT_RADIUS,
    sats: { iss: true, css: true, hst: true },
    orient: 'zenith',     // disc drawing: 'zenith' (as you see it) or 'north' (telescope)
    data: null,           // { computedAt, epochMs, lat, lng, day, results }
    status: 'idle',       // idle | waiting-location | loading-orbit | computing | ready | error
    progress: 0,
    error: '',
    jobId: 0,
    worker: null,
    maps: [],
    pollTimer: null,
    lastLoc: null,       // last location seen (to notice changes)
    calcLoc: null,       // location the current results were calculated for
    rendered: false
  };

  function getLoc() {
    try {
      var l = opts.getLocation ? opts.getLocation() : null;
      if (l && isFinite(l.lat) && isFinite(l.lng)) return { lat: +l.lat, lng: +l.lng };
    } catch (e) {}
    return null;
  }
  function isDefaultLoc(l) { return Math.abs(l.lat - DEFAULT_LOC.lat) < 0.00005 && Math.abs(l.lng - DEFAULT_LOC.lng) < 0.00005; }
  function localDay() { var d = new Date(); return d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate(); }

  // ---------- Cache ----------
  function readCache(loc) {
    var raw = lsGet(CACHE_KEY);
    if (!raw) return null;
    try {
      var c = JSON.parse(raw);
      if (!c || !c.results || !c.sats) return null;
      if (c.day !== localDay()) return null;
      if (Date.now() - c.computedAt > CACHE_MAX_AGE) return null;
      if (haversineKm(c.lat, c.lng, loc.lat, loc.lng) > RECOMPUTE_KM) return null;
      return c;
    } catch (e) { return null; }
  }
  function writeCache(d) {
    if (!lsSet(CACHE_KEY, JSON.stringify(d))) {
      // Not enough space: keep it in memory only
      try { localStorage.removeItem(CACHE_KEY); } catch (e) {}
    }
  }

  // ---------- Orbit data for the ISS ----------
  function epochFromSatrec(s) {
    if (!s || s.epochyr == null || s.epochdays == null) return null;
    var y = s.epochyr < 57 ? 2000 + s.epochyr : 1900 + s.epochyr;
    return Date.UTC(y, 0, 1) + (s.epochdays - 1) * 86400000;
  }
  function epochFromOmm(o) {
    if (!o || !o.EPOCH) return null;
    var e = String(o.EPOCH);
    var t = Date.parse(/[zZ]|[+-]\d\d:?\d\d$/.test(e) ? e : e + 'Z');
    return isFinite(t) ? t : null;
  }
  function matchSat(name, norad) {
    for (var i = 0; i < SATS.length; i++) {
      if (String(norad) === SATS[i].norad || SATS[i].re.test(String(name || '').trim())) return SATS[i].id;
    }
    return null;
  }

  // Returns { iss: {satrec|omm, epochMs}, css: {...}, hst: {...} } (missing ones left out)
  function getElements() {
    return new Promise(function (resolve) {
      var tries = 0;
      function fromPage() {
        var list = null;
        try { list = opts.getSatellites ? opts.getSatellites() : null; } catch (e) { list = null; }
        if (list && list.length) {
          var found = {};
          for (var i = 0; i < list.length; i++) {
            var s = list[i];
            var id = s && s.satrec ? matchSat(s.name, s.noradId) : null;
            if (id && !found[id]) found[id] = { satrec: s.satrec, epochMs: epochFromSatrec(s.satrec) };
          }
          if (Object.keys(found).length === SATS.length) { resolve(found); return; }
          // Some are not in the page list: fetch the raw orbit data for those
          fetchOmms().then(function (o) {
            o = o || {};
            SATS.forEach(function (c) { if (!found[c.id] && o[c.id]) found[c.id] = o[c.id]; });
            resolve(found);
          });
          return;
        }
        // The page is still downloading its satellite list: wait for it (up to 45 s)
        if (opts.getSatellites && ++tries < 90) { setTimeout(fromPage, 500); return; }
        fetchOmms().then(function (o) { resolve(o || {}); });
      }
      fromPage();
    });
  }

  function ommsToResult(omms) {
    var out = {};
    Object.keys(omms || {}).forEach(function (id) { out[id] = { omm: omms[id], epochMs: epochFromOmm(omms[id]) }; });
    return out;
  }

  function fetchOmms() {
    var cached = null;
    try { cached = JSON.parse(lsGet(OMM_KEY) || 'null'); } catch (e) { cached = null; }
    if (cached && cached.omms && Date.now() - cached.savedAt < 6 * 3600 * 1000) {
      return Promise.resolve(ommsToResult(cached.omms));
    }
    return fetch(TLE_URL).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    }).then(function (arr) {
      var omms = {};
      for (var i = 0; i < arr.length; i++) {
        var id = matchSat(arr[i].OBJECT_NAME, arr[i].NORAD_CAT_ID);
        if (id && !omms[id]) omms[id] = arr[i];
      }
      lsSet(OMM_KEY, JSON.stringify({ savedAt: Date.now(), omms: omms }));
      return ommsToResult(omms);
    }).catch(function () {
      return cached && cached.omms ? ommsToResult(cached.omms) : {};
    });
  }

  // ---------- Calculation ----------
  function stopWorker() {
    if (state.worker) { try { state.worker.terminate(); } catch (e) {} state.worker = null; }
  }

  function run(force) {
    var loc = getLoc();
    if (!loc) { state.status = 'waiting-location'; render(); return; }
    state.lastLoc = loc;
    state.calcLoc = loc;

    if (!force) {
      if (state.data && state.data.day === localDay() && Date.now() - state.data.computedAt < CACHE_MAX_AGE &&
          haversineKm(state.data.lat, state.data.lng, loc.lat, loc.lng) <= RECOMPUTE_KM) {
        stopWorker(); state.jobId++; state.status = 'ready'; render(); return;
      }
      var c = readCache(loc);
      if (c) { stopWorker(); state.jobId++; state.data = c; state.status = 'ready'; render(); return; }
    }

    stopWorker();
    var job = ++state.jobId;
    state.status = 'loading-orbit';
    state.progress = 0;
    render();

    getElements().then(function (els) {
      if (job !== state.jobId) return;
      if (!els || !Object.keys(els).length) { fail('Could not load the orbit data. Check your connection and try again.'); return; }
      startWorker(job, loc, els);
    });
  }

  function startWorker(job, loc, els) {
    var w;
    try { w = new Worker((opts && opts.workerUrl) || WORKER_URL); }
    catch (e) { fail('This browser cannot run the transit calculator.'); return; }
    state.worker = w;
    state.status = 'computing';
    state.progress = 0.01;
    render();

    function satList(useOmm) {
      return SATS.filter(function (c) { return els[c.id]; }).map(function (c) {
        var e = els[c.id], o = { id: c.id, sizeKm: c.sizeKm };
        if (e.satrec && !useOmm) o.satrec = e.satrec; else o.omm = e.omm;
        return o;
      });
    }
    function epochs() {
      return SATS.map(function (c) { return { id: c.id, epochMs: els[c.id] ? els[c.id].epochMs : null }; });
    }

    w.onmessage = function (ev) {
      var m = ev.data || {};
      if (m.id !== job || job !== state.jobId) return;
      if (m.type === 'progress') { state.progress = m.value; updateProgress(); }
      else if (m.type === 'done') {
        stopWorker();
        state.data = {
          computedAt: Date.now(), day: localDay(), sats: epochs(),
          lat: loc.lat, lng: loc.lng, results: m.results
        };
        writeCache(state.data);
        state.status = 'ready';
        render();
        track('transits_calculated', { count: m.results.length });
      } else if (m.type === 'error') {
        stopWorker();
        fail('The calculation failed. Please try again.');
      }
    };
    w.onerror = function () {
      if (job !== state.jobId) return;
      stopWorker();
      fail('The calculator could not start. Check your connection and try again.');
    };

    var params = { lat: loc.lat, lon: loc.lng, start: Date.now(), days: DAYS, sats: satList(false) };
    try {
      w.postMessage({ type: 'compute', id: job, params: params });
    } catch (e) {
      // Could not hand the page's orbit objects to the worker: use the raw orbit data instead
      fetchOmms().then(function (o) {
        if (job !== state.jobId) return;
        els = o || {};
        params.sats = satList(true).filter(function (x) { return x.omm; });
        if (!params.sats.length) { fail('Could not load the orbit data. Check your connection and try again.'); return; }
        w.postMessage({ type: 'compute', id: job, params: params });
      });
    }
  }

  function fail(msg) {
    state.status = 'error';
    state.error = msg;
    render();
  }

  // ---------- Rendering ----------
  function destroyMaps() {
    state.maps.forEach(function (m) { try { m.remove(); } catch (e) {} });
    state.maps = [];
  }

  function locHtml() {
    var loc = getLoc();
    if (!loc) {
      return '<div class="sft-loc is-pending">' + svg('pin') + '<span>Detecting your location...</span></div>';
    }
    var canChange = !!opts.changeLocation;
    if (isDefaultLoc(loc)) {
      return '<div class="sft-loc is-default">' + svg('pin') + '<span>Showing Madrid (default)</span>' +
        (canChange ? '<button type="button" class="sft-loc-btn" data-sft-act="loc">Set your location</button>' : '') + '</div>';
    }
    return '<div class="sft-loc">' + svg('pin') + '<span><span class="sr-only">Searching around </span><strong>' +
      loc.lat.toFixed(4) + '°, ' + loc.lng.toFixed(4) + '°</strong></span>' +
      (canChange ? '<button type="button" class="sft-loc-btn" data-sft-act="loc" aria-label="Change location">Change</button>' : '') + '</div>';
  }

  function shell() {
    var radios = RADII.map(function (r) {
      return '<label><input type="radio" name="sftRadius" value="' + r + '"' + (r === state.radius ? ' checked' : '') + '><span>' + r + ' km</span></label>';
    }).join('');
    var chips = SATS.map(function (c) {
      return '<label class="sft-chip" title="' + esc(c.full) + '"><input type="checkbox" name="sftSat" value="' + c.id + '"' +
        (state.sats[c.id] ? ' checked' : '') + '><span>' + esc(c.label) + '</span></label>';
    }).join('');
    panel.innerHTML =
      '<header class="sft-head">' +
        '<h2 class="sft-title">Satellites crossing the Sun and Moon</h2>' +
        '<p class="sft-sub">For a split second, the ISS, Tiangong or Hubble slips in front of the Sun or the Moon. You only see it from a narrow strip a few kilometres wide, so here is where to stand and when to look.</p>' +
        '<div data-sft="loc">' + locHtml() + '</div>' +
      '</header>' +
      '<div class="sft-controls">' +
        '<fieldset class="sft-radius"><legend>Satellites</legend><div class="sft-chips">' + chips + '</div></fieldset>' +
        '<fieldset class="sft-radius"><legend>Search radius</legend><div class="sft-seg">' + radios + '</div></fieldset>' +
        '<fieldset class="sft-radius"><legend>Disc drawings</legend><div class="sft-seg">' +
          '<label><input type="radio" name="sftOrient" value="zenith"' + (state.orient === 'zenith' ? ' checked' : '') + '><span>Up = zenith</span></label>' +
          '<label><input type="radio" name="sftOrient" value="north"' + (state.orient === 'north' ? ' checked' : '') + '><span>North up</span></label>' +
        '</div></fieldset>' +
        '<button type="button" class="sft-recalc" data-sft-act="recalc">' + svg('refresh') + 'Recalculate</button>' +
      '</div>' +
      '<div class="sft-meta">' +
        '<p class="sft-orbit" data-sft="orbit"></p>' +
        '<div class="sft-note" role="note">' + svg('warn') + '<span>Anything more than 2 or 3 days away is a rough guide: space stations adjust their orbits often, so times can move by seconds and paths by kilometres. Check again the day before you head out.</span></div>' +
      '</div>' +
      '<div data-sft="body" aria-live="polite"></div>';

    panel.addEventListener('change', function (e) {
      if (e.target && e.target.name === 'sftRadius') {
        state.radius = parseInt(e.target.value, 10) || DEFAULT_RADIUS;
        lsSet(RADIUS_KEY, String(state.radius));
        renderBody();
      } else if (e.target && e.target.name === 'sftOrient') {
        state.orient = e.target.value === 'north' ? 'north' : 'zenith';
        lsSet(ORIENT_KEY, state.orient);
        renderBody();
      } else if (e.target && e.target.name === 'sftSat') {
        state.sats[e.target.value] = e.target.checked;
        lsSet(SATS_KEY, JSON.stringify(state.sats));
        renderBody();
      }
    });
    panel.addEventListener('click', function (e) {
      var b = e.target.closest ? e.target.closest('[data-sft-act]') : null;
      if (!b) return;
      var act = b.getAttribute('data-sft-act');
      if (act === 'recalc' || act === 'retry') run(true);
      else if (act === 'loc' && opts.changeLocation) opts.changeLocation();
      else if (act === 'radius200') {
        state.radius = 200; lsSet(RADIUS_KEY, '200');
        var inp = panel.querySelector('input[name="sftRadius"][value="200"]'); if (inp) inp.checked = true;
        renderBody();
      }
    });
    state.rendered = true;
  }

  function render() {
    if (!panel || state.tab !== 'transits') return;
    if (!state.rendered) shell();
    var l = panel.querySelector('[data-sft="loc"]');
    if (l) l.innerHTML = locHtml();
    var rc = panel.querySelector('[data-sft-act="recalc"]');
    if (rc) rc.disabled = (state.status === 'computing' || state.status === 'loading-orbit');
    var orb = panel.querySelector('[data-sft="orbit"]');
    if (orb) {
      if (state.data && state.data.sats) {
        var parts = state.data.sats.map(function (x) {
          var c = satInfo(x.id);
          return '<strong>' + esc(c.label) + '</strong> ' + (x.epochMs ? ageText(Date.now() - x.epochMs) : 'not available right now');
        });
        orb.innerHTML = svg('sat') + '<span>Orbit data: ' + parts.join(', ') + '. Searching the next ' + DAYS + ' days.</span>';
      } else {
        orb.innerHTML = svg('sat') + '<span>Searching the next ' + DAYS + ' days with the latest orbit data.</span>';
      }
    }
    renderBody();
  }

  function updateProgress() {
    var bar = panel && panel.querySelector('.sft-bar i');
    var pct = panel && panel.querySelector('[data-sft="pct"]');
    var v = Math.max(0, Math.min(100, Math.round(state.progress * 100)));
    if (bar) bar.style.width = v + '%';
    if (pct) pct.textContent = v + '%';
    var pb = panel && panel.querySelector('.sft-progress');
    if (pb) pb.setAttribute('aria-valuenow', String(v));
  }

  function renderBody() {
    var box = panel && panel.querySelector('[data-sft="body"]');
    if (!box) return;
    destroyMaps();

    if (state.status === 'waiting-location' || state.status === 'idle') {
      box.innerHTML = '<div class="sft-progress"><p>Waiting for your location...</p><div class="sft-bar"><i></i></div></div>';
      return;
    }
    if (state.status === 'loading-orbit' || state.status === 'computing') {
      var label = state.status === 'loading-orbit' ? 'Loading the latest orbit data...' : 'Searching the next ' + DAYS + ' days...';
      box.innerHTML = '<div class="sft-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" aria-label="Calculating transits">' +
        '<p>' + label + ' <strong data-sft="pct">0%</strong></p><div class="sft-bar"><i></i></div></div>';
      updateProgress();
      return;
    }
    if (state.status === 'error') {
      box.innerHTML = '<div class="sft-err"><p>' + esc(state.error) + '</p><button type="button" class="sft-recalc" data-sft-act="retry">' + svg('refresh') + 'Try again</button></div>';
      return;
    }
    if (!state.data) return;

    var loc = getLoc() || { lat: state.data.lat, lng: state.data.lng };
    var now = Date.now();
    if (!SATS.some(function (c) { return state.sats[c.id]; })) {
      box.innerHTML = '<div class="sft-empty"><p>Pick at least one satellite above to see its transits.</p></div>';
      return;
    }
    var all = state.data.results.filter(function (r) { return state.sats[r.sat || 'iss']; }).map(function (r) {
      return { r: r, p: localize(r, loc) };
    }).filter(function (x) { return x.p.t > now - 5000; });
    var inRadius = all.filter(function (x) { return x.p.distKm <= state.radius; });

    if (!inRadius.length) {
      var next = all.filter(function (x) { return x.p.distKm <= 200; })[0];
      var html = '<div class="sft-empty"><p>Nothing crossing the Sun or the Moon within ' + state.radius + ' km of you in the next ' + DAYS + ' days.</p>';
      if (next && state.radius < 200) {
        var nd = roundSec(next.p.t);
        html += '<p>The closest one is ' + esc(satInfo(next.r.sat).label) + ' crossing the ' + next.r.body + ' on ' + esc(fmtDate(nd)) + ', ' + km(next.p.distKm) + ' away.</p>' +
          '<button type="button" class="sft-recalc" data-sft-act="radius200">Search 200 km</button>';
      } else {
        html += '<p>Transits over any one spot are rare. Have another look in a few days, once the orbits have been updated.</p>';
      }
      box.innerHTML = html + '</div>';
      return;
    }

    var shown = inRadius;

    var out = '<div class="sft-list">';
    shown.forEach(function (x, i) { out += cardHtml(x.r, x.p, i, now); });
    out += '</div>';

    out += '<p class="sft-foot" style="margin-top:16px">The big time is for the point of the centre line closest to you. Near the edges of the path the transit is shorter. Map tiles by Esri.</p>';
    box.innerHTML = out;

    // Maps: only after the cards are on screen
    shown.forEach(function (x, i) {
      var el = box.querySelector('[data-sft-map="' + i + '"]');
      if (el) { var m = drawMap(el, x.r, x.p, loc); if (m) state.maps.push(m); }
    });
  }

  // ---------- Number formats for telescopes ----------
  function pad2(n) { return (n < 10 ? '0' : '') + n; }
  function fmtTimeFrac(t, utc, decimals) {
    var step = decimals === 2 ? 10 : 100;
    var tr = Math.round(t / step) * step;
    var whole = new Date(Math.floor(tr / 1000) * 1000);
    var frac = String(Math.round((tr % 1000) / step));
    while (frac.length < decimals) frac = '0' + frac;
    return fmtTime(whole, utc) + '.' + frac;
  }
  function fmtRA(h) {
    var s = Math.round(((h % 24) + 24) % 24 * 3600);
    return Math.floor(s / 3600) + 'h ' + pad2(Math.floor(s / 60) % 60) + 'm ' + pad2(s % 60) + 's';
  }
  function fmtDMS(d) {
    var sign = d < 0 ? '\u2212' : '+', s = Math.round(Math.abs(d) * 3600);
    return sign + Math.floor(s / 3600) + '\u00b0 ' + pad2(Math.floor(s / 60) % 60) + '\u2032 ' + pad2(s % 60) + '\u2033';
  }
  function fmtSep(d) {
    var s = Math.round(Math.abs(d) * 3600);
    if (s < 3600) return Math.floor(s / 60) + '\u2032 ' + pad2(s % 60) + '\u2033';
    return Math.floor(s / 3600) + '\u00b0 ' + pad2(Math.floor(s / 60) % 60) + '\u2032';
  }
  function deg1(x) { return (Math.round(x * 10) / 10).toFixed(1) + '\u00b0'; }
  function signed1(x) { return (x < 0 ? '\u2212' : x > 0 ? '+' : '') + Math.abs(x).toFixed(1); }
  function signed2(x) { return (x < 0 ? '\u2212' : x > 0 ? '+' : '') + Math.abs(x).toFixed(2); }
  function arcsec(x) { return (x < 10 ? x.toFixed(2) : x.toFixed(1)) + '\u2033'; }
  function secs(x) { return (x < 1 ? x.toFixed(2) : x.toFixed(1)) + ' s'; }

  function cardHtml(r, p, i, now) {
    var isSun = r.body === 'Sun';
    var B = isSun ? 'Sun' : 'Moon';
    var sat = satInfo(r.sat);
    var S = esc(sat.label);
    var d = roundSec(p.t);
    var w = (r.wLeft || 0) + (r.wRight || 0);
    var halfSide = p.side > 0 ? r.wRight : r.wLeft;
    var inside = p.distKm <= halfSide;
    var frac = inside && halfSide > 0 ? p.s / halfSide : null;
    var dir = compass(p.bearing) + ' (bearing ' + Math.round(p.bearing) + '\u00b0)';
    var where = inside
      ? '<span class="sft-inside">You are inside the path.</span> The centre line is ' + km(p.distKm) + ' ' + dir + ', where ' + S + ' crosses right through the middle.'
      : 'Go <strong>' + km(p.distKm) + '</strong> ' + dir + ' to reach the centre line.';
    var utcDate = fmtDate(d, true), locDate = fmtDate(d);
    var gmaps = 'https://www.google.com/maps/search/?api=1&query=' + p.lat.toFixed(5) + ',' + p.lon.toFixed(5);
    var Y = r.you, Cn = r.centre;

    var h = '<article class="sft-card ' + (isSun ? 'is-sun' : 'is-moon') + '">';
    h += '<div class="sft-card-top"><span class="sft-kind">' + svg(isSun ? 'sun' : 'moon') + S + ' crosses the ' + B + '</span>' +
      '<span class="sft-rel" data-sft-rel="' + Math.round(p.t) + '">' + relTime(p.t - now) + '</span>' +
      (p.t - now > LESS_RELIABLE_MS ? '<span class="sft-tag">' + svg('warn') + 'Less reliable, check again the day before</span>' : '') +
      '</div>';

    if (isSun) h += '<div class="sft-sunwarn" role="note">' + svg('warn') + '<span>' + SUN_WARNING + '</span></div>';

    // 1) At the centre line: where and when to be
    var tfrac = fmtTimeFrac(p.t, false, 1);
    h += '<section class="sft-sec"><h3 class="sft-h3">At the centre line</h3>' +
      '<div class="sft-timebox">' +
        '<div class="sft-time-local">' + esc(tfrac.slice(0, -2)) + '<span class="sft-dec">' + esc(tfrac.slice(-2)) + '</span></div>' +
        '<div class="sft-time-date">' + esc(locDate) + ' <span class="sft-rel">' + esc(tzName(d)) + ', your local time</span></div>' +
        '<div class="sft-time-utc">' + esc(fmtTimeFrac(p.t, true, 1)) + ' UTC' + (utcDate !== locDate ? ', ' + esc(utcDate) : '') + '</div>' +
        '<div class="sft-dur">' + svg('clock') + 'Lasts ' + secs(r.duration) + '</div>' +
      '</div>' +
      '<dl class="sft-facts">' +
        '<div class="sft-fact"><dt>Where to watch</dt><dd>' + where + '</dd></div>' +
        '<div class="sft-fact"><dt>Path width</dt><dd><strong>' + km(w) + '</strong>. Outside this strip ' + S + ' misses the ' + B + '.</dd></div>' +
        '<div class="sft-fact"><dt>' + B + ' in the sky</dt><dd><strong>' + Math.round(r.bodyAlt) + '\u00b0</strong> above the horizon, towards the ' +
          compass(r.bodyAz) + ' (azimuth ' + Math.round(r.bodyAz) + '\u00b0). Check nothing blocks that direction.</dd></div>' +
      '</dl>' +
      '<div class="sft-mapwrap"><div class="sft-map" data-sft-map="' + i + '" role="img" aria-label="Map of the transit path and your position"></div>' +
      '<div class="sft-legend"><span><i class="l-you"></i>You</span><span><i class="l-near"></i>Closest point</span><span><i class="l-line"></i>Centre line</span><span><i class="l-path"></i>Visible path</span></div></div>' +
      '<div class="sft-actions"><a class="sft-btn" href="' + gmaps + '" target="_blank" rel="noopener">' + svg('nav') + 'Open in Google Maps</a></div>' +
      '</section>';

    // 2) How it crosses the disc
    var Zc = chordOf(r, 0), Ac = chordOf(r, 0.6), Bc = chordOf(r, -0.6);
    var sideA = compass(r.track + 90), sideB = compass(r.track - 90);
    var pa = function (ch) { return Math.round(paOf(r, ch.entry)) + '\u00b0 \u2192 ' + Math.round(paOf(r, ch.exit)) + '\u00b0'; };
    var rows = '<li><b>Centre line:</b> through the middle, ' + secs(r.duration) + '. Enters and leaves at ' + pa(Zc) + '.</li>' +
      '<li><b>A:</b> ' + km(0.6 * r.wRight) + ' ' + sideA + ' of the centre line, ' + secs(r.duration * Ac.half) + '. ' + pa(Ac) + '.</li>' +
      '<li><b>B:</b> ' + km(0.6 * r.wLeft) + ' ' + sideB + ' of the centre line, ' + secs(r.duration * Bc.half) + '. ' + pa(Bc) + '.</li>';
    if (frac != null) {
      var Uc = chordOf(r, frac);
      rows += '<li class="sft-you"><b>You:</b> ' + secs(Y && Y.duration ? Y.duration : r.duration * Uc.half) + '. ' + pa(Uc) + '.</li>';
    }
    h += '<section class="sft-sec"><h3 class="sft-h3">How it crosses the disc</h3>' +
      '<div class="sft-cross"><figure class="sft-sky">' + skySvg(r, frac, i + '_' + Math.round(r.t / 1000), state.orient) +
      '<figcaption>' + (state.orient === 'north' ? 'North up, east left, as in a telescope without a diagonal' : 'As you see it, straight up at the top') + '</figcaption></figure>' +
      '<div><ul class="sft-chords">' + rows + '</ul>' +
      '<p class="sft-small">Entry and exit points are position angles: degrees round the disc from north, through east. A diagonal or mirror flips the view left to right.</p></div></div>' +
      '</section>';

    // 3) From where you are now (the numbers Transit Finder shows for your location)
    if (Y) {
      h += '<section class="sft-sec"><h3 class="sft-h3">From where you are now</h3><dl class="sft-facts">' +
        '<div class="sft-fact"><dt>What you would see</dt><dd>' + (Y.isTransit
          ? '<span class="sft-inside">A transit:</span> ' + S + ' crosses the ' + B + ' from your spot.'
          : '<strong>A close pass:</strong> ' + S + ' passes ' + fmtSep(Y.sepDeg) + ' from the centre of the ' + B + '.') + '</dd></div>' +
        '<div class="sft-fact"><dt>Closest moment</dt><dd><strong>' + esc(fmtTimeFrac(Y.t, false, 2)) + '</strong> local, ' + esc(fmtTimeFrac(Y.t, true, 2)) + ' UTC</dd></div>' +
        '<div class="sft-fact"><dt>' + S + ' in the sky</dt><dd>Altitude <strong>' + deg1(Y.satAlt) + '</strong>, azimuth <strong>' + deg1(Y.satAz) + '</strong></dd></div>' +
        '<div class="sft-fact"><dt>Centre line distance</dt><dd>' + km(p.distKm) + ', path width ' + km(w) + '</dd></div>' +
        '</dl></section>';
    }

    // 4) Equipment details
    if (Y && Cn) {
      var row = function (label, a, b) { return '<tr><th scope="row">' + label + '</th><td>' + a + '</td><td>' + b + '</td></tr>'; };
      var one = function (label, a) { return '<tr><th scope="row">' + label + '</th><td colspan="2">' + a + '</td></tr>'; };
      var t = '<table class="sft-tbl"><thead><tr><th></th><th scope="col">From you</th><th scope="col">Centre line</th></tr></thead><tbody>';
      t += row('Time', esc(fmtTimeFrac(Y.t, false, 2)), esc(fmtTimeFrac(Cn.t, false, 2)));
      t += row('Angular separation', fmtSep(Y.sepDeg), fmtSep(Cn.sepDeg));
      t += row(S + ' altitude / azimuth', deg1(Y.satAlt) + ' / ' + deg1(Y.satAz), deg1(Cn.satAlt) + ' / ' + deg1(Cn.satAz));
      t += row(B + ' altitude / azimuth', deg1(Y.bodyAlt) + ' / ' + deg1(Y.bodyAz), deg1(Cn.bodyAlt) + ' / ' + deg1(Cn.bodyAz));
      t += row(S + ' angular size', arcsec(Y.sizeArcsec), arcsec(Cn.sizeArcsec));
      t += row(S + ' distance', Y.rangeKm.toFixed(1) + ' km', Cn.rangeKm.toFixed(1) + ' km');
      t += row(S + ' R.A. / Dec', fmtRA(Y.satRaH) + '<br>' + fmtDMS(Y.satDecDeg), fmtRA(Cn.satRaH) + '<br>' + fmtDMS(Cn.satDecDeg));
      t += row(B + ' R.A. / Dec', fmtRA(Y.bodyRaH) + '<br>' + fmtDMS(Y.bodyDecDeg), fmtRA(Cn.bodyRaH) + '<br>' + fmtDMS(Cn.bodyDecDeg));
      t += row('Parallactic angle', signed1(Y.parallactic) + '\u00b0', signed1(Cn.parallactic) + '\u00b0');
      t += row('Angular speed', Y.angSpeedArcminS.toFixed(1) + '\u2032/s', Cn.angSpeedArcminS.toFixed(1) + '\u2032/s');
      t += row('Speed: transverse', Y.vTransverse.toFixed(2) + ' km/s', Cn.vTransverse.toFixed(2) + ' km/s');
      t += row('Speed: radial', signed2(Y.vRadial) + ' km/s', signed2(Cn.vRadial) + ' km/s');
      t += row('Speed: total', Y.vTotal.toFixed(2) + ' km/s', Cn.vTotal.toFixed(2) + ' km/s');
      t += row('Direction of motion from straight up (clockwise)', deg1(Y.dirZenith), deg1(Cn.dirZenith));
      t += row('Direction of motion (position angle)', deg1(Y.dirPA), deg1(Cn.dirPA));
      t += row(B + ' angular size', Y.bodyDiamArcmin.toFixed(1) + '\u2032, ' + (Y.bodyDiamArcmin * 60 / Y.sizeArcsec).toFixed(1) + '\u00d7 ' + S,
                                   Cn.bodyDiamArcmin.toFixed(1) + '\u2032, ' + (Cn.bodyDiamArcmin * 60 / Cn.sizeArcsec).toFixed(1) + '\u00d7 ' + S);
      if (!isSun) {
        t += one('Moon illumination', (r.moonIllum != null ? (r.moonIllum * 100).toFixed(1) + '%' : '-') + ', ' + (Y.moonSunSepDeg != null ? Y.moonSunSepDeg.toFixed(1) + '\u00b0 from the Sun' : ''));
        t += row('Sun altitude', deg1(Y.sunAlt), deg1(Cn.sunAlt));
        t += row(S + ' lit by the Sun?', Y.satInShadow ? 'No, in Earth\u2019s shadow' : 'Yes', Cn.satInShadow ? 'No, in Earth\u2019s shadow' : 'Yes');
      }
      // Extras for cameras (centre line)
      var ownLenMs = Cn.sizeArcsec / (Cn.angSpeedArcminS * 60) * 1000;
      var shutter = Math.max(1, Math.round(1000 / (ownLenMs / 10)));
      t += one('Moves its own length in', ownLenMs.toFixed(1) + ' ms (centre line)');
      t += one('Shutter for a sharp outline', '1/' + shutter + ' s or faster (blur under a tenth of its size)');
      t += one('Video frames during the transit', Math.round(r.duration * 30) + ' at 30 fps, ' + Math.round(r.duration * 60) + ' at 60 fps, ' + Math.round(r.duration * 120) + ' at 120 fps');
      t += one('Entry and exit, centre line', pa(Zc));
      t += one('Size used for ' + S, Math.round(sat.sizeKm * 1000) + ' m');
      t += '</tbody></table>';
      h += '<details class="sft-details"><summary>' + svg('sat') + 'Equipment details</summary>' +
        '<p class="sft-small">"From you" is the moment ' + S + ' gets closest to the ' + B + ' as seen from your location, the same numbers Transit Finder gives. "Centre line" is the best spot, shown on the map. R.A. and Dec are for the true equator and equinox of date. Parallactic angle uses Transit Finder\u2019s sign.</p>' +
        '<div class="sft-tblwrap">' + t + '</div></details>';
    }

    var notes = [];
    if (r.sat === 'hst') notes.push('Hubble is only about 13 m long, so its outline is tiny. You will need a telescope or a long telephoto lens to make it out.');
    if (!isSun) {
      if (r.moonIllum != null && r.moonIllum < 0.05) notes.push('The Moon is almost new, so it is barely visible. This transit will be very hard or impossible to see.');
      else if (r.moonIllum != null && r.moonIllum < 0.3) notes.push('Thin Moon: ' + sat.label + ' only shows up against the lit part, so this one will be harder to see.');
      if (r.sunAlt != null && r.sunAlt > -6) notes.push('The sky will still be bright, so the Moon will look washed out.');
    }
    if (notes.length) h += '<div class="sft-moonwarn">' + svg('warn') + '<span>' + notes.join(' ') + '</span></div>';
    h += '</article>';
    return h;
  }

  function drawMap(el, r, p, loc) {
    if (typeof L === 'undefined') { el.parentNode.style.display = 'none'; return null; }
    var map = L.map(el, {
      zoomControl: true, attributionControl: true, scrollWheelZoom: false,
      dragging: !L.Browser.mobile, tap: false
    });
    L.tileLayer(TILE_URL, { maxZoom: 16, attribution: 'Map tiles &copy; Esri' }).addTo(map);
    if (map.attributionControl && map.attributionControl.setPrefix) map.attributionControl.setPrefix(false);

    var pts = r.line.map(function (q) { return [q[0], q[1]]; });
    var left = [], right = [];
    for (var i = 0; i < pts.length; i++) {
      var a = pts[Math.max(0, i - 1)], b = pts[Math.min(pts.length - 1, i + 1)];
      var brg = bearing(a[0], a[1], b[0], b[1]);
      left.push(destination(pts[i][0], pts[i][1], brg - 90, r.wLeft));
      right.push(destination(pts[i][0], pts[i][1], brg + 90, r.wRight));
    }
    if (pts.length > 1) {
      L.polygon(left.concat(right.slice().reverse()), { stroke: false, fillColor: '#c084fc', fillOpacity: 0.25, interactive: false, smoothFactor: 0 }).addTo(map);
      L.polyline(left, { color: '#c084fc', weight: 1, opacity: 0.8, dashArray: '4 4', interactive: false, smoothFactor: 0 }).addTo(map);
      L.polyline(right, { color: '#c084fc', weight: 1, opacity: 0.8, dashArray: '4 4', interactive: false, smoothFactor: 0 }).addTo(map);
      L.polyline(pts, { color: '#e0aaff', weight: 2.5, interactive: false, smoothFactor: 0 }).addTo(map);
    }
    var you = [loc.lat, loc.lng], near = [p.lat, p.lon];
    if (p.distKm > 0.05) L.polyline([you, near], { color: '#8ab4ff', weight: 1.5, dashArray: '3 5', interactive: false }).addTo(map);
    L.circleMarker(near, { radius: 7, color: '#1a1a1a', weight: 2, fillColor: '#ffb547', fillOpacity: 1 })
      .bindTooltip('Closest point on the centre line').addTo(map);
    L.circleMarker(you, { radius: 7, color: '#ffffff', weight: 2, fillColor: '#2196f3', fillOpacity: 1 })
      .bindTooltip('You').addTo(map);

    var trk = r.track || 0;
    var span = Math.max(6, Math.min(60, p.distKm * 0.6));
    var bounds = L.latLngBounds([you, near]);
    bounds.extend(destination(p.lat, p.lon, trk, span));
    bounds.extend(destination(p.lat, p.lon, trk + 180, span));
    map.fitBounds(bounds, { padding: [24, 24], maxZoom: 13 });
    setTimeout(function () { try { map.invalidateSize(); } catch (e) {} }, 60);
    return map;
  }

  // ---------- Tabs ----------
  function setTab(tab, fromUser) {
    state.tab = tab;
    var root = document.documentElement;
    root.classList.toggle('sft-show-transits', tab === 'transits');
    var btns = document.querySelectorAll('[data-sft-tab]');
    for (var i = 0; i < btns.length; i++) {
      var on = btns[i].getAttribute('data-sft-tab') === tab;
      btns[i].setAttribute('aria-selected', on ? 'true' : 'false');
      btns[i].setAttribute('tabindex', on ? '0' : '-1');
    }
    try {
      var u = new URL(location.href);
      if (tab === 'transits') u.searchParams.set('tab', 'transits'); else u.searchParams.delete('tab');
      history.replaceState(history.state, '', u.pathname + u.search + u.hash);
    } catch (e) {}

    if (tab === 'transits') {
      if (fromUser) track('transits_tab_open', { page_path: location.pathname });
      if (state.status === 'computing' || state.status === 'loading-orbit') render();
      else run(false);
      startPolling();
      state.maps.forEach(function (m) { try { m.invalidateSize(); } catch (e) {} });
    } else {
      stopPolling();
      // Let the maps on the Passes tab re-measure themselves
      try { window.dispatchEvent(new Event('resize')); } catch (e) {}
    }
  }

  function startPolling() {
    stopPolling();
    state.pollTimer = setInterval(function () {
      if (state.tab !== 'transits') return;
      var loc = getLoc();
      if (loc && (!state.lastLoc || haversineKm(state.lastLoc.lat, state.lastLoc.lng, loc.lat, loc.lng) > 0.02)) {
        state.lastLoc = loc;
        var base = state.calcLoc;
        if (!base || haversineKm(base.lat, base.lng, loc.lat, loc.lng) > RECOMPUTE_KM) {
          run(false);          // new area: use the cache or calculate again
        } else if (state.status === 'ready') {
          render();            // same area: just re-measure distances from the new spot
        }
      }
      // Keep the "in X min" texts current without redrawing the maps
      if (state.status === 'ready' && panel) {
        var now = Date.now(), els = panel.querySelectorAll('[data-sft-rel]');
        for (var i = 0; i < els.length; i++) els[i].textContent = relTime(+els[i].getAttribute('data-sft-rel') - now);
      }
    }, 1500);
  }
  function stopPolling() { if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; } }

  function initTabs() {
    var btns = Array.prototype.slice.call(document.querySelectorAll('[data-sft-tab]'));
    btns.forEach(function (b, i) {
      b.addEventListener('click', function () { setTab(b.getAttribute('data-sft-tab'), true); });
      b.addEventListener('keydown', function (e) {
        if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
        e.preventDefault();
        var n = btns[(i + (e.key === 'ArrowRight' ? 1 : btns.length - 1)) % btns.length];
        n.focus();
        setTab(n.getAttribute('data-sft-tab'), true);
      });
    });
  }

  // ---------- Public API ----------
  function init(options) {
    opts = options || {};
    panel = typeof opts.panel === 'string' ? document.querySelector(opts.panel) : (opts.panel || document.getElementById('transitsPanel'));
    if (!panel) return;
    panel.classList.add('sft-panel');
    var r = parseInt(lsGet(RADIUS_KEY) || '', 10);
    state.radius = RADII.indexOf(r) !== -1 ? r : DEFAULT_RADIUS;
    try {
      var saved = JSON.parse(lsGet(SATS_KEY) || 'null');
      if (saved) SATS.forEach(function (c) { if (typeof saved[c.id] === 'boolean') state.sats[c.id] = saved[c.id]; });
    } catch (e) {}
    state.orient = lsGet(ORIENT_KEY) === 'north' ? 'north' : 'zenith';
    initTabs();
    setTab(tabFromUrl(), false);
  }

  global.SatFleetTransits = {
    init: init,
    showTab: function (t) { setTab(t === 'transits' ? 'transits' : 'passes', true); },
    recalculate: function () { run(true); }
  };
})(window);