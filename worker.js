/**
 * SatFleet Live — Cloudflare Worker Mark V
 * 
 * API Space Devs v2.3.0 + KV cache + Token Auth + Throttle info
 * 
 * Variables de entorno necesarias (Cloudflare Dashboard → Workers → Settings → Variables):
 *   - SPACEDEVS_TOKEN  (secret)  → tu API Key de The Space Devs
 * 
 * KV Namespace necesario:
 *   - LAUNCHES_KV  → binding con una KV Namespace llamada LAUNCHES_KV
 */

'use strict';

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════

const CELESTRAK_URL   = 'https://jaimeautomatiza.github.io/tle-proxy/data/tle.json';
const CELESTRAK_META_URL = 'https://jaimeautomatiza.github.io/tle-proxy/data/meta.json';
const SPACEDEVS_BASE  = 'https://ll.thespacedevs.com/2.3.0';
const SPACEDEVS_URL   = `${SPACEDEVS_BASE}/launches/upcoming/?limit=100&mode=detailed&format=json`;
const THROTTLE_URL    = `${SPACEDEVS_BASE}/api-throttle/`;

const TLE_TTL         = 12 * 3600;   // 12 horas (Cache API)
const TLE_STALE       = 3600;

// TTLs en segundos para el KV (se ajustan dinámicamente)
const TTL_LIVE        = 5  * 60;     // 5 min  — lanzamiento inminente / en vuelo
const TTL_SOON        = 15 * 60;     // 15 min — < 24 horas
const TTL_DEFAULT     = 60 * 60;     // 1 hora — lanzamiento lejano

const KV_KEY_LAUNCHES = 'launches_v3';
const KV_KEY_META     = 'launches_meta_v3';  // { lastFetch, throttleInfo, ttlUsed }

const CACHE_KEY_TLE   = 'https://internal.satfleetlive/cache/tle-v3';

// ═══════════════════════════════════════════════════════════════
// RATE LIMITING (in-memory, por isolate)
// ═══════════════════════════════════════════════════════════════

const RL_WINDOW_MS = 60_000;
const RL_MAX_REQ   = 60;
const _rl = new Map();

function rateLimited(ip) {
  const now = Date.now();
  const rec = _rl.get(ip);
  if (!rec || now > rec.reset) {
    _rl.set(ip, { n: 1, reset: now + RL_WINDOW_MS });
    return false;
  }
  if (rec.n >= RL_MAX_REQ) return true;
  rec.n++;
  return false;
}

function pruneRL() {
  const now = Date.now();
  for (const [k, v] of _rl) {
    if (now > v.reset) _rl.delete(k);
  }
}

// ═══════════════════════════════════════════════════════════════
// HEADERS
// ═══════════════════════════════════════════════════════════════

const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Accept',
  'Access-Control-Expose-Headers': 'X-TLE-Updated',
};

const SEC_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options':        'DENY',
  'X-XSS-Protection':       '1; mode=block',
  'Referrer-Policy':        'strict-origin-when-cross-origin',
  'Permissions-Policy':     'geolocation=(), microphone=()',
};

function makeHeaders(extra = {}) {
  return { ...CORS_HEADERS, ...SEC_HEADERS, ...extra };
}

function wrapCached(cached) {
  const h = new Headers(cached.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) h.set(k, v);
  return new Response(cached.body, { status: cached.status, headers: h });
}

// ═══════════════════════════════════════════════════════════════
// TRIM — solo los campos que usa el frontend
// ═══════════════════════════════════════════════════════════════

function trimLaunch(l) {
  return {
    id:           l.id,
    name:         l.name,
    net:          l.net,
    window_start: l.window_start,
    window_end:   l.window_end,
    status: l.status ? {
      id:          l.status.id,
      abbrev:      l.status.abbrev,
      name:        l.status.name,
      description: l.status.description,
    } : null,
    probability:   l.probability   ?? null,
    webcast_live:  l.webcast_live  ?? false,
    weather_concerns: l.weather_concerns ?? null,
    rocket: l.rocket ? {
      configuration: l.rocket.configuration ? {
        name:                    l.rocket.configuration.name,
        family:                  l.rocket.configuration.family,
        variant:                 l.rocket.configuration.variant,
        description:             l.rocket.configuration.description,
        total_launch_count:      l.rocket.configuration.total_launch_count,
        successful_launches:     l.rocket.configuration.successful_launches,
        failed_launches:         l.rocket.configuration.failed_launches,
        pending_launches:        l.rocket.configuration.pending_launches,
      } : null,
    } : null,
    launch_service_provider: l.launch_service_provider ? {
        name:         l.launch_service_provider.name,
        type:         l.launch_service_provider.type?.name || l.launch_service_provider.type || '',
        country_code: l.launch_service_provider.country_code,
        description:  l.launch_service_provider.description,
        logo: l.launch_service_provider.logo ? {
        image_url: l.launch_service_provider.logo.image_url,
      } : null,
    } : null,
    mission: l.mission ? {
      name:        l.mission.name,
      description: l.mission.description,
      type:        l.mission.type,
      orbit: l.mission.orbit ? {
        name:   l.mission.orbit.name,
        abbrev: l.mission.orbit.abbrev,
      } : null,
    } : null,
    pad: l.pad ? {
      name:               l.pad.name,
      latitude:           l.pad.latitude,
      longitude:          l.pad.longitude,
      total_launch_count: l.pad.total_launch_count,
      location: l.pad.location ? {
        name:         l.pad.location.name,
        country_code: l.pad.location.country_code,
      } : null,
    } : null,
    image: l.image ? {
      image_url:     l.image.image_url,
      thumbnail_url: l.image.thumbnail_url,
    } : null,
    vid_urls: (l.vid_urls || []).map(v => ({
      url:   v.url,
      title: v.title,
    })),
    info_urls: (l.info_urls || []).map(v => ({
      url:   v.url,
      title: v.title,
    })),
  };
}

// ═══════════════════════════════════════════════════════════════
// ONESIGNAL — PUSH NOTIFICATIONS
// ═══════════════════════════════════════════════════════════════

async function sendOneSignalToPlayer(env, playerId, headings, contents, sendAfterIso) {
  const appId  = env.ONESIGNAL_APP_ID;
  const apiKey = env.ONESIGNAL_REST_API_KEY;
  if (!appId || !apiKey || !playerId) return;

  const body = {
    app_id:             appId,
    include_subscription_ids: [playerId],
    headings,
    contents,
    large_icon:        'https://satfleetlive.com/images/logo.png',
    chrome_web_icon:   'https://satfleetlive.com/images/logo.png',
    priority:          10,
    ttl:               3600,
  };

  if (sendAfterIso) body.send_after = sendAfterIso;

  try {
    const res = await fetch('https://onesignal.com/api/v1/notifications', {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Basic ${apiKey}`,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const txt = await res.text();
      console.error('OneSignal player notif error', res.status, txt);
      return null;
    }
    const json = await res.json();
    return json.id || null;
  } catch (err) {
    console.error('OneSignal player fetch error:', err.message);
    return null;
  }
}

async function handleNotifyPass(request, env) {
  let payload;
  try {
    payload = await request.json();
  } catch(e) {
    return new Response(JSON.stringify({ error: 'Invalid JSON' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const { playerId, satelliteName, passTimeIso, maxElevation, direction, brightness, cancel } = payload;

  if (!playerId || !passTimeIso || !satelliteName) {
    return new Response(JSON.stringify({ error: 'Missing fields' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  if (cancel) {
    const { notifIds } = payload;
    if (notifIds && Array.isArray(notifIds) && notifIds.length) {
      const appId  = env.ONESIGNAL_APP_ID;
      const apiKey = env.ONESIGNAL_REST_API_KEY;
      await Promise.all(notifIds.map(id =>
        fetch(`https://onesignal.com/api/v1/notifications/${id}?app_id=${appId}`, {
          method: 'DELETE',
          headers: { 'Authorization': `Basic ${apiKey}` },
        }).catch(() => {})
      ));
    }
    return new Response(JSON.stringify({ ok: true, cancelled: true }), {
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const passTime = new Date(passTimeIso).getTime();
  const now      = Date.now();

  if (passTime <= now) {
    return new Response(JSON.stringify({ error: 'Pass already happened' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const magStr = (typeof brightness === 'number')
    ? `Mag ${brightness >= 0 ? '+' : ''}${brightness.toFixed(1)}`
    : '';

  // Deduplicación: si ya programamos notif para este pase, devolvemos las mismas
  const dedupKey = `pass_notif_${playerId}_${passTimeIso}`;
  try {
    const existing = await env.LAUNCHES_KV.get(dedupKey);
    if (existing) {
      const parsed = JSON.parse(existing);
      return new Response(JSON.stringify({ ok: true, scheduled: parsed.notifIds.length, notifIds: parsed.notifIds, deduplicated: true }), {
        headers: makeHeaders({ 'Content-Type': 'application/json' }),
      });
    }
  } catch(e) {}

  const alerts = [
    { ms: 10 * 60 * 1000, label: '10 minutes' },
    { ms:  2 * 60 * 1000, label: '2 minutes'  },
  ];

  const promises = alerts.map(({ ms, label }) => {
    const fireAt = passTime - ms;
    if (fireAt <= now) return Promise.resolve();

    const sendAfterIso = new Date(fireAt).toISOString();
    return sendOneSignalToPlayer(
      env,
      playerId,
      { en: `${satelliteName} passes in ${label}!` },
      { en: `Max ${maxElevation}° · ${direction}${magStr ? ' · ' + magStr : ''}` },
      sendAfterIso
    );
  });

  const results = await Promise.all(promises);
  const notifIds = results.filter(Boolean);

  // Guardamos en KV para deduplicar futuros clicks
  const ttlSeconds = Math.max(60, Math.ceil((passTime - Date.now()) / 1000) + 3600);
  try {
    await env.LAUNCHES_KV.put(dedupKey, JSON.stringify({ notifIds }), { expirationTtl: ttlSeconds });
  } catch(e) {}

  return new Response(JSON.stringify({ ok: true, scheduled: notifIds.length, notifIds }), {
    headers: makeHeaders({ 'Content-Type': 'application/json' }),
  });
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/tle/:noradId — TLE de un satélite concreto
// ═══════════════════════════════════════════════════════════════
const KV_KEY_TLE_SINGLE_PREFIX = 'tle_single_v2_';
const TLE_SINGLE_TTL = 12 * 3600;

async function handleTleSingle(request, env) {
  const noradId = new URL(request.url).pathname.split('/').pop();
  if (!noradId || !/^\d+$/.test(noradId)) {
    return new Response('Invalid NORAD ID', { status: 400, headers: makeHeaders({ 'Content-Type': 'text/plain' }) });
  }

  // 1. Primero miramos el KV (caché por satélite, 6 horas)
  const kvKey = KV_KEY_TLE_SINGLE_PREFIX + noradId;
  try {
    const cached = await env.LAUNCHES_KV.get(kvKey);
    if (cached) return new Response(cached, { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache': 'HIT' }) });
  } catch(e) {}

  // 2. Si no está en KV, buscamos en el JSON completo del proxy (ya cacheado por Cloudflare)
  try {
    const bulkRes = await fetch(CELESTRAK_URL, {
      headers: { 'User-Agent': 'SatFleetLive/3.0 (https://satfleetlive.com)', 'Accept': 'application/json' },
    });
    if (!bulkRes.ok) throw new Error(`Proxy returned ${bulkRes.status}`);

    const allSats = await bulkRes.json();
    const sat = allSats.find(s => String(s.NORAD_CAT_ID) === String(noradId));

    if (!sat) return new Response('Not found', { status: 404, headers: makeHeaders({ 'Content-Type': 'text/plain' }) });

    const result = JSON.stringify([sat]);
    try { await env.LAUNCHES_KV.put(kvKey, result, { expirationTtl: TLE_SINGLE_TTL }); } catch(e) {}
    return new Response(result, { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache': 'MISS' }) });
  } catch(err) {
    return new Response('Error: ' + err.message, { status: 502, headers: makeHeaders({ 'Content-Type': 'text/plain' }) });
  }
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/satellite/:noradId — Info desde Wikidata
// ═══════════════════════════════════════════════════════════════

const KV_KEY_SAT_INFO_PREFIX = 'sat_info_v4_';
const SAT_INFO_TTL = 30 * 24 * 3600;

async function handleSatelliteInfo(request, env) {
  const noradId = new URL(request.url).pathname.split('/').pop();
  if (!noradId || !/^\d+$/.test(noradId)) {
    return new Response(JSON.stringify({ error: 'Invalid NORAD ID' }), { status: 400, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
  }

  const kvKey = KV_KEY_SAT_INFO_PREFIX + noradId;
  const kvKeyLock = kvKey + '_lock';
  try {
    const cached = await env.LAUNCHES_KV.get(kvKey);
    if (cached) return new Response(cached, { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json', 'X-Cache': 'HIT' }) });
    const lock = await env.LAUNCHES_KV.get(kvKeyLock);
    if (lock) return new Response(JSON.stringify({ noradId }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json', 'X-Cache': 'LOCK' }) });
    await env.LAUNCHES_KV.put(kvKeyLock, '1', { expirationTtl: 60 });
  } catch(e) {}

  const query = `
SELECT ?item ?itemLabel ?description ?launchDate ?mass ?image
       ?operatorLabel ?manufacturerLabel ?countryLabel ?launchVehicleLabel ?article WHERE {
  ?item wdt:P377 "${noradId}" .
  OPTIONAL { ?item schema:description ?description . FILTER(LANG(?description) = "en") }
  OPTIONAL { ?item wdt:P619 ?launchDate }
  OPTIONAL { ?item wdt:P1090 ?mass }
  OPTIONAL { ?item wdt:P18 ?image }
  OPTIONAL { ?item wdt:P137 ?operator }
  OPTIONAL { ?item wdt:P176 ?manufacturer }
  OPTIONAL { ?item wdt:P17 ?country }
  OPTIONAL { ?item wdt:P375 ?launchVehicle }
  OPTIONAL {
    ?article schema:about ?item .
    ?article schema:inLanguage "en" .
    FILTER(CONTAINS(STR(?article), "en.wikipedia.org"))
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
LIMIT 1`;

  let info = {};
  try {
    const sparqlRes = await fetch(
      'https://query.wikidata.org/sparql?query=' + encodeURIComponent(query) + '&format=json',
      { headers: { 'Accept': 'application/json', 'User-Agent': 'SatFleetLive/3.0 (https://satfleetlive.com; contact: jaime.automatiza@gmail.com)' } }
    );
    if (sparqlRes.ok) {
      const data = await sparqlRes.json();
      const b = data.results?.bindings?.[0];
      if (b) {
        info = {
          name:          b.itemLabel?.value || null,
          description:   b.description?.value || null,
          launchDate:    b.launchDate?.value?.substring(0, 10) || null,
          mass:          b.mass?.value ? Math.round(parseFloat(b.mass.value)) + ' kg' : null,
          image:         b.image?.value ? b.image.value.replace('http://', 'https://') : null,
          operator:      b.operatorLabel?.value || null,
          manufacturer:  b.manufacturerLabel?.value || null,
          country:       b.countryLabel?.value || null,
          launchVehicle: b.launchVehicleLabel?.value || null,
          wikidataUrl:   b.item?.value || null,
          wikipediaUrl:  null,
        };

        const wikiUrl = b.article?.value;
        if (wikiUrl) {
          try {
            const title = decodeURIComponent(wikiUrl.split('/wiki/')[1] || '');
            if (title) {
              const wikiRes = await fetch(
                `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`,
                { headers: { 'User-Agent': 'SatFleetLive/3.0 (https://satfleetlive.com; contact: jaime.automatiza@gmail.com)' } }
              );
              if (wikiRes.ok) {
                const wikiData = await wikiRes.json();
                if (wikiData.extract && wikiData.extract.length > 80) {
                  info.description = wikiData.extract;
                }
                if (!info.image && wikiData.originalimage?.source) {
                  info.image = wikiData.originalimage.source;
                } else if (!info.image && wikiData.thumbnail?.source) {
                  info.image = wikiData.thumbnail.source;
                }
                info.wikipediaUrl = wikiUrl;
              }
            }
          } catch(wikiErr) { console.error('Wikipedia summary error:', wikiErr.message); }
        }
      }
    }
  } catch(e) { console.error('Wikidata error:', e.message); }

  const result = JSON.stringify({ noradId, ...info });
  try {
    await env.LAUNCHES_KV.put(kvKey, result, { expirationTtl: SAT_INFO_TTL });
    await env.LAUNCHES_KV.delete(kvKeyLock);
  } catch(e) {}
  return new Response(result, { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json', 'X-Cache': 'MISS' }) });
}

async function sendOneSignalNotification(env, headings, contents, data = {}) {
  const appId  = env.ONESIGNAL_APP_ID;
  const apiKey = env.ONESIGNAL_REST_API_KEY;
  if (!appId || !apiKey) return;

  try {
    const res = await fetch('https://onesignal.com/api/v1/notifications', {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Basic ${apiKey}`,
      },
      body: JSON.stringify({
        app_id:            appId,
        included_segments: ['Total Subscriptions'],
        headings,
        contents,
        data,
        large_icon:        'https://satfleetlive.com/images/logo.png',
        chrome_web_icon:   'https://satfleetlive.com/images/logo.png',
        chrome_web_badge:  'https://satfleetlive.com/images/logo.png',
        firefox_icon:      'https://satfleetlive.com/images/logo.png',
        priority:          10,
        ttl:               3600,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      console.error('OneSignal HTTP', res.status, body);
    }
  } catch (err) {
    console.error('OneSignal fetch error:', err.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// TTL DINÁMICO
// ═══════════════════════════════════════════════════════════════

function calcDynamicTTL(results) {
  if (!results || results.length === 0) return TTL_DEFAULT;

  const now = Date.now();
  const times = results
    .map(l => new Date(l.net).getTime() - now)
    .filter(d => d > -3_600_000)
    .sort((a, b) => a - b);

  if (times.length === 0) return TTL_DEFAULT;

  const nextMs    = times[0];
  const hoursAway = nextMs / 3_600_000;

  const hasLive = results.some(l =>
    l.webcast_live === true || l.status?.abbrev === 'In Flight'
  );

  if (hasLive || hoursAway < 2)   return TTL_LIVE;
  if (hoursAway < 24)             return TTL_SOON;
  return TTL_DEFAULT;
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/tle
// ═══════════════════════════════════════════════════════════════

async function handleTle(ctx, env) {
  const cache = caches.default;
  const hit = await cache.match(CACHE_KEY_TLE);
  if (hit) return wrapCached(hit);

  const cooldownUntil = await env.LAUNCHES_KV.get('celestrak:cooldown');
  if (cooldownUntil && Date.now() < parseInt(cooldownUntil)) {
    return new Response('Esperando a que CelesTrak se recupere...', { status: 502, headers: makeHeaders({ 'Content-Type': 'text/plain' }) });
  }

  // Fetch TLE y meta en paralelo
  let upstream, tleUpdatedAt = null;
  try {
    const [tleRes, metaRes] = await Promise.all([
      fetch(CELESTRAK_URL, {
        headers: {
          'User-Agent': 'SatFleetLive/3.0 (https://satfleetlive.com; contact: jaime.automatiza@gmail.com)',
          'Accept':     'application/json',
        },
      }),
      fetch(CELESTRAK_META_URL).catch(() => null),
    ]);

    upstream = tleRes;

    // Leer la fecha de actualización del meta.json si está disponible
    if (metaRes && metaRes.ok) {
      try {
        const meta = await metaRes.json();
        tleUpdatedAt = meta.updated || null;
      } catch(e) {}
    }
  } catch (err) {
    ctx.waitUntil(env.LAUNCHES_KV.put('celestrak:cooldown', String(Date.now() + 300000), { expirationTtl: 300 }));
    return new Response('Celestrak unreachable: ' + err.message, {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'text/plain' }),
    });
  }

  if (!upstream.ok) {
    ctx.waitUntil(env.LAUNCHES_KV.put('celestrak:cooldown', String(Date.now() + 300000), { expirationTtl: 300 }));
    return new Response(`Celestrak returned HTTP ${upstream.status}`, {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'text/plain' }),
    });
  }

  const rawText = await upstream.text();
  let gpData;
  try {
    gpData = JSON.parse(rawText);
  } catch(e) {
    ctx.waitUntil(env.LAUNCHES_KV.put('celestrak:cooldown', String(Date.now() + 300000), { expirationTtl: 300 }));
    return new Response('CelesTrak no devolvió un JSON válido. Probablemente bloqueo activo.', {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'text/plain' })
    });
  }

  const body = JSON.stringify(gpData);

  const resp = new Response(body, {
    status: 200,
    headers: makeHeaders({
      'Content-Type':   'application/json; charset=utf-8',
      'Cache-Control':  `s-maxage=${TLE_TTL}, max-age=${TLE_TTL}, stale-while-revalidate=3600`,
      'Vary':           'Accept-Encoding',
      // ← AQUÍ: visible en F12 → Network → /api/tle → Response Headers
      ...(tleUpdatedAt ? { 'X-TLE-Updated': tleUpdatedAt } : {}),
    }),
  });

  ctx.waitUntil(cache.put(CACHE_KEY_TLE, resp.clone()));
  return resp;
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/launches/upcoming
// ═══════════════════════════════════════════════════════════════

async function handleLaunches(ctx, env, forceRefresh = false) {
  let cached = null;
  let meta   = null;

  try {
    const [rawData, rawMeta] = await Promise.all([
      env.LAUNCHES_KV.get(KV_KEY_LAUNCHES),
      env.LAUNCHES_KV.get(KV_KEY_META),
    ]);

    if (rawData) cached = JSON.parse(rawData);
    if (rawMeta) meta   = JSON.parse(rawMeta);
  } catch (e) {
    console.error('KV read error:', e.message);
  }

  const now       = Date.now();
  const ttlUsed   = meta?.ttlUsed   ?? TTL_DEFAULT;
  const lastFetch = meta?.lastFetch ?? 0;
  const isFresh   = !forceRefresh && cached && (now - lastFetch) < ttlUsed * 1000;

  if (isFresh) {
    return new Response(JSON.stringify({
      results:      cached.results || [],
      _meta: {
        source:       'kv_cache',
        lastFetch:    new Date(lastFetch).toISOString(),
        ttlUsed,
        nextRefreshIn: Math.round((ttlUsed - (now - lastFetch) / 1000)),
        throttle:     meta?.throttle ?? null,
      }
    }), {
      status: 200,
      headers: makeHeaders({
        'Content-Type':  'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        'X-Cache':       'HIT',
      }),
    });
  }

  const authHeader = env.SPACEDEVS_TOKEN
    ? { 'Authorization': `Token ${env.SPACEDEVS_TOKEN}` }
    : {};

  let upstream;
  try {
    upstream = await fetch(SPACEDEVS_URL, {
      headers: {
        'User-Agent': 'SatFleetLive/3.0 (https://satfleetlive.com)',
        'Accept':     'application/json',
        ...authHeader,
      },
    });
  } catch (err) {
    if (cached) {
      return new Response(JSON.stringify({
        results: cached.results || [],
        _meta: { source: 'stale_cache', error: err.message, lastFetch: new Date(lastFetch).toISOString() }
      }), {
        status: 200,
        headers: makeHeaders({ 'Content-Type': 'application/json', 'X-Cache': 'STALE' }),
      });
    }
    return new Response(JSON.stringify({ error: 'Space Devs unreachable: ' + err.message }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  if (upstream.status === 429) {
    const retryAfter = parseInt(upstream.headers.get('Retry-After') || '300', 10);
    const banMeta = {
      lastFetch,
      ttlUsed:   retryAfter,
      throttle: { banned: true, retryAfter, message: 'Rate limited by The Space Devs API' },
    };
    ctx.waitUntil(env.LAUNCHES_KV.put(KV_KEY_META, JSON.stringify(banMeta), { expirationTtl: retryAfter + 60 }));

    const body = JSON.stringify({
      results: cached?.results || [],
      _meta:   { source: 'stale_cache', ...banMeta }
    });
    return new Response(body, {
      status: 200,
      headers: makeHeaders({ 'Content-Type': 'application/json', 'X-Cache': 'STALE-429' }),
    });
  }

  if (!upstream.ok) {
    const errBody = JSON.stringify({ error: `Space Devs error ${upstream.status}`, results: cached?.results || [] });
    return new Response(errBody, {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const data     = await upstream.json();
  const results  = (data.results || []).map(trimLaunch);
  const dynamicTTL = calcDynamicTTL(results);

  let throttleInfo = meta?.throttle ?? null;
  ctx.waitUntil((async () => {

    try {
      const tr = await fetch(THROTTLE_URL, {
        headers: { 'Accept': 'application/json', ...authHeader }
      });
      if (tr.ok) throttleInfo = await tr.json();
    } catch(e) {}

    const nowMs = Date.now();
    const newMeta = {
      lastFetch: nowMs,
      ttlUsed:   dynamicTTL,
      throttle:  throttleInfo,
    };

    const notifPromises = [];

    if (cached?.results?.length && results.length) {
      const lastFetch = meta?.lastFetch ?? 0;
      const FIVE_MIN  = 5 * 60 * 1000;
      const oldMap = new Map(cached.results.map(l => [l.id, l]));

      for (const newL of results) {
        const oldL = oldMap.get(newL.id);

        if (newL.webcast_live === true && (!oldL || !oldL.webcast_live)) {
          const liveKey = `notified_live_${newL.id}`;
          const alreadySent = await env.LAUNCHES_KV.get(liveKey).catch(() => null);
          if (!alreadySent) {
            await env.LAUNCHES_KV.put(liveKey, '1', { expirationTtl: 7200 }).catch(() => {});
            notifPromises.push(sendOneSignalNotification(env,
              { en: '🔴 Live now',    es: '🔴 En directo' },
              {
                en: `${newL.name} is streaming live right now. Watch it on SatFleet!`,
                es: `La misión ${newL.name} está transmitiendo en directo. ¡Síguelo en SatFleet!`,
              },
              { launchId: newL.id, type: 'webcast_live' }
            ));
          }
        }

        if (newL.net) {
          const launchTime       = new Date(newL.net).getTime();
          const msUntilNow       = launchTime - nowMs;
          const msUntilLastFetch = launchTime - lastFetch;

          if (msUntilNow > -60_000 && msUntilNow <= FIVE_MIN) {
          const notifKey = `notified_t5_${newL.id}`;
          const alreadySent = await env.LAUNCHES_KV.get(notifKey).catch(() => null);
          if (!alreadySent) {
            await env.LAUNCHES_KV.put(notifKey, '1', { expirationTtl: 3600 }).catch(() => {});
            notifPromises.push(sendOneSignalNotification(env,
              { en: 'Liftoff imminent', es: 'Despegue inminente' },
              {
                en: `${newL.name} launches in less than 5 minutes!`,
                es: `¡La misión ${newL.name} despega en menos de 5 minutos!`,
              },
              { launchId: newL.id, type: 't_minus_5' }
            ));
          }
        }
        }

        if (
          newL.status?.abbrev === 'Go' &&
          oldL && oldL.status?.abbrev !== 'Go' &&
          new Date(newL.net).getTime() - nowMs > FIVE_MIN
        ) {
          notifPromises.push(sendOneSignalNotification(env,
            { en: '✅ Launch confirmed', es: '✅ Lanzamiento confirmado' },
            {
              en: `Mission ${newL.name} is GO for launch. Add it to your calendar!`,
              es: `La misión ${newL.name} tiene luz verde. ¡Apúntalo en tu calendario!`,
            },
            { launchId: newL.id, type: 'status_go' }
          ));
        }

        if (
          !oldL &&
          newL.net &&
          new Date(newL.net).getTime() - nowMs < 48 * 3_600_000 &&
          new Date(newL.net).getTime() > nowMs
        ) {
          notifPromises.push(sendOneSignalNotification(env,
            { en: 'New launch in 48 h', es: 'Nuevo lanzamiento en 48 h' },
            {
              en: `${newL.name} just appeared on the schedule — launches within 48 hours!`,
              es: `¡${newL.name} acaba de aparecer en el calendario y despega en menos de 48 horas!`,
            },
            { launchId: newL.id, type: 'new_launch_soon' }
          ));
        }
      }
    }

    await Promise.all([
      env.LAUNCHES_KV.put(KV_KEY_LAUNCHES, JSON.stringify({ results }), { expirationTtl: dynamicTTL + 300 }),
      env.LAUNCHES_KV.put(KV_KEY_META,     JSON.stringify(newMeta),     { expirationTtl: dynamicTTL + 300 }),
      ...notifPromises,
    ]);
  })());

  const responseBody = JSON.stringify({
    results,
    _meta: {
      source:    'fresh_fetch',
      lastFetch: new Date().toISOString(),
      ttlUsed:   dynamicTTL,
      throttle:  throttleInfo,
    }
  });

  return new Response(responseBody, {
    status: 200,
    headers: makeHeaders({
      'Content-Type':  'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
      'X-Cache':       'MISS',
    }),
  });
}

// ═══════════════════════════════════════════════════════════════
// ROUTER PRINCIPAL
// ═══════════════════════════════════════════════════════════════

export default {
  async scheduled(event, env, ctx) {
    const fakeCtx = { waitUntil: (p) => ctx.waitUntil(p) };
    await handleLaunches(fakeCtx, env, true); // fuerza refresh para ejecutar notificaciones
  },

  async fetch(request, env, ctx) {
    const { pathname } = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: makeHeaders() });
    }

    const ip = request.headers.get('CF-Connecting-IP') ?? '0.0.0.0';
    if (rateLimited(ip)) {
      return new Response(JSON.stringify({ error: 'Too Many Requests' }), {
        status: 429,
        headers: makeHeaders({ 'Content-Type': 'application/json', 'Retry-After': '60' }),
      });
    }
    if (Math.random() < 0.01) ctx.waitUntil(Promise.resolve().then(pruneRL));

    if (pathname === '/api/notify/pass' && request.method === 'POST') return handleNotifyPass(request, env);

    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405, headers: makeHeaders() });
    }

    if (pathname === '/api/tle') return handleTle(ctx, env);
    if (pathname.startsWith('/api/tle/'))        return handleTleSingle(request, env);
    if (pathname.startsWith('/api/satellite/'))  return handleSatelliteInfo(request, env);
    if (pathname.startsWith('/api/launches/upcoming')) return handleLaunches(ctx, env);

    return new Response(JSON.stringify({ error: 'Not Found', path: pathname }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  },
};