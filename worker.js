/**
 * SatFleet Live — Cloudflare Worker Mark 23 VERSIÓN PREMIUM PERRRROOO
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

// ── Fuentes originales (antes las descargaba GitHub Actions) ──
const CELESTRAK_GP_URL = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json';
const SPACETRACK_BASE  = 'https://www.space-track.org';

// ── Claves KV de los datasets (sin expirationTtl: si una API falla se sigue sirviendo el último dato válido) ──
const KV_KEY_TLE         = 'tle_active_v1';
const KV_KEY_TLE_META    = 'tle_meta_v1';
const KV_KEY_DEBRIS      = 'debris_v1';
const KV_KEY_REENTRIES   = 'reentries_v1';
const KV_KEY_ROVER_PREFIX = 'rover_trail_v1_';

// ── Crons (deben coincidir EXACTAMENTE con los Cron Triggers de wrangler.toml / dashboard) ──
const TLE_CRON        = '7 */2 * * *';   // fetch-tle.yml
const DEBRIS_CRON     = '41 */12 * * *'; // fecth-debris.yml
const REENTRIES_CRON  = '17 */6 * * *';  // fetch-reentries.yml
const ROVER_CRON      = '23 */6 * * *';  // fetch-rover-trail.yml
const SPACEDEVS_BASE  = 'https://ll.thespacedevs.com/2.3.0';
const SPACEDEVS_URL   = `${SPACEDEVS_BASE}/launches/upcoming/?limit=100&mode=detailed&format=json`;
const THROTTLE_URL    = `${SPACEDEVS_BASE}/api-throttle/`;

const TLE_TTL         = 1800;        // 30 min (Cache API + navegador); el KV se refresca cada 2 h
const TLE_STALE       = 3600;

// TTLs en segundos para el KV (se ajustan dinámicamente)
const TTL_LIVE        = 5  * 60;     // 5 min  — lanzamiento inminente / en vuelo
const TTL_SOON        = 15 * 60;     // 15 min — < 24 horas
const TTL_DEFAULT     = 60 * 60;     // 1 hora — lanzamiento lejano

const KV_KEY_LAUNCHES = 'launches_v3';
const KV_KEY_META     = 'launches_meta_v3';  // { lastFetch, throttleInfo, ttlUsed }

const CACHE_KEY_TLE   = 'https://internal.satfleetlive/cache/tle-v4';

// ═══════════════════════════════════════════════════════════════
// ORBITADORES LUNARES/PLANETARIOS — JPL Horizons
// ═══════════════════════════════════════════════════════════════

const HORIZONS_BASE       = 'https://ssd.jpl.nasa.gov/api/horizons.api';
const KV_KEY_MOON_ORBITERS = 'moon_orbiters_v1';
const KV_KEY_MARS_ORBITERS = 'mars_orbiters_v1';
const MOON_ORBITERS_TTL    = 6 * 3600;
const MARS_ORBITERS_TTL    = 6 * 3600;

const MOON_ORBITER_TARGETS = [
  { id: 'lro',    name: 'LRO (NASA)',           bodyCommand: '301', observerCommand: '-85',  bodyRadiusKm: 1737.4, launchDate: '2009-06-18' },
  { id: 'ch2',    name: 'Chandrayaan-2 (ISRO)', bodyCommand: '301', observerCommand: '-152', bodyRadiusKm: 1737.4, launchDate: '2019-07-22' },
  { id: 'danuri', name: 'Danuri (KARI)',        bodyCommand: '301', observerCommand: '-155', bodyRadiusKm: 1737.4, launchDate: '2022-08-04' },
];

// Solo orbitadores con vectores REALES en Horizons. MAVEN, ExoMars TGO y
// Tianwen-1 no los tienen (comprobado) — se quedan fuera hasta que existan.
const MARS_ORBITER_TARGETS = [
  { id: 'mro',     name: 'MRO (NASA)',          bodyCommand: '499', observerCommand: '-74', bodyRadiusKm: 3389.5, launchDate: '2005-08-12' },
  { id: 'odyssey', name: 'Mars Odyssey (NASA)', bodyCommand: '499', observerCommand: '-53', bodyRadiusKm: 3389.5, launchDate: '2001-04-07' },
  { id: 'mex',     name: 'Mars Express (ESA)',  bodyCommand: '499', observerCommand: '-41', bodyRadiusKm: 3389.5, launchDate: '2003-06-02' },
  { id: 'hope',    name: 'Hope / EMM (UAE)',    bodyCommand: '499', observerCommand: '-62', bodyRadiusKm: 3389.5, launchDate: '2020-07-19' },
  // Lunas naturales de Marte — mismo radio de Marte para calcular su altitud real
  { id: 'phobos',  name: 'Phobos',              bodyCommand: '499', observerCommand: '401', bodyRadiusKm: 3389.5 },
  { id: 'deimos',  name: 'Deimos',              bodyCommand: '499', observerCommand: '402', bodyRadiusKm: 3389.5 },
];

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
  'Access-Control-Allow-Headers': 'Content-Type, Accept, Authorization',
  'Access-Control-Expose-Headers': 'X-TLE-Updated, X-Data-Stale, X-List-Updated',
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

// ════════════════════════════════════════════════════════
// FCM — Autenticación (sustituye a la sencilla clave de OneSignal)
// ════════════════════════════════════════════════════════

let fcmTokenCache = null; // guardamos el permiso mientras siga siendo válido, para no pedir uno nuevo en cada aviso

function base64UrlEncode(data) {
  let str;
  if (typeof data === 'string') {
    str = btoa(unescape(encodeURIComponent(data)));
  } else {
    str = btoa(String.fromCharCode(...new Uint8Array(data)));
  }
  return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function pemToArrayBuffer(pem) {
  const b64 = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\s/g, '');
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}

async function getFcmAccessToken(env) {
  const now = Math.floor(Date.now() / 1000);

  // Si ya tenemos un permiso vigente (con 1 minuto de margen de seguridad), lo reutilizamos
  if (fcmTokenCache && fcmTokenCache.expiresAt > now + 60) {
    return fcmTokenCache.token;
  }

  const serviceAccount = JSON.parse(env.FCM_SERVICE_ACCOUNT_JSON);

  const header = { alg: 'RS256', typ: 'JWT' };
  const claims = {
    iss:   serviceAccount.client_email,
    scope: 'https://www.googleapis.com/auth/firebase.messaging',
    aud:   serviceAccount.token_uri || 'https://oauth2.googleapis.com/token',
    exp:   now + 3600,
    iat:   now,
  };

  const unsignedJwt = `${base64UrlEncode(JSON.stringify(header))}.${base64UrlEncode(JSON.stringify(claims))}`;

  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8',
    pemToArrayBuffer(serviceAccount.private_key),
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    cryptoKey,
    new TextEncoder().encode(unsignedJwt)
  );

  const signedJwt = `${unsignedJwt}.${base64UrlEncode(signature)}`;

  const tokenRes = await fetch(serviceAccount.token_uri || 'https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    `grant_type=${encodeURIComponent('urn:ietf:params:oauth:grant-type:jwt-bearer')}&assertion=${signedJwt}`,
  });

  if (!tokenRes.ok) {
    const errText = await tokenRes.text();
    throw new Error(`FCM auth failed: ${tokenRes.status} ${errText}`);
  }

  const tokenData = await tokenRes.json();
  fcmTokenCache = { token: tokenData.access_token, expiresAt: now + tokenData.expires_in };
  return tokenData.access_token;
}

// ════════════════════════════════════════════════════════
// FCM — Envío real (sustituye a las dos funciones de OneSignal)
// ════════════════════════════════════════════════════════

async function sendFcmMessage(env, target, targetType, title, body, data = {}, ttlSeconds = 7200) {
  const serviceAccount = JSON.parse(env.FCM_SERVICE_ACCOUNT_JSON);
  const accessToken = await getFcmAccessToken(env);

  // targetType: 'token' (a un dispositivo concreto) o 'topic' (a todos los apuntados)
  const messageTarget = targetType === 'topic' ? { topic: target } : { token: target };

  // Los valores de "data" tienen que ser todos texto, FCM no acepta números ni objetos ahí dentro
  const dataAsStrings = {};
  for (const [k, v] of Object.entries(data)) dataAsStrings[k] = String(v);

  const payload = {
    message: {
      ...messageTarget,
      data: { title, body, ...dataAsStrings }, // todo como "data", nunca "notification" — así el clic siempre abre la URL correcta, esté la app como esté
      webpush: {
        notification: {
          icon: 'https://satfleetlive.com/images/logo.png',
          badge: 'https://satfleetlive.com/images/logo.png',
        },
        headers: { TTL: String(ttlSeconds) },
      },
      android: {
        priority: 'high',
        ttl: `${ttlSeconds}s`,
      },
      apns: {
        headers: { 'apns-expiration': String(Math.floor(Date.now() / 1000) + ttlSeconds) },
      },
    },
  };

  const res = await fetch(
    `https://fcm.googleapis.com/v1/projects/${serviceAccount.project_id}/messages:send`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${accessToken}`,
      },
      body: JSON.stringify(payload),
    }
  );

  if (!res.ok) {
    const errText = await res.text();
    console.error('FCM send error:', res.status, errText);
    throw new Error(`FCM send failed: ${res.status} ${errText}`);
  }

  return await res.json();
}

// ════════════════════════════════════════════════════════
// FCM — Apuntar un dispositivo a un tema (equivalente a "suscribirse")
// ════════════════════════════════════════════════════════

async function subscribeFcmTokenToTopic(env, token, topic) {
  const accessToken = await getFcmAccessToken(env);
  const serviceAccount = JSON.parse(env.FCM_SERVICE_ACCOUNT_JSON);

  const res = await fetch('https://iid.googleapis.com/iid/v1:batchAdd', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${accessToken}`,
      'access_token_auth': 'true',
    },
    body: JSON.stringify({
      to: `/topics/${topic}`,
      registration_tokens: [token],
    }),
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error('FCM subscribe error:', res.status, errText);
    throw new Error(`FCM subscribe failed: ${res.status} ${errText}`);
  }

  return await res.json();
}

async function handleFcmSubscribe(request, env) {
  try {
    const { token } = await request.json();
    if (!token) {
      return new Response(JSON.stringify({ ok: false, error: 'Falta el token' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
    }
    const result = await subscribeFcmTokenToTopic(env, token, 'todos_los_usuarios');
    return new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
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

  // "playerId" es el nombre histórico del campo — hoy contiene un token de
  // FCM, no un Player ID de OneSignal, pero lo dejamos igual para no tener
  // que tocar next-passes.html ni la app de Android para esto.
  const { playerId: token, satelliteName, passTimeIso, maxElevation, direction, brightness, cancel } = payload;

  if (!token || !passTimeIso || !satelliteName) {
    return new Response(JSON.stringify({ error: 'Missing fields' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const alertKey = `pass_alert_${token}_${passTimeIso}`;

  if (cancel) {
    try {
      await env.LAUNCHES_KV.delete(alertKey);
    } catch(e) {}
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

  // Deduplicación: si ya hay un aviso activo para este pase y este dispositivo,
  // no lo volvemos a programar por duplicado.
  try {
    const existing = await env.LAUNCHES_KV.get(alertKey);
    if (existing) {
      return new Response(JSON.stringify({ ok: true, scheduled: true, deduplicated: true }), {
        headers: makeHeaders({ 'Content-Type': 'application/json' }),
      });
    }
  } catch(e) {}

  // Marcamos el aviso como activo ANTES de programar nada — así, aunque el
  // primer mensaje de la cola se procese casi al instante, ya encuentra la
  // marca puesta.
  const ttlSeconds = Math.max(60, Math.ceil((passTime - now) / 1000) + 3600);
  try {
    await env.LAUNCHES_KV.put(alertKey, '1', { expirationTtl: ttlSeconds });
  } catch(e) {}

  const alerts = [
    { ms: 10 * 60 * 1000, label: '10 minutes' },
    { ms:  2 * 60 * 1000, label: '2 minutes'  },
  ];

  const title = `${satelliteName} passes soon!`;
  const body  = `Max ${maxElevation}° · ${direction}${magStr ? ' · ' + magStr : ''}`;
  // Al tocar el aviso, llevamos a la página de pases: ahí está la alerta
  // guardada, con su cuenta atrás y el botón para abrir el AR. Mandar a la
  // home obligaría al usuario a buscar por dónde entrar.
  const url   = 'https://satfleetlive.com/next-passes.html';

  for (const { ms, label } of alerts) {
    const fireAt = passTime - ms;
    if (fireAt <= now) continue; // ya pasó ese aviso concreto, nos lo saltamos

    const remainingMs = fireAt - now;
    const alertTitle = `${satelliteName} passes in ${label}!`;

    try {
      if (remainingMs <= 86_400_000) {
        // Cabe en un solo tramo — programamos el aviso final, preciso
        await env.PASS_ALERT_QUEUE.send(
          { type: 'fire', alertKey, token, title: alertTitle, body, url },
          { delaySeconds: Math.ceil(remainingMs / 1000) }
        );
      } else {
        // Falta más de 24h — el primer relevo de la posta
        await env.PASS_ALERT_QUEUE.send(
          { type: 'recheck', alertKey, token, title: alertTitle, body, fireAt, url },
          { delaySeconds: 86400 }
        );
      }
    } catch (err) {
      console.error('Error programando aviso en la cola:', err.message);
    }
  }

  return new Response(JSON.stringify({ ok: true, scheduled: true }), {
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
    const bulkRaw = await env.LAUNCHES_KV.get(KV_KEY_TLE);
    if (!bulkRaw) throw new Error('TLE data not available yet');
    const allSats = JSON.parse(bulkRaw);
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

// ═══════════════════════════════════════════════════════════════
// PREMIUM MIDDLEWARE — Firebase JWT + RevenueCat
// Solo protege /api/notify/pass. El resto sigue siendo público.
// ═══════════════════════════════════════════════════════════════

// Caché en memoria: { uid → { isPremium, expiresAt } }
// Dura mientras el isolate de Cloudflare esté vivo (~minutos)
const _premiumCache = new Map();
const PREMIUM_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutos

// Caché corta para /api/check-premium — protege contra picos de tráfico Y
// sirve de red de seguridad: si RevenueCat falla justo en ese momento, en
// vez de decir "no eres premium" a lo bruto, devolvemos la última respuesta
// buena que teníamos guardada de ese usuario.
const _checkPremiumCache = new Map();
const CHECK_PREMIUM_CACHE_TTL_MS = 3 * 60 * 1000; // 3 minutos

/**
 * Extrae el uid de Firebase de un JWT sin verificar la firma
 * (la verificación criptográfica completa no es necesaria aquí porque
 * RevenueCat confirma la suscripción de forma independiente).
 * Devuelve null si el token no existe o está malformado.
 */
function extractUidFromJWT(authHeader) {
    if (!authHeader || !authHeader.startsWith('Bearer ')) return null;
    const token = authHeader.slice(7).trim();
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    try {
        // Decodificar el payload (parte central) en base64url
        const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
        const padded  = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
        const decoded = atob(padded);
        const payload = JSON.parse(decoded);
        const uid = payload.user_id || payload.uid || payload.sub || null;
        // Rechazar tokens expirados (exp es Unix timestamp en segundos)
        if (payload.exp && Math.floor(Date.now() / 1000) > payload.exp) return null;
        return uid;
    } catch (e) {
        return null;
    }
}

/**
 * Consulta RevenueCat y devuelve true si el uid tiene el
 * entitlement "SatFleet Premium" activo.
 * Cachea el resultado 5 minutos para no abusar de la API.
 */
async function checkPremiumStatus(uid, env) {
    if (!uid) return false;

    // 1. Mirar caché en memoria
    const cached = _premiumCache.get(uid);
    if (cached && Date.now() < cached.expiresAt) {
        return cached.isPremium;
    }

    // 2. Consultar RevenueCat
    const apiKey = env.REVENUECAT_API_KEY;
    if (!apiKey) {
        console.warn('REVENUECAT_API_KEY not configured — blocking notify/pass for safety');
        return false;
    }

    try {
        const res = await fetch(
            `https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(uid)}`,
            {
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type':  'application/json',
                },
            }
        );

        if (!res.ok) {
            // 404 = usuario no encontrado en RC → Free
            // Otros errores → bloquear por precaución
            console.error('RevenueCat error:', res.status);
            _premiumCache.set(uid, { isPremium: false, expiresAt: Date.now() + PREMIUM_CACHE_TTL_MS });
            return false;
        }

        const data      = await res.json();
        const entitlements = data?.subscriber?.entitlements || {};
        const isPremium = Object.values(entitlements).some(ent => {
    if (!ent.expires_date) return true; 
    return new Date(ent.expires_date) > new Date();
});

        // 3. Guardar en caché
        _premiumCache.set(uid, { isPremium, expiresAt: Date.now() + PREMIUM_CACHE_TTL_MS });

        // Limpiar caché que ya expiró (para no acumular en memoria)
        if (_premiumCache.size > 500) {
            const now = Date.now();
            for (const [key, val] of _premiumCache) {
                if (now > val.expiresAt) _premiumCache.delete(key);
            }
        }

        return isPremium;

    } catch (e) {
        console.error('RevenueCat fetch error:', e.message);
        return false;
    }
}

/**
 * Respuesta 403 estándar para usuarios Free que intentan
 * usar un endpoint Premium.
 */
function premiumRequired() {
    return new Response(
        JSON.stringify({
            error:   'premium_required',
            message: 'This feature requires an active SatFleet Live Premium subscription.',
        }),
        {
            status: 403,
            headers: makeHeaders({ 'Content-Type': 'application/json' }),
        }
    );
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
// ARCHIVO HISTÓRICO DE TLE — una foto al día, guardada en formato clásico
// ═══════════════════════════════════════════════════════════════

const TLE_ARCHIVE_TTL_DAYS = 30;
const TLE_ARCHIVE_CRON     = '0 3 * * *'; // 03:00 UTC — una vez al día basta, es un archivo histórico
const DEEP_SPACE_WARM_CRON = '0 * * * *'; // cada hora — barato de comprobar (no hace nada si la caché sigue viva), y así nunca pasan más de ~1h de margen antes de que alguien real se encuentre la caché caducada

const ALPHA5_LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // NORAD "Alpha-5": se salta la I y la O
const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

function isLeapYear(y) { return (y % 4 === 0 && y % 100 !== 0) || (y % 400 === 0); }

function satnumStr(noradId) {
  if (noradId <= 99999) return String(noradId).padStart(5, '0');
  // A partir de 100000, el catálogo NORAD usa una letra en vez del primer dígito
  const thousands = Math.floor(noradId / 10000); // 10..33
  const remainder  = noradId % 10000;
  const letter = ALPHA5_LETTERS[thousands - 10];
  return letter + String(remainder).padStart(4, '0');
}

function parseIntldesg(objectId) {
  const m = /^(\d{4})-(\d{3})([A-Za-z]*)/.exec(objectId || '');
  if (!m) return ' '.repeat(8);
  const yy = m[1].slice(2);
  return (yy + m[2] + m[3]).padEnd(8, ' ');
}

// Calculado a mano, sin pasar por el objeto Date de JS: Date solo guarda
// milisegundos, y el EPOCH de CelesTrak trae microsegundos — usar Date aquí
// perdía precisión y desplazaba el último dígito (y con él, el checksum).
function epochToYyddd(epochIso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?/.exec(epochIso);
  const year = Number(m[1]), month = Number(m[2]), day = Number(m[3]);
  const hour = Number(m[4]), minute = Number(m[5]), second = Number(m[6]);
  const microsecond = Number((m[7] || '').padEnd(6, '0').slice(0, 6));

  let doy = day;
  for (let mo = 1; mo < month; mo++) {
    doy += DAYS_IN_MONTH[mo - 1];
    if (mo === 2 && isLeapYear(year)) doy += 1;
  }
  const fracOfDay = (hour * 3600 + minute * 60 + second + microsecond / 1e6) / 86400;
  const ddd = (doy + fracOfDay).toFixed(8).padStart(12, '0');
  return String(year % 100).padStart(2, '0') + ddd;
}

function fmtNdot(value) {
  const sign = value < 0 ? '-' : ' ';
  let s = sign + Math.abs(value).toFixed(8);
  const idx = s.indexOf('0');
  s = s.slice(0, idx) + s.slice(idx + 1);
  return s + ' ';
}

function toExpPython(value, decimals) {
  const s = value.toExponential(decimals);
  const m = /^(-?)(\d(?:\.\d+)?)e([+-])(\d+)$/.exec(s);
  const sign = m[1] ? '-' : ' ';
  return sign + m[2] + 'e' + m[3] + m[4].padStart(2, '0');
}

function abbreviateRate(value, zeroExponentString) {
  let s = toExpPython(value, 4) + ' ';
  s = s.replace('.', '');
  s = s.replace('e+00', zeroExponentString);
  s = s.replace('e-0', '-');
  s = s.replace('e+0', '+');
  return s;
}

function fmtDeg8_4f(value) {
  const s = Math.abs(value).toFixed(4);
  return (value < 0 ? '-' + s : s).padStart(8, ' ');
}

function computeTleChecksum(line) {
  let total = 0;
  for (const c of line.slice(0, 68)) {
    if (c >= '0' && c <= '9') total += Number(c);
    else if (c === '-') total += 1;
  }
  return total % 10;
}

function ommToTle(obj) {
  const satnum = satnumStr(obj.NORAD_CAT_ID);
  const classification = (obj.CLASSIFICATION_TYPE || 'U').trim() || 'U';
  const intldesg  = parseIntldesg(obj.OBJECT_ID);
  const epochField = epochToYyddd(obj.EPOCH);
  const ndotField  = fmtNdot(obj.MEAN_MOTION_DOT || 0);
  const nddotField = abbreviateRate((obj.MEAN_MOTION_DDOT || 0) * 10.0, '-0');
  const bstarField = abbreviateRate((obj.BSTAR || 0) * 10.0, '+0');
  const ephtype = obj.EPHEMERIS_TYPE ?? 0;
  const elnum   = String(obj.ELEMENT_SET_NO ?? 0).padStart(4, ' ');

  let line1 = `1 ${satnum}${classification} ${intldesg} ${epochField} ${ndotField}${nddotField}${bstarField}${ephtype} ${elnum}`;
  line1 += String(computeTleChecksum(line1));

  const eccField = obj.ECCENTRICITY.toFixed(7).replace('0.', '');
  const mmField  = obj.MEAN_MOTION.toFixed(8).padStart(11, ' ');
  const revField = String(obj.REV_AT_EPOCH ?? 0).padStart(5, ' ');

  let line2 = `2 ${satnum} ${fmtDeg8_4f(obj.INCLINATION)} ${fmtDeg8_4f(obj.RA_OF_ASC_NODE)} ${eccField} ${fmtDeg8_4f(obj.ARG_OF_PERICENTER)} ${fmtDeg8_4f(obj.MEAN_ANOMALY)} ${mmField}${revField}`;
  line2 += String(computeTleChecksum(line2));

  return { name: obj.OBJECT_NAME || 'UNKNOWN', line1, line2 };
}

async function archiveTleSnapshot(env) {
  let gpData;
  try {
    const raw = await env.LAUNCHES_KV.get(KV_KEY_TLE);
    if (!raw) throw new Error('KV sin datos TLE todavía');
    gpData = JSON.parse(raw);
  } catch (e) {
    console.error('TLE archive: no se pudo obtener CelesTrak, se reintenta mañana:', e.message);
    return;
  }

  // No archivar si los datos no se han renovado en las últimas 12 h:
  // la "foto" de hoy sería en realidad la de otro día repetida.
  let tleAgeH = Infinity;
  try {
    const meta = JSON.parse(await env.LAUNCHES_KV.get(KV_KEY_TLE_META) || '{}');
    tleAgeH = (Date.now() - new Date(meta.updated).getTime()) / 3600000;
  } catch (e) {}
  if (!(tleAgeH < 12)) {
    console.error(`TLE archive: los datos no se han renovado en ${Number.isFinite(tleAgeH) ? Math.round(tleAgeH) + ' h' : 'mucho tiempo'}, hoy no se archiva para no repetir un día.`);
    return;
  }

  // No archivar un día si lo que llegó no es una lista razonable: una foto
  // vacía quedaría 30 días en el calendario de Playback como un día "válido".
  if (!Array.isArray(gpData) || gpData.length < 5000) {
    console.error('TLE archive: tle.json no es una lista válida, hoy no se archiva.');
    return;
  }

  let text = '';
  let converted = 0;
  for (const obj of gpData) {
    try {
      const { name, line1, line2 } = ommToTle(obj);
      text += name + '\n' + line1 + '\n' + line2 + '\n';
      converted++;
    } catch (e) {
      continue;
    }
  }

  if (converted < 5000) {
    console.error(`TLE archive: solo se convirtieron ${converted} objetos, hoy no se archiva.`);
    return;
  }

  const dateKey = new Date().toISOString().slice(0, 10);
  await env.LAUNCHES_KV.put(`tle_archive_${dateKey}`, text, {
    expirationTtl: TLE_ARCHIVE_TTL_DAYS * 24 * 3600,
  });

  let index = [];
  try {
    const raw = await env.LAUNCHES_KV.get('tle_archive_index');
    if (raw) index = JSON.parse(raw);
  } catch (e) {}
  index = index.filter(d => d !== dateKey);
  index.push(dateKey);
  index = index.filter(d => (Date.now() - new Date(d + 'T00:00:00Z').getTime()) / 86400000 <= TLE_ARCHIVE_TTL_DAYS).sort();
  await env.LAUNCHES_KV.put('tle_archive_index', JSON.stringify(index), {
    expirationTtl: TLE_ARCHIVE_TTL_DAYS * 24 * 3600,
  });

  console.log(`TLE archive: guardado ${dateKey} — ${converted}/${gpData.length} objetos, ${text.length} bytes`);
}

async function handleTlePlayback(request, env) {
  const url = new URL(request.url);
  const dateParam = url.searchParams.get('date');

  const validFormat = typeof dateParam === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateParam);
  const realDate = validFormat && new Date(dateParam + 'T00:00:00Z').toISOString().slice(0, 10) === dateParam;
  const todayUtc = new Date().toISOString().slice(0, 10);
  if (!validFormat || !realDate || dateParam > todayUtc) {
    return new Response(JSON.stringify({ error: 'Parámetro ?date=YYYY-MM-DD inválido' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const text = await env.LAUNCHES_KV.get(`tle_archive_${dateParam}`);
  if (!text) {
    return new Response(JSON.stringify({ error: `No hay snapshot archivado para ${dateParam} — o aún no existía el archivo ese día, o ya caducaron sus 30 días` }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  return new Response(text, {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'public, max-age=86400' }),
  });
}

async function handleTleArchiveIndex(env) {
  const raw = await env.LAUNCHES_KV.get('tle_archive_index');
  return new Response(raw || '[]', {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'application/json' }),
  });
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/tle
// ═══════════════════════════════════════════════════════════════

async function handleTle(ctx, env) {
  return serveFromKv(ctx, env, {
    kvKey: KV_KEY_TLE, cacheKey: CACHE_KEY_TLE, ttl: TLE_TTL,
    label: 'TLE', updatedFromKey: KV_KEY_TLE_META,
  });
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
            notifPromises.push(sendFcmMessage(env, 'todos_los_usuarios', 'topic',
              '🔴 Live now',
              `${newL.name} is streaming live right now. Watch it on SatFleet!`,
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
            notifPromises.push(sendFcmMessage(env, 'todos_los_usuarios', 'topic',
              'Liftoff imminent',
              `${newL.name} launches in less than 5 minutes!`,
              { launchId: newL.id, type: 't_minus_5' },
              900 // 15 min — pasado eso, ya habrá despegado o no tiene sentido
            ));
          }
        }
        }

        if (
          newL.status?.abbrev === 'Go' &&
          oldL && oldL.status?.abbrev !== 'Go' &&
          new Date(newL.net).getTime() - nowMs > FIVE_MIN
        ) {
          notifPromises.push(sendFcmMessage(env, 'todos_los_usuarios', 'topic',
            '✅ Launch confirmed',
            `Mission ${newL.name} is GO for launch. Add it to your calendar!`,
            { launchId: newL.id, type: 'status_go' },
            172800 // 48h — sigue teniendo sentido aunque tardes un rato en verlo
          ));
        }

        if (
          !oldL &&
          newL.net &&
          new Date(newL.net).getTime() - nowMs < 48 * 3_600_000 &&
          new Date(newL.net).getTime() > nowMs
        ) {
          notifPromises.push(sendFcmMessage(env, 'todos_los_usuarios', 'topic',
            'New launch in 48 h',
            `${newL.name} just appeared on the schedule — launches within 48 hours!`,
            { launchId: newL.id, type: 'new_launch_soon' },
            172800 // 48h — coincide con lo que dice el propio texto del aviso
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
// HANDLER: /api/stripe/checkout — Crea sesión dinámica con metadata
// ═══════════════════════════════════════════════════════════════

async function handleStripeCheckout(request, env) {
  const uid = extractUidFromJWT(request.headers.get('Authorization'));
  if (!uid) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  let body;
  try { body = await request.json(); }
  catch(e) {
    return new Response(JSON.stringify({ error: 'Invalid JSON' }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const email = body.email || '';

  // Construimos el cuerpo a mano para que los corchetes lleguen
  // literales a Stripe (URLSearchParams los codificaría como %5B%5D
  // y RevenueCat no los leería bien en el webhook de suscripción)
  const enc = (v) => encodeURIComponent(String(v));
  const formBody = [
    `mode=subscription`,
    `line_items[0][price]=${enc(env.STRIPE_PRICE_ID)}`,
    `line_items[0][quantity]=1`,
    `success_url=${enc('https://satfleetlive.com/account.html?upgraded=1')}`,
    `cancel_url=${enc('https://satfleetlive.com/account.html')}`,
    `client_reference_id=${enc(uid)}`,
    `customer_email=${enc(email)}`,
    `metadata[app_user_id]=${enc(uid)}`,
    `subscription_data[metadata][app_user_id]=${enc(uid)}`,
    `subscription_data[trial_period_days]=7`,
    `consent_collection[terms_of_service]=required`,
    `custom_text[terms_of_service_acceptance][message]=${enc('I agree to the [Terms of Service](https://satfleetlive.com/terms-of-service) and ask for Premium access to start immediately, accepting the withdrawal conditions described there.')}`,
  ].join('&');

  try {
    const res = await fetch('https://api.stripe.com/v1/checkout/sessions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.STRIPE_SECRET_KEY}`,
        'Content-Type':  'application/x-www-form-urlencoded',
      },
      body: formBody,
    });

    if (!res.ok) {
      const err = await res.text();
      console.error('Stripe checkout error:', err);
      return new Response(JSON.stringify({ error: 'Stripe error', detail: err }), {
        status: 502,
        headers: makeHeaders({ 'Content-Type': 'application/json' }),
      });
    }

    const session = await res.json();
    return new Response(JSON.stringify({ url: session.url }), {
      status: 200,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });

  } catch(err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/moon-orbiters
// ═══════════════════════════════════════════════════════════════

const HORIZONS_MONTHS = { Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12' };
const AU_KM = 149597870.7; // 1 Unidad Astronómica en kilómetros

// Convierte el texto crudo de Horizons (tabla OBSERVER, "sub-observer point")
// en puntos {time, lat, lng, alt} — ESTE es el método que verificamos en vivo
// contra tres hechos reales independientes: la rotación de Marte, el periodo
// orbital del MRO, y su altitud real conocida. A diferencia del método viejo
// (VECTORS + cálculo manual), este directamente le pregunta a la NASA "¿qué
// punto de la superficie tiene la nave/el Sol justo encima ahora mismo?" —
// sin que tengamos que interpretar nosotros ningún sistema de referencia.
function parseSubObserverTable(resultText, bodyRadiusKm) {
  if (!resultText) return [];
  const soeIdx = resultText.indexOf('$$SOE');
  const eoeIdx = resultText.indexOf('$$EOE');
  if (soeIdx === -1 || eoeIdx === -1) return [];

  const lines = resultText.substring(soeIdx + 5, eoeIdx).split('\n').map(l => l.trim()).filter(Boolean);
  const points = [];

  for (const line of lines) {
    // ' 2026-Aug-30 00:00     347.614180  55.577067  0.00002465608612   0.0142241'
    const m = line.match(/^(\d{4})-(\w{3})-(\d{2})\s+(\d{2}):(\d{2})\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+(?:[eE][+-]?\d+)?)/);
    if (!m) continue;
    const [, year, monAbbr, day, hh, mm, westLngStr, latStr, deltaAuStr] = m;
    const month = HORIZONS_MONTHS[monAbbr];
    if (!month) continue;

    const time = `${year}-${month}-${day}T${hh}:${mm}:00Z`;
    const deltaKm = parseFloat(deltaAuStr) * AU_KM;
    const alt = deltaKm - bodyRadiusKm;

    // La tabla da longitud Oeste-positiva; el resto de nuestro código usa Este-positiva.
    let lng = -parseFloat(westLngStr);
    lng = ((lng + 180) % 360 + 360) % 360 - 180;

    points.push({ time, lat: parseFloat(latStr), lng, alt });
  }
  return points;
}

// target.bodyCommand = el cuerpo del que queremos un punto en su superficie (Marte=499, Luna=301...)
// target.observerCommand = quién "mira" ese cuerpo (una nave, el Sol, o una luna natural)
function buildSubObserverUrl(target, startTime, stopTime) {
  const q = [
    `format=json`,
    `COMMAND='${target.bodyCommand}'`,
    `CENTER='@${target.observerCommand}'`,
    `QUANTITIES='14,20'`,
    `OBJ_DATA='NO'`,
    `START_TIME='${startTime}'`,
    `STOP_TIME='${stopTime}'`,
    `STEP_SIZE='15%20m'`,
  ].join('&');
  return `${HORIZONS_BASE}?${q}`;
}

// El "motor" — calcula posiciones para CUALQUIER fecha que le des, sin
// decidir él mismo cuál usar. Eso lo deciden las dos funciones de abajo.
async function computeOrbiters(refDate, targets, previousOrbiters) {
  const startTime = refDate.toISOString().slice(0, 10);
  const stopTime  = new Date(refDate.getTime() + 48 * 3600 * 1000).toISOString().slice(0, 10);

  const orbiters = {};
  let anySuccess = false;
  let skippedCount = 0;

  for (const target of targets) {
    // Si la nave aún no se había lanzado en esa fecha, sabemos la respuesta sin
    // preguntar a Horizons (evita 3 intentos inútiles + esperas). Las lunas naturales no tienen launchDate.
    if (target.launchDate && refDate < new Date(target.launchDate + 'T00:00:00Z')) { skippedCount++; continue; }

    let points = null;
    let lastError;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const res = await fetch(buildSubObserverUrl(target, startTime, stopTime), { headers: { 'Accept': 'application/json' } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        const parsed = parseSubObserverTable(data.result, target.bodyRadiusKm);
        if (!parsed.length) throw new Error('Sin puntos');
        points = parsed;
        break;
      } catch (err) {
        lastError = err;
        if (attempt < 2) await new Promise(r => setTimeout(r, 300));
      }
    }

    if (points) {
      orbiters[target.id] = { name: target.name, points, lastFreshAt: Date.now() };
      anySuccess = true;
    } else {
      console.error(`Horizons fetch failed for ${target.id}:`, lastError?.message);
      if (previousOrbiters[target.id]) {
        orbiters[target.id] = previousOrbiters[target.id];
        anySuccess = true;
      }
    }
  }

  // Si TODOS estaban sin lanzar en esa fecha (p. ej. Luna en el año 2000), no es un error de la NASA:
  // devolvemos una respuesta válida y vacía en vez de un 502 engañoso.
  if (!anySuccess && skippedCount === targets.length) anySuccess = true;

  return { orbiters, anySuccess };
}

// Puerta de entrada 1: "ahora mismo" — la de siempre, sin cambios de comportamiento
async function handleOrbiters(ctx, env, targets, kvKey, ttl) {
  try {
    const cached = await env.LAUNCHES_KV.get(kvKey);
    if (cached) {
      const parsed = JSON.parse(cached);
      if (Date.now() - parsed.fetchedAt < ttl * 1000) {
        return new Response(JSON.stringify({ orbiters: parsed.orbiters, _meta: { source: 'kv_cache', fetchedAt: new Date(parsed.fetchedAt).toISOString() } }), {
          status: 200,
          headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache': 'HIT' }),
        });
      }
    }
  } catch(e) { console.error('Orbiters KV read error:', e.message); }

  let previousOrbiters = {};
  try {
    const prevCached = await env.LAUNCHES_KV.get(kvKey);
    if (prevCached) previousOrbiters = JSON.parse(prevCached).orbiters || {};
  } catch(e) {}

  const { orbiters, anySuccess } = await computeOrbiters(new Date(), targets, previousOrbiters);

  if (!anySuccess) {
    try {
      const stale = await env.LAUNCHES_KV.get(kvKey);
      if (stale) {
        const parsed = JSON.parse(stale);
        return new Response(JSON.stringify({ orbiters: parsed.orbiters, _meta: { source: 'stale_cache' } }), {
          status: 200,
          headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache': 'STALE' }),
        });
      }
    } catch(e) {}
    return new Response(JSON.stringify({ error: 'Horizons unreachable', orbiters: {} }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const payload = { orbiters, fetchedAt: Date.now() };
  ctx.waitUntil(env.LAUNCHES_KV.put(kvKey, JSON.stringify(payload), { expirationTtl: ttl + 3600 }));

  return new Response(JSON.stringify({ orbiters, _meta: { source: 'fresh_fetch', fetchedAt: new Date(payload.fetchedAt).toISOString() } }), {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache': 'MISS' }),
  });
}

// Puerta de entrada 2: NUEVA — una fecha del pasado, para el Playback.
// Mismo motor de arriba, pero cacheado 30 días (el pasado no cambia nunca).
async function handleOrbitersPlayback(request, ctx, env, targets, kvKeyPrefix) {
  const url = new URL(request.url);
  const dateParam = url.searchParams.get('date');

  const PLAYBACK_MIN_DATE = '1957-10-04'; // mismo límite que ya validamos en Deep Space
  const todayUtc = new Date().toISOString().slice(0, 10);
  const validFormat = typeof dateParam === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateParam);
  const realDate = validFormat && new Date(dateParam + 'T00:00:00Z').toISOString().slice(0, 10) === dateParam;
  if (!validFormat || !realDate || dateParam < PLAYBACK_MIN_DATE || dateParam > todayUtc) {
    return new Response(JSON.stringify({ error: `Parámetro ?date=YYYY-MM-DD inválido (rango permitido: ${PLAYBACK_MIN_DATE} a ${todayUtc})` }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const kvKey = `${kvKeyPrefix}_playback_${dateParam}`;
  try {
    const cached = await env.LAUNCHES_KV.get(kvKey);
    if (cached) {
      return new Response(JSON.stringify({ orbiters: JSON.parse(cached).orbiters, _meta: { source: 'kv_cache_playback' } }), {
        status: 200,
        headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=86400' }),
      });
    }
  } catch(e) { console.error('Orbiters playback KV read error:', e.message); }

  const refDate = new Date(dateParam + 'T00:00:00Z');
  const { orbiters, anySuccess } = await computeOrbiters(refDate, targets, {});

  if (!anySuccess) {
    return new Response(JSON.stringify({ error: 'Horizons unreachable para esa fecha', orbiters: {} }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  ctx.waitUntil(env.LAUNCHES_KV.put(kvKey, JSON.stringify({ orbiters }), { expirationTtl: 30 * 24 * 3600 }));

  return new Response(JSON.stringify({ orbiters, _meta: { source: 'fresh_fetch_playback', date: dateParam } }), {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=86400' }),
  });
}

async function handleMoonOrbiters(ctx, env) {
  return handleOrbiters(ctx, env, MOON_ORBITER_TARGETS, KV_KEY_MOON_ORBITERS, MOON_ORBITERS_TTL);
}

async function handleMoonOrbitersPlayback(request, ctx, env) {
  return handleOrbitersPlayback(request, ctx, env, MOON_ORBITER_TARGETS, 'moon_orbiters');
}

async function handleMarsOrbiters(ctx, env) {
  return handleOrbiters(ctx, env, MARS_ORBITER_TARGETS, KV_KEY_MARS_ORBITERS, MARS_ORBITERS_TTL);
}

async function handleMarsOrbitersPlayback(request, ctx, env) {
  return handleOrbitersPlayback(request, ctx, env, MARS_ORBITER_TARGETS, 'mars_orbiters');
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/rover-trail/:id — rastro real de los rovers de Marte
// ═══════════════════════════════════════════════════════════════
// Los rastros los genera el cron del propio Worker (fetchRoverTrailsToKv) y
// se guardan en KV; aquí solo se leen, con caché de Cloudflare por delante.

const ROVER_TRAIL_TTL  = 1800;
const ROVER_IDS        = ['curiosity', 'perseverance'];

async function handleRoverTrail(request, ctx, env) {
  const roverId = new URL(request.url).pathname.split('/').pop();
  if (!ROVER_IDS.includes(roverId)) {
    return new Response(JSON.stringify({ error: 'Unknown rover' }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }
  return serveFromKv(ctx, env, {
    kvKey: KV_KEY_ROVER_PREFIX + roverId,
    cacheKey: `https://internal.satfleetlive/cache/rover-trail-v2-${roverId}`,
    ttl: ROVER_TRAIL_TTL, label: 'Rover trail',
  });
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/deep-space — naves más allá de la órbita terrestre
// ═══════════════════════════════════════════════════════════════
// Aquí SÍ usamos vectores X/Y/Z directos respecto al Sol — a diferencia de
// los orbitadores de Marte/Luna, aquí no hay ninguna superficie que gire de
// por medio, así que el método "viejo" (el que abandonamos para planetas)
// es aquí la herramienta correcta, verificado en vivo con el Voyager 1.

const KV_KEY_DEEP_SPACE = 'deep_space_v1';
const DEEP_SPACE_TTL = 6 * 3600;

const DEEP_SPACE_TARGETS = [
  // Planetas — posición real y en vivo, no decorativa (mismo bucle, casi gratis)
  { id: 'mercury', name: 'Mercury', command: '199', isPlanet: true },
  { id: 'venus',   name: 'Venus',   command: '299', isPlanet: true },
  { id: 'earth',   name: 'Earth',   command: '399', isPlanet: true },
  { id: 'mars',    name: 'Mars',    command: '499', isPlanet: true },
  { id: 'jupiter', name: 'Jupiter', command: '599', isPlanet: true },
  { id: 'saturn',  name: 'Saturn',  command: '699', isPlanet: true },
  { id: 'uranus',  name: 'Uranus',  command: '799', isPlanet: true },
  { id: 'neptune', name: 'Neptune', command: '899', isPlanet: true },
  { id: 'pluto', name: 'Pluto', command: '999', isPlanet: true },
  { id: 'ceres', name: 'Ceres', command: '2000001', isPlanet: true },
  { id: 'moon', name: 'Moon', command: '301', isPlanet: true },
  { id: 'phobos', name: 'Phobos', command: '401', isPlanet: true, fastOrbit: true },
  { id: 'deimos', name: 'Deimos', command: '402', isPlanet: true, fastOrbit: true },
  { id: 'io', name: 'Io', command: '501', isPlanet: true, fastOrbit: true },
  { id: 'europa', name: 'Europa', command: '502', isPlanet: true, fastOrbit: true },
  { id: 'ganymede', name: 'Ganymede', command: '503', isPlanet: true, fastOrbit: true },
  { id: 'callisto', name: 'Callisto', command: '504', isPlanet: true, fastOrbit: true },
  { id: 'titan', name: 'Titan', command: '606', isPlanet: true },
  { id: 'enceladus', name: 'Enceladus', command: '602', isPlanet: true },
  { id: 'triton', name: 'Triton', command: '801', isPlanet: true },
  { id: 'charon', name: 'Charon', command: '901', isPlanet: true },
  { id: 'eris', name: 'Eris', command: '136199', isPlanet: true },
  { id: 'haumea', name: 'Haumea', command: '136108', isPlanet: true },
  { id: 'makemake', name: 'Makemake', command: '136472', isPlanet: true },
  { id: 'sedna', name: 'Sedna', command: '90377', isPlanet: true },
  { id: 'quaoar', name: 'Quaoar', command: '50000', isPlanet: true },
  { id: 'gonggong', name: 'Gonggong', command: '225088', isPlanet: true },
  { id: 'orcus', name: 'Orcus', command: '90482', isPlanet: true },
  // Naves de espacio profundo — launchDate real de cada una, para el Playback:
  // así nunca preguntamos a Horizons por una nave que todavía no existía.

  //Tripuladas:
  { id: 'artemis2', name: 'Artemis II', command: '-1024' },
  { id: 'snoopy', name: 'Snoopy (Apollo 10 LM)', command: '-399101' },

  //Curiosidades:
  { id: 'apollo11s4b', name: 'Apollo 11 (S-IVB stage)', command: '-399110' },
  { id: 'teslaroadster', name: 'Tesla Roadster (Starman)', command: '-143205' },

  //Naves Espaciales
  { id: 'artemis1', name: 'Artemis I', command: '-1023' },
  { id: 'voyager1', name: 'Voyager 1', command: '-31', launchDate: '1977-09-05' },
  { id: 'voyager2', name: 'Voyager 2', command: '-32', launchDate: '1977-08-20' },
  { id: 'jwst',      name: 'James Webb Space Telescope', command: '-170', launchDate: '2021-12-25' },
  { id: 'parker',    name: 'Parker Solar Probe', command: '-96', launchDate: '2018-08-12' },
  { id: 'newhorizons', name: 'New Horizons', command: '-98', launchDate: '2006-01-19' },
  { id: 'juno',      name: 'Juno', command: '-61', launchDate: '2011-08-05' },
  { id: 'hera', name: 'Hera', command: '-91', launchDate: '2024-10-07' },
  { id: 'bepicolombo', name: 'BepiColombo', command: '-121', launchDate: '2018-10-20' },
  { id: 'europaclipper', name: 'Europa Clipper', command: '-159', launchDate: '2024-10-14' },
  { id: 'lucy', name: 'Lucy', command: '-49', launchDate: '2021-10-16' },
  { id: 'psyche', name: 'Psyche', command: '-255', launchDate: '2023-10-13' },
  { id: 'romantelescope', name: 'Nancy Grace Roman Space Telescope', command: '-211', launchDate: '2026-08-30' },
  { id: 'solarorbiter', name: 'Solar Orbiter', command: '-144', launchDate: '2020-02-10' },
  { id: 'osirisapex', name: 'OSIRIS-APEX', command: '-64', launchDate: '2016-09-08' },
];
function parseHeliocentricVectors(resultText) {
  if (!resultText) return [];
  const soeIdx = resultText.indexOf('$$SOE');
  const eoeIdx = resultText.indexOf('$$EOE');
  if (soeIdx === -1 || eoeIdx === -1) return [];

  const lines = resultText.substring(soeIdx + 5, eoeIdx).split('\n').map(l => l.trim()).filter(Boolean);
  const points = [];

  for (let i = 0; i < lines.length; i += 2) {
    const dateLine = lines[i];
    const xyzLine  = lines[i + 1];
    if (!dateLine || !xyzLine) continue;

    const dateMatch = dateLine.match(/A\.D\.\s+(\d{4})-(\w{3})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
    const xMatch = xyzLine.match(/X\s*=\s*(-?\d+\.\d+E[+-]\d+)/);
    const yMatch = xyzLine.match(/Y\s*=\s*(-?\d+\.\d+E[+-]\d+)/);
    const zMatch = xyzLine.match(/Z\s*=\s*(-?\d+\.\d+E[+-]\d+)/);
    if (!dateMatch || !xMatch || !yMatch || !zMatch) continue;

    const [, year, monAbbr, day, hh, mm, ss] = dateMatch;
    const month = HORIZONS_MONTHS[monAbbr];
    if (!month) continue;

    points.push({
      time: `${year}-${month}-${day}T${hh}:${mm}:${ss}Z`,
      x: parseFloat(xMatch[1]),
      y: parseFloat(yMatch[1]),
      z: parseFloat(zMatch[1]),
    });
  }
  return points;
}

function buildHeliocentricUrl(target, startTime, stopTime, stepSize) {
  const q = [
    `format=json`,
    `COMMAND='${target.command}'`,
    `OBJ_DATA='NO'`,
    `MAKE_EPHEM='YES'`,
    `EPHEM_TYPE='VECTORS'`,
    `CENTER='500@10'`,
    `REF_PLANE='ECLIPTIC'`,
    `START_TIME='${startTime}'`,
    `STOP_TIME='${stopTime}'`,
    `STEP_SIZE='${stepSize}'`,
    `VEC_TABLE='1'`,
    `OUT_UNITS='KM-S'`,
  ].join('&');
  return `${HORIZONS_BASE}?${q}`;
}

async function computeDeepSpaceObjects(refDate, previousObjects, env, isLive = true) {
  const now = refDate; // "now" en el sentido de "el instante alrededor del cual pedimos la ventana" — puede ser hoy, o una fecha del pasado para el Playback
  const startTime = now.toISOString().slice(0, 10);
  const objects = {};
  let anySuccess = false;

  // Cuando la NASA dice "no hay datos después de tal fecha", esa fecha
  // exacta viene en su propio mensaje de error — la leemos y volvemos a
  // preguntar justo con ese límite, en vez de adivinar con escalones fijos
  // que podrían dejar días reales sin pedir.
  const MESES_HORIZONS = { JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUN:'06',JUL:'07',AUG:'08',SEP:'09',OCT:'10',NOV:'11',DEC:'12' };

  // Horizons usa dos frases distintas según en qué extremo falten datos:
  // "after A.D. X" → los datos se acaban antes de lo pedido (hay que acortar el FINAL)
  // "prior to A.D. X" → los datos empiezan después de lo pedido (hay que adelantar el INICIO)
  // Antes solo sabíamos leer la primera — por eso Voyager 1, pedido desde
  // antes de su lanzamiento, se descartaba entero en vez de ajustar el inicio.
  function extractEphemerisBoundary(errMsg) {
    const msg = errMsg || '';
    const m = /(after|prior\s+to)\s+A\.D\.\s+(\d{4})-(\w{3})-(\d{2})(?:\s+(\d{2}):(\d{2}))?/i.exec(msg);
    if (!m) return null;
    const mm = MESES_HORIZONS[m[3].toUpperCase()];
    if (!mm) return null;
    const type = /^after/i.test(m[1]) ? 'after' : 'prior';
    // Ahora SÍ leemos la hora exacta que trae el mensaje (ej. "02:05"), y
    // dejamos solo 10 minutos de margen en vez de un día entero. Así no
    // perdemos el primer día de una misión corta (ej. Artemis II). Si algún
    // mensaje no trae hora, hacemos lo de siempre.
    const conHora = m[5] !== undefined;
    const base = new Date(`${m[2]}-${mm}-${m[4]}T${conHora ? m[5] + ':' + m[6] : '00:00'}:00Z`);
    const margenMs = conHora ? 10 * 60 * 1000 : (type === 'after' ? 0 : 24 * 3600 * 1000);
    const segura = new Date(base.getTime() + (type === 'after' ? -margenMs : margenMs)).toISOString();
    const date = conHora ? `${segura.slice(0, 10)}%20${segura.slice(11, 16)}` : segura.slice(0, 10);
    return { type, date };
  }

  async function intentarUnaVez(target, stopTime, stepSize, inicioPersonalizado) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await fetch(buildHeliocentricUrl(target, inicioPersonalizado || startTime, stopTime, stepSize), { headers: { 'Accept': 'application/json' }, signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      const points = parseHeliocentricVectors(data.result);
      if (!points.length) throw new Error('Sin puntos');
      return points;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  async function fetchVentanaConLimiteReal(target, dias, stepSize) {
    let inicio = startTime;
    let fin = new Date(now.getTime() + dias * 24 * 3600 * 1000).toISOString().slice(0, 10);
    const comparable = s => s.replace('%20', 'T');
    let ajustes = 0;
    let lastErr;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        return await intentarUnaVez(target, fin, stepSize, inicio);
      } catch (err) {
        lastErr = err;
        const boundary = extractEphemerisBoundary(err.message);
        // Una nave puede tener los DOS extremos fuera de la ventana a la vez
        // (ej. Artemis II: sus datos empiezan horas después del lanzamiento y
        // acaban el día del amerizaje). Corregimos un extremo, volvemos a
        // preguntar, y si falta el otro la NASA nos lo dice y lo corregimos.
        if (boundary && ajustes < 2) {
          if (boundary.type === 'after') {
            // Si sus datos acabaron antes incluso de nuestro inicio, no hay
            // nada que pedir en esta ventana (misión ya terminada).
            if (comparable(boundary.date) <= comparable(inicio)) throw err;
            fin = boundary.date;
          } else {
            if (comparable(boundary.date) >= comparable(fin)) throw err;
            inicio = boundary.date;
          }
          ajustes++;
          attempt--; // corregir la ventana no cuenta como un reintento por fallo
          continue;
        }
        if (/no ephemeris/i.test(err.message || '')) throw err;
        if (attempt < 2) await new Promise(r => setTimeout(r, 300 * (attempt + 1) * (attempt + 1)));
      }
    }
    throw lastErr;
  }

  // Escalones de detalle, de más ancho a más fino. Si algo lleva la etiqueta
  // manual (fastOrbit/ultraFastOrbit — Fobos, las lunas de Júpiter...),
  // empezamos directamente en ese escalón, porque ya sabemos que lo necesita.
  // Para todo lo demás (incluida una futura Starship), empezamos ancho y
  // solo bajamos si el propio movimiento real nos dice que hace falta.
  const STEP_TIERS = [
    { step: '12%20h', ventanas: [30] },  // objetos normales — SIEMPRE el rango ancho completo
    { step: '3%20h',  ventanas: [30] },  // si a 12h se ve "brusco", afinamos la resolución, sin recortar el rango
    { step: '30%20m', ventanas: [30] },  // fastOrbit — mismo rango de 30 días, solo con paso más fino
    { step: '1%20m',  ventanas: [1] },   // ultraFastOrbit — aquí sí se mantiene corto, sería demasiado dato a este detalle
  ];

  // Mide si el salto más grande entre dos fotos consecutivas es una porción
  // demasiado grande de todo el recorrido — vale igual para una luna que
  // gira en horas que para una nave que cruza millones de km, porque se
  // mide en proporción, no en kilómetros fijos.
  function saltoDemasiadoGrande(points) {
    if (points.length < 2) return false;
    let total = 0, maxSalto = 0;
    for (let i = 0; i < points.length - 1; i++) {
      const dx = points[i+1].x - points[i].x, dy = points[i+1].y - points[i].y, dz = points[i+1].z - points[i].z;
      const d = Math.sqrt(dx*dx + dy*dy + dz*dz);
      total += d;
      if (d > maxSalto) maxSalto = d;
    }
    return total > 0 && (maxSalto / total) > 0.15; // ningún salto debería superar el 15% del recorrido total
  }

  async function fetchConDeteccionAutomatica(target) {
    let tierIdx = target.ultraFastOrbit ? 3 : (target.fastOrbit ? 2 : 0);
    let lastError = new Error('No se pudo obtener ningún dato');
    let segundaOportunidadUsada = false;
    while (tierIdx < STEP_TIERS.length) {
      const tier = STEP_TIERS[tierIdx];
      let necesitaAfinar = false;
      for (const dias of tier.ventanas) {
        try {
          const points = await fetchVentanaConLimiteReal(target, dias, tier.step);
          if (tierIdx === STEP_TIERS.length - 1 || !saltoDemasiadoGrande(points)) {
            return points; // esto ya vale, terminamos
          }
          necesitaAfinar = true;
          break; // hay datos, pero demasiado bastos — probamos el siguiente escalón
        } catch (err) {
          lastError = err;
          // "No ephemeris" = la nave no existía en esa fecha: probar otro
          // escalón no ayuda, es el único caso donde sí nos rendimos ya.
          if (/no ephemeris/i.test(err.message || '')) throw err;
          await new Promise(r => setTimeout(r, 300));
        }
      }
      // Si fue un fallo real (un 503 puntual, por ejemplo), primero
      // repetimos el MISMO escalón tras una pausa más larga.
      if (!necesitaAfinar && !segundaOportunidadUsada) {
        segundaOportunidadUsada = true;
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }
      // El último escalón (paso de 1 minuto) solo cubre 1 DÍA, no 30. Si
      // bajáramos a él por un fallo, el objeto se quedaría congelado tras
      // ese día en el Playback. Por un fallo, nunca bajamos a él.
      if (!necesitaAfinar && tierIdx + 1 === STEP_TIERS.length - 1) break;
      tierIdx++;
    }
    throw lastError;
  }

  // Ni todo de golpe (satura a la NASA, falla uno aleatorio cada vez) ni
  // uno a uno (demasiado lento) — grupos pequeños, uno detrás de otro.
  const BATCH_SIZE = 6;
  for (let i = 0; i < DEEP_SPACE_TARGETS.length; i += BATCH_SIZE) {
    const batch = DEEP_SPACE_TARGETS.slice(i, i + BATCH_SIZE);
    const results = await Promise.allSettled(batch.map(async (target) => {
      // Ni preguntamos: si la fecha pedida es anterior a su lanzamiento real,
      // sabemos la respuesta sin gastar ninguna llamada a Horizons.
      // Solo descartamos sin preguntar si el lanzamiento cae CLARAMENTE fuera
      // de cualquier ventana que vayamos a intentar (30 días es la más ancha
      // que probamos) — si cae dentro, lo intentamos: Horizons y nuestro
      // propio sistema de reintentos ya saben ajustarse a la fecha real.
      const maxVentanaMs = 30 * 24 * 3600 * 1000;
      if (target.launchDate && new Date(target.launchDate + 'T00:00:00Z').getTime() > now.getTime() + maxVentanaMs) {
        return { target, points: [], skipped: true };
      }

      // Red de seguridad automática: si ya detectamos antes que esto terminó
      // (la propia NASA nos lo dijo), no volvemos a preguntar hasta que
      // caduque el aviso — solo aplica en directo, nunca en Playback.
      if (isLive && env) {
        try {
          const yaTerminada = await env.LAUNCHES_KV.get(`mission_ended_${target.id}`);
          if (yaTerminada) return { target, points: [], skipped: true };
        } catch (e) { /* si el KV falla al leer, seguimos e intentamos con normalidad */ }
      }

      try {
        const points = await fetchConDeteccionAutomatica(target);
        return { target, points };
      } catch (err) {
        // Si la propia NASA nos dice explícitamente "no hay datos después de
        // tal fecha", y estamos en directo, lo apuntamos para no volver a
        // preguntar — con caducidad, por si algún día publican más datos.
        if (isLive && env) {
          const boundary = extractEphemerisBoundary(err.message);
          if (boundary?.type === 'after') {
            try {
              await env.LAUNCHES_KV.put(`mission_ended_${target.id}`, boundary.date, { expirationTtl: 75 * 24 * 3600 });
            } catch (e) { /* si falla el guardado, no pasa nada — se reintentará la próxima vez */ }
            return { target, points: [], skipped: true };
          }
        }
        throw err;
      }
    }));

    results.forEach((result, j) => {
      const target = batch[j];
      if (result.status === 'fulfilled' && result.value.skipped) return; // nave aún no lanzada en esa fecha: no es un fallo, no lo registramos como tal
      if (result.status === 'fulfilled' && result.value.points.length) {
        objects[target.id] = { name: target.name, isPlanet: !!target.isPlanet, points: result.value.points, lastFreshAt: Date.now() };
        anySuccess = true;
      } else {
        const motivo = result.status === 'rejected' ? result.reason?.message : 'sin puntos devueltos';
        console.error(`Horizons fetch failed for ${target.id}:`, motivo);
        // Si falló hoy pero teníamos algo bueno de la última vez, mejor eso
        // que dejarlo desaparecido del todo — aunque sea de hace unas horas.
        if (previousObjects[target.id]) {
          objects[target.id] = previousObjects[target.id];
          anySuccess = true;
        }
      }
    });
  }

  return { objects, anySuccess };
}

async function handleDeepSpace(ctx, env, fromCron = false) {
  let previousObjects = {};
  try {
    const cached = await env.LAUNCHES_KV.get(KV_KEY_DEEP_SPACE);
    if (cached) {
      const parsed = JSON.parse(cached);
      previousObjects = parsed.objects || {};
      const ageMs = Date.now() - parsed.fetchedAt;
      // Visitante real: siempre recibe lo guardado, al instante, aunque tenga horas.
      // Solo el cron (fromCron) decide refrescar, cuando pasan 5 horas.
      if (!fromCron || ageMs < (DEEP_SPACE_TTL - 3600) * 1000) {
        return new Response(JSON.stringify({ objects: parsed.objects, _meta: { source: 'kv_cache' } }), {
          status: 200,
          headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'X-Cache-Age-Minutes': String(Math.round(ageMs / 60000)) }),
        });
      }
    }
  } catch(e) { console.error('Deep space KV read error:', e.message); }

  const { objects, anySuccess } = await computeDeepSpaceObjects(new Date(), previousObjects, env, true);

  if (!anySuccess) {
    try {
      const stale = await env.LAUNCHES_KV.get(KV_KEY_DEEP_SPACE);
      if (stale) {
        const parsed = JSON.parse(stale);
        return new Response(JSON.stringify({ objects: parsed.objects, _meta: { source: 'stale_cache' } }), {
          status: 200,
          headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8' }),
        });
      }
    } catch(e) {}
    return new Response(JSON.stringify({ error: 'Horizons unreachable', objects: {} }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const payload = { objects, fetchedAt: Date.now() };
  ctx.waitUntil(env.LAUNCHES_KV.put(KV_KEY_DEEP_SPACE, JSON.stringify(payload), { expirationTtl: 7 * 24 * 3600 }));

  return new Response(JSON.stringify({ objects, _meta: { source: 'fresh_fetch' } }), {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8' }),
  });
}

// ════════ PLAYBACK — mismo motor, apuntando a una fecha del pasado ════════
// El pasado no cambia nunca, así que cacheamos 30 días — evita volver a
// preguntarle a Horizons por una fecha que ya trajimos antes.
async function handleDeepSpacePlayback(request, ctx, env) {
  const url = new URL(request.url);
  const dateParam = url.searchParams.get('date'); // formato exacto: 2025-03-15

  // Solo aceptamos una fecha real, con formato exacto, entre el inicio de la era espacial y hoy.
  // Sin esto, cualquiera podía pedir "0001-01-01" o formatos raros (que además hacían fallar el Worker).
  const PLAYBACK_MIN_DATE = '1957-10-04'; // Sputnik 1 — probado en vivo: fechas muy anteriores no responden bien
  const todayUtc = new Date().toISOString().slice(0, 10);
  const validFormat = typeof dateParam === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateParam);
  const realDate = validFormat && new Date(dateParam + 'T00:00:00Z').toISOString().slice(0, 10) === dateParam; // rechaza 2025-02-30
  if (!validFormat || !realDate || dateParam < PLAYBACK_MIN_DATE || dateParam > todayUtc) {
    return new Response(JSON.stringify({ error: `Parámetro ?date=YYYY-MM-DD inválido (rango permitido: ${PLAYBACK_MIN_DATE} a ${todayUtc})` }), {
      status: 400,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const kvKey = `deep_space_playback_v3_${dateParam}`;
  try {
    const cached = await env.LAUNCHES_KV.get(kvKey);
    if (cached) {
      return new Response(JSON.stringify({ objects: JSON.parse(cached).objects, _meta: { source: 'kv_cache_playback' } }), {
        status: 200,
        headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=86400' }),
      });
    }
  } catch(e) { console.error('Playback KV read error:', e.message); }

  const refDate = new Date(dateParam + 'T00:00:00Z'); // los datos devueltos cubren ese día y los ~30 siguientes (no van centrados en la fecha)
  const { objects, anySuccess } = await computeDeepSpaceObjects(refDate, {}, env, false); // sin red de seguridad de "lo último bueno" — no aplica a fechas concretas del pasado

  if (!anySuccess) {
    return new Response(JSON.stringify({ error: 'Horizons unreachable para esa fecha', objects: {} }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  ctx.waitUntil(env.LAUNCHES_KV.put(kvKey, JSON.stringify({ objects }), { expirationTtl: 30 * 24 * 3600 }));

  return new Response(JSON.stringify({ objects, _meta: { source: 'fresh_fetch_playback', date: dateParam } }), {
    status: 200,
    headers: makeHeaders({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=86400' }),
  });
}

// ═══════════════════════════════════════════════════════════════
// ROUTER PRINCIPAL
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/data/:id — basura espacial y reentradas (desde KV)
// ═══════════════════════════════════════════════════════════════

const DATA_FILES = {
  debris:    { kvKey: KV_KEY_DEBRIS,    ttl: 3600, label: 'Debris' },    // el KV se refresca cada 12 h
  reentries: { kvKey: KV_KEY_REENTRIES, ttl:  600, label: 'Reentries' }, // el KV se refresca cada 6 h; una predicción cercana importa
};

async function handleDataFile(request, ctx, env) {
  const id  = new URL(request.url).pathname.split('/').pop();
  const cfg = DATA_FILES[id];
  if (!cfg) {
    return new Response(JSON.stringify({ error: 'Unknown dataset' }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }
  return serveFromKv(ctx, env, {
    kvKey: cfg.kvKey,
    cacheKey: `https://internal.satfleetlive/cache/data-v2-${id}`,
    ttl: cfg.ttl, label: cfg.label,
  });
}

// Lector genérico KV → Response, con caché de Cloudflare por delante para no
// gastar una lectura de KV (y hasta ~10 MB) por cada visitante.
async function serveFromKv(ctx, env, { kvKey, cacheKey, ttl, label, updatedFromKey = null }) {
  const cache = caches.default;
  const hit = await cache.match(cacheKey);
  if (hit) return wrapCached(hit);

  let body, updated = null, listUpdated = null;
  try {
    body = await env.LAUNCHES_KV.get(kvKey, { type: 'stream' });
    if (body && updatedFromKey) {
      try { const m = JSON.parse(await env.LAUNCHES_KV.get(updatedFromKey)); updated = m.updated || null; listUpdated = m.listUpdated || null; } catch (e) {}
    }
  } catch (err) {
    return new Response(JSON.stringify({ error: `${label}: KV error — ${err.message}` }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }),
    });
  }
  if (!body) {
    // Aún no se ha ejecutado el cron por primera vez (o nunca tuvo éxito). No se cachea.
    return new Response(JSON.stringify({ error: `${label}: datos aún no disponibles` }), {
      status: 503,
      headers: makeHeaders({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Retry-After': '300' }),
    });
  }

  const resp = new Response(body, {
    status: 200,
    headers: makeHeaders({
      'Content-Type':  'application/json; charset=utf-8',
      'Cache-Control': `s-maxage=${ttl}, max-age=${ttl}`,
      ...(updated ? { 'X-TLE-Updated': updated } : {}),
      ...(listUpdated ? { 'X-List-Updated': listUpdated } : {}),
    }),
  });
  ctx.waitUntil(cache.put(cacheKey, resp.clone()));
  return resp;
}

// ═══════════════════════════════════════════════════════════════
// COLECTORES (CRON) — sustituyen a los workflows de GitHub Actions
// ═══════════════════════════════════════════════════════════════
// Regla común: se descarga, se valida y SOLO entonces se hace put() en KV.
// Cualquier error/timeout/respuesta rara → se registra y se conserva el
// último dato válido que ya hubiera en KV.

const COLLECTOR_UA = 'SatFleetLive/3.0 (https://satfleetlive.com; contact: jaime.automatiza@gmail.com)';
const KV_MAX_BYTES = 24 * 1024 * 1024; // KV admite 25 MiB por valor; dejamos margen
const MIN_OBJECTS  = 5000;             // red de seguridad contra respuestas truncadas

async function putKvChecked(env, key, text) {
  const bytes = new TextEncoder().encode(text).length;
  if (bytes > KV_MAX_BYTES) throw new Error(`el valor pesa ${(bytes / 1048576).toFixed(1)} MB, cerca del límite de KV (25 MiB)`);
  await env.LAUNCHES_KV.put(key, text);
  return bytes;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── 1. TLE de satélites activos ─────────────────────────────────
// Las ÓRBITAS vienen de Space-Track cada 2 h (CelesTrak corta las conexiones
// que salen de Cloudflare). La LISTA de qué satélites están activos es la de
// CelesTrak, pero la descarga tu repositorio de GitHub y el Worker la lee de ahí.
const KV_KEY_ACTIVE_IDS     = 'tle_active_ids_v1';
const KV_KEY_ACTIVE_ATTEMPT = 'tle_active_ids_attempt_v1';
const ACTIVE_LIST_EVERY_MS  = 6 * 3600 * 1000;   // cada 6 h mira si hay lista nueva en GitHub
// Archivo que publica tu repositorio de GitHub. Si cambias el nombre del repo, cambia también esta línea.
const ACTIVE_IDS_URL = 'https://raw.githubusercontent.com/jaimeautomatiza/satfleet-active-list/data/active-ids.json';
const RECENT_LAUNCH_DAYS    = 30;                // los recién lanzados entran aunque no estén en la lista
const TLE_FIELDS = 'OBJECT_NAME,OBJECT_ID,EPOCH,MEAN_MOTION,ECCENTRICITY,INCLINATION,RA_OF_ASC_NODE,ARG_OF_PERICENTER,MEAN_ANOMALY,EPHEMERIS_TYPE,CLASSIFICATION_TYPE,NORAD_CAT_ID,ELEMENT_SET_NO,REV_AT_EPOCH,BSTAR,MEAN_MOTION_DOT,MEAN_MOTION_DDOT,LAUNCH_DATE';
const TLE_INT_FIELDS   = ['NORAD_CAT_ID', 'EPHEMERIS_TYPE', 'ELEMENT_SET_NO', 'REV_AT_EPOCH'];
const TLE_FLOAT_FIELDS = ['MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 'RA_OF_ASC_NODE', 'ARG_OF_PERICENTER', 'MEAN_ANOMALY', 'BSTAR', 'MEAN_MOTION_DOT', 'MEAN_MOTION_DDOT'];
const TLE_REQUIRED     = ['NORAD_CAT_ID', 'EPOCH', 'MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 'RA_OF_ASC_NODE', 'ARG_OF_PERICENTER', 'MEAN_ANOMALY'];
const nowIso = () => new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');

// Cada 6 h intenta renovar la lista de activos desde tu repositorio de GitHub
// (allí la descarga de CelesTrak sí funciona). Si falla no pasa nada:
// se sigue usando la última lista buena.
async function refreshActiveIds(env) {
  let lastTry = 0;
  try { lastTry = Number(await env.LAUNCHES_KV.get(KV_KEY_ACTIVE_ATTEMPT)) || 0; } catch (e) {}
  if (Date.now() - lastTry < ACTIVE_LIST_EVERY_MS) return;
  await env.LAUNCHES_KV.put(KV_KEY_ACTIVE_ATTEMPT, String(Date.now()));

  try {
    const res = await fetch(ACTIVE_IDS_URL, {
      headers: { 'User-Agent': COLLECTOR_UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    let data;
    try { data = JSON.parse(await res.text()); } catch (e) { throw new Error('la respuesta no es JSON válido'); }
    const ids = Array.isArray(data?.ids) ? data.ids.map(Number).filter(Number.isInteger) : [];
    if (ids.length < MIN_OBJECTS) throw new Error(`lista demasiado corta (${ids.length})`);

    // Si CelesTrak lleva tiempo fallando, GitHub sigue sirviendo la MISMA lista
    // vieja. Solo se acepta si es más nueva que la que ya tenemos: así una
    // lista repetida nunca borra los satélites recién lanzados que hemos añadido.
    try {
      const have = JSON.parse(await env.LAUNCHES_KV.get(KV_KEY_ACTIVE_IDS));
      if (have?.celestrakUpdated && data.updated && data.updated <= have.celestrakUpdated) {
        console.log(`Lista de activos: sin cambios (sigue siendo la de CelesTrak del ${have.celestrakUpdated})`);
        return;
      }
    } catch (e) {}

    // Los recién lanzados que ya habíamos añadido se conservan aunque
    // CelesTrak todavía no los tenga en su lista.
    let recentlyAdded = [];
    try {
      const prev = JSON.parse(await env.LAUNCHES_KV.get(KV_KEY_ACTIVE_IDS));
      const listSet = new Set(ids);
      recentlyAdded = (prev?.added || []).filter(x => !listSet.has(x.id) && Date.now() - x.at < RECENT_LAUNCH_DAYS * 86400000);
    } catch (e) {}

    const all = ids.concat(recentlyAdded.map(x => x.id));
    await env.LAUNCHES_KV.put(KV_KEY_ACTIVE_IDS, JSON.stringify({
      updated: nowIso(), source: 'github', celestrakUpdated: data.updated || null,
      count: all.length, ids: all, added: recentlyAdded,
    }));
    console.log(`Lista de activos: ${ids.length} satélites (GitHub, descargada de CelesTrak el ${data.updated || '?'})`);
  } catch (err) {
    console.warn('Lista de activos: GitHub no responde, se sigue usando la anterior —', err.message);
  }
}

// Devuelve la lista de activos guardada. La primera vez la saca de los
// TLE de CelesTrak que ya había en KV, para no empezar de cero.
async function loadActiveIds(env) {
  try {
    const raw = await env.LAUNCHES_KV.get(KV_KEY_ACTIVE_IDS);
    if (raw) {
      const p = JSON.parse(raw);
      if (Array.isArray(p.ids) && p.ids.length >= MIN_OBJECTS) return { set: new Set(p.ids), celestrakUpdated: p.celestrakUpdated || null };
    }
  } catch (e) {}
  try {
    const raw = await env.LAUNCHES_KV.get(KV_KEY_TLE);
    if (raw) {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr) && arr.length >= MIN_OBJECTS) {
        const ids = arr.map(o => Number(o.NORAD_CAT_ID)).filter(Number.isInteger);
        // Esos TLE venían de CelesTrak: su fecha es la fecha de esta lista.
        let celestrakUpdated = null;
        try { celestrakUpdated = JSON.parse(await env.LAUNCHES_KV.get(KV_KEY_TLE_META)).updated || null; } catch (e) {}
        await env.LAUNCHES_KV.put(KV_KEY_ACTIVE_IDS, JSON.stringify({ updated: nowIso(), source: 'tle guardados', celestrakUpdated, count: ids.length, ids }));
        return { set: new Set(ids), celestrakUpdated };
      }
    }
  } catch (e) {}
  return null;
}

async function fetchTleToKv(env) {
  try {
    await refreshActiveIds(env);
    const activeInfo = await loadActiveIds(env);
    if (!activeInfo) throw new Error('todavía no hay lista de satélites activos');
    const active = activeInfo.set;

    // Satélites (PAYLOAD) y objetos aún sin clasificar de lanzamientos recientes
    // (TBA, UNKNOWN), sin reentrar y con elementos de los últimos 30 días.
    const raw = await spaceTrackQuery(env,
      `class/gp/OBJECT_TYPE/PAYLOAD,TBA,UNKNOWN/DECAY_DATE/null-val/EPOCH/%3Enow-30/predicates/${TLE_FIELDS}/orderby/NORAD_CAT_ID%20asc/format/json`,
      240000);
    if (!Array.isArray(raw)) throw new Error('Space-Track no devolvió una lista (¿error o límite de uso?)');

    const recentCut = new Date(Date.now() - RECENT_LAUNCH_DAYS * 86400000).toISOString().slice(0, 10);
    const final = [];
    let skipped = 0, recent = 0;
    const newIds = [];
    for (const r of raw) {
      // Space-Track entrega todo como texto: se pasa a número (mismo formato que daba CelesTrak).
      let ok = true;
      for (const k of TLE_INT_FIELDS) {
        if (!isEmptyVal(r[k])) { const n = Number(r[k]); if (Number.isInteger(n)) r[k] = n; else ok = false; }
      }
      for (const k of TLE_FLOAT_FIELDS) {
        if (!isEmptyVal(r[k])) { const n = Number(r[k]); if (Number.isFinite(n)) r[k] = n; else ok = false; }
      }
      if (!ok || TLE_REQUIRED.some(k => isEmptyVal(r[k]))) { skipped++; continue; }

      const isActive = active.has(r.NORAD_CAT_ID);
      const isRecent = !isEmptyVal(r.LAUNCH_DATE) && r.LAUNCH_DATE >= recentCut;
      if (!isActive && !isRecent) continue;
      if (!isActive) { recent++; newIds.push(r.NORAD_CAT_ID); }
      delete r.LAUNCH_DATE;   // solo hacía falta para el filtro
      final.push(r);
    }
    if (final.length < MIN_OBJECTS) throw new Error(`solo ${final.length} satélites válidos (mínimo ${MIN_OBJECTS})`);

    const bytes = await putKvChecked(env, KV_KEY_TLE, JSON.stringify(final));
    await env.LAUNCHES_KV.put(KV_KEY_TLE_META, JSON.stringify({
      updated: nowIso(),
      source: 'space-track.org',
      count: final.length,
      listUpdated: activeInfo.celestrakUpdated,   // fecha de la lista de activos de CelesTrak que se ha usado
    }));
    if (newIds.length) {
      try {
        const p = JSON.parse(await env.LAUNCHES_KV.get(KV_KEY_ACTIVE_IDS));
        const known = new Set(p.ids);
        const add = newIds.filter(id => !known.has(id));
        if (add.length) {
          p.ids = p.ids.concat(add);
          p.count = p.ids.length;
          p.added = (p.added || []).concat(add.map(id => ({ id, at: Date.now() })));
          await env.LAUNCHES_KV.put(KV_KEY_ACTIVE_IDS, JSON.stringify(p));
        }
      } catch (e) {}
    }
    console.log(`TLE: ${final.length} satélites guardados desde Space-Track (${recent} recién lanzados, ${(bytes / 1048576).toFixed(1)} MB). Descartados por datos incompletos: ${skipped}`);
  } catch (err) {
    console.error('TLE: fallo, se conservan los datos anteriores —', err.message);
  }
}

// ── Space-Track: sesión por cookie (lo que hacía curl -c/-b) ────
async function spaceTrackLogin(env) {
  if (!env.SPACETRACK_USER || !env.SPACETRACK_PASS) throw new Error('faltan los secrets SPACETRACK_USER / SPACETRACK_PASS');
  const res = await fetch(`${SPACETRACK_BASE}/ajaxauth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': COLLECTOR_UA },
    body: new URLSearchParams({ identity: env.SPACETRACK_USER, password: env.SPACETRACK_PASS }).toString(),
    signal: AbortSignal.timeout(30000),
  });
  await res.text().catch(() => {});
  const setCookies = typeof res.headers.getSetCookie === 'function'
    ? res.headers.getSetCookie()
    : (res.headers.get('set-cookie') || '').split(/,(?=\s*[^;,]+=)/);
  const cookie = setCookies.map(c => c.split(';')[0].trim()).filter(Boolean).join('; ');
  // Con credenciales malas responde 200 {"Login":"Failed"}: se comprueba la cookie, no el HTTP.
  if (!cookie.includes('chocolatechip')) throw new Error('el login no devolvió sesión; revisa las credenciales');
  return cookie;
}

async function spaceTrackLogout(cookie) {
  try {
    const r = await fetch(`${SPACETRACK_BASE}/ajaxauth/logout`, {
      headers: { 'Cookie': cookie, 'User-Agent': COLLECTOR_UA },
      signal: AbortSignal.timeout(15000),
    });
    await r.body?.cancel();
  } catch (e) { /* cerrar sesión es cortesía, no debe romper nada */ }
}

// Login → una consulta → logout (siempre). Devuelve el JSON ya parseado.
async function spaceTrackQuery(env, path, timeoutMs) {
  const cookie = await spaceTrackLogin(env);
  try {
    const res = await fetch(`${SPACETRACK_BASE}/basicspacedata/query/${path}`, {
      headers: { 'Cookie': cookie, 'User-Agent': COLLECTOR_UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) throw new Error('Space-Track devolvió HTTP ' + res.status);
    const text = await res.text();
    try { return JSON.parse(text); } catch (e) { throw new Error('la respuesta no es JSON válido'); }
  } finally {
    await spaceTrackLogout(cookie);
  }
}

// Como spaceTrackQuery, pero con UNA sola sesión para varias consultas seguidas:
// login → consulta 1 → consulta 2 → ... → logout.
async function spaceTrackSession(env, fn) {
  const cookie = await spaceTrackLogin(env);
  try {
    const query = async (path, timeoutMs) => {
      const res = await fetch(`${SPACETRACK_BASE}/basicspacedata/query/${path}`, {
        headers: { 'Cookie': cookie, 'User-Agent': COLLECTOR_UA, 'Accept': 'application/json' },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!res.ok) throw new Error('Space-Track devolvió HTTP ' + res.status);
      const text = await res.text();
      try { return JSON.parse(text); } catch (e) { throw new Error('la respuesta no es JSON válido'); }
    };
    return await fn(query);
  } finally {
    await spaceTrackLogout(cookie);
  }
}

// ── 2. Basura espacial (fecth-debris.yml) ───────────────────────
const DEBRIS_FIELDS = 'NORAD_CAT_ID,OBJECT_NAME,OBJECT_ID,EPOCH,MEAN_MOTION,ECCENTRICITY,INCLINATION,RA_OF_ASC_NODE,ARG_OF_PERICENTER,MEAN_ANOMALY,EPHEMERIS_TYPE,CLASSIFICATION_TYPE,ELEMENT_SET_NO,REV_AT_EPOCH,BSTAR,MEAN_MOTION_DOT,MEAN_MOTION_DDOT,OBJECT_TYPE,RCS_SIZE,COUNTRY_CODE';
const DEBRIS_INT_FIELDS   = ['NORAD_CAT_ID', 'EPHEMERIS_TYPE', 'ELEMENT_SET_NO', 'REV_AT_EPOCH'];
const DEBRIS_FLOAT_FIELDS = ['MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 'RA_OF_ASC_NODE', 'ARG_OF_PERICENTER', 'MEAN_ANOMALY', 'BSTAR', 'MEAN_MOTION_DOT', 'MEAN_MOTION_DDOT'];
const DEBRIS_REQUIRED     = ['NORAD_CAT_ID', 'EPOCH', 'MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 'RA_OF_ASC_NODE', 'ARG_OF_PERICENTER', 'MEAN_ANOMALY'];
const isEmptyVal = (v) => v === null || v === undefined || v === '';

async function fetchDebrisToKv(env) {
  try {
    // Fragmentos (DEBRIS) y cuerpos de cohete (ROCKET BODY), sin reentrar ya
    // (DECAY_DATE null) y con elementos de los últimos 30 días.
    // %3E = '>', %20 = espacio. Van en una sola consulta y se distinguen luego
    // por OBJECT_TYPE, que ahora se pide y se guarda como campo.
    const raw = await spaceTrackQuery(env,
      `class/gp/OBJECT_TYPE/DEBRIS,ROCKET%20BODY/DECAY_DATE/null-val/EPOCH/%3Enow-30/predicates/${DEBRIS_FIELDS}/orderby/NORAD_CAT_ID%20asc/format/json`,
      240000);
    if (!Array.isArray(raw)) throw new Error('Space-Track no devolvió una lista (¿error o límite de uso?)');

    // Space-Track entrega todo como texto: se pasa a número (mismo formato que tle.json).
    const objects = new Map();
    let skipped = 0;
    for (const r of raw) {
      let ok = true;
      for (const k of DEBRIS_INT_FIELDS) {
        if (!isEmptyVal(r[k])) { const n = Number(r[k]); if (Number.isInteger(n)) r[k] = n; else ok = false; }
      }
      for (const k of DEBRIS_FLOAT_FIELDS) {
        if (!isEmptyVal(r[k])) { const n = Number(r[k]); if (Number.isFinite(n)) r[k] = n; else ok = false; }
      }
      if (!ok || DEBRIS_REQUIRED.some(k => isEmptyVal(r[k]))) { skipped++; continue; }
      objects.set(r.NORAD_CAT_ID, r);
    }

    const final = [...objects.keys()].sort((a, b) => a - b).map(k => objects.get(k));
    if (final.length < MIN_OBJECTS) throw new Error(`solo ${final.length} objetos válidos (mínimo esperado ${MIN_OBJECTS})`);

    const bytes = await putKvChecked(env, KV_KEY_DEBRIS, JSON.stringify({
      updated: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
      source: 'Space-Track.org (18th Space Defense Squadron)',
      count: final.length,
      objects: final,
    }));
    const nRb = final.reduce((a, o) => a + (o.OBJECT_TYPE === 'ROCKET BODY' ? 1 : 0), 0);
    console.log(`Debris: ${final.length} objetos guardados — ${final.length - nRb} fragmentos + ${nRb} cuerpos de cohete (${(bytes / 1048576).toFixed(1)} MB). Descartados por datos incompletos: ${skipped}`);
  } catch (err) {
    console.error('Debris: fallo, se conservan los datos anteriores —', err.message);
  }
}

// ── 3. Predicciones de reentrada (fetch-reentries.yml) ──────────
async function fetchReentriesToKv(env) {
  try {
    // Libreta de nombres: lo que ya guardamos la última vez. Así solo preguntamos
    // a Space-Track por los objetos que todavía no conocemos.
    const known = new Map();
    try {
      const prevRaw = await env.LAUNCHES_KV.get(KV_KEY_REENTRIES);
      if (prevRaw) {
        for (const r of (JSON.parse(prevRaw).reentries || [])) {
          if (r.name) known.set(r.norad, { name: r.name, objectType: r.objectType, country: r.country, launch: r.launch, rcs: r.rcs });
        }
      }
    } catch (e) { /* sin libreta previa: se piden todos */ }

    // Una sola sesión de Space-Track para las dos consultas.
    const { raw, satcat } = await spaceTrackSession(env, async (query) => {
      const raw = await query('class/tip/DECAY_EPOCH/%3Enow/orderby/DECAY_EPOCH%20asc/format/json', 120000);
      if (!Array.isArray(raw)) throw new Error('Space-Track no devolvió una lista (¿error o límite de uso?)');

      const missing = [...new Set(raw.map(r => parseInt(r.NORAD_CAT_ID, 10)).filter(n => n && !known.has(n)))];
      const satcat = [];
      if (missing.length) {
        // Si esta segunda consulta falla, NO se pierde la lista de reentradas:
        // se publican igualmente, solo que sin nombre (se reintenta en 6 h).
        try {
          for (let i = 0; i < missing.length; i += 150) {
            const chunk = missing.slice(i, i + 150);
            const rows = await query(
              `class/satcat/NORAD_CAT_ID/${chunk.join(',')}/CURRENT/Y/predicates/NORAD_CAT_ID,OBJECT_NAME,OBJECT_TYPE,COUNTRY,LAUNCH,RCS_SIZE/format/json`,
              60000);
            if (Array.isArray(rows)) satcat.push(...rows);
          }
        } catch (e) {
          console.warn('Reentries: no se pudieron obtener los nombres, se publica sin ellos —', e.message);
        }
      }
      return { raw, satcat };
    });

    for (const s of satcat) {
      const n = parseInt(s.NORAD_CAT_ID, 10);
      if (n) known.set(n, { name: s.OBJECT_NAME || null, objectType: s.OBJECT_TYPE || null, country: s.COUNTRY || null, launch: s.LAUNCH || null, rcs: s.RCS_SIZE || null });
    }

    // Un objeto recibe varios avisos según se afina la predicción: nos quedamos con el más reciente.
    const latest = new Map();
    for (const r of raw) {
      const norad = parseInt(r.NORAD_CAT_ID, 10);
      if (!norad) continue;
      const info = known.get(norad) || {};
      const item = {
        norad,
        name:         info.name       || null,   // NUEVO
        objectType:   info.objectType || null,   // NUEVO: PAYLOAD / ROCKET BODY / DEBRIS
        country:      info.country    || null,   // NUEVO
        launch:       info.launch     || null,   // NUEVO
        rcs:          info.rcs        || null,   // NUEVO
        decayEpoch:   r.DECAY_EPOCH,    // momento previsto (UTC)
        window:       r.WINDOW,         // incertidumbre en minutos
        lat:          r.LAT,            // dónde cruza los 10 km de altitud (NO el punto de impacto)
        lon:          r.LON,
        incl:         r.INCL,
        direction:    r.DIRECTION,
        rev:          r.REV,
        nextReport:   r.NEXT_REPORT,    // horas hasta el próximo aviso
        highInterest: r.HIGH_INTEREST,
        msgEpoch:     r.MSG_EPOCH,
        insertEpoch:  r.INSERT_EPOCH,
      };
      const prev = latest.get(norad);
      if (!prev || (item.insertEpoch || '') > (prev.insertEpoch || '')) latest.set(norad, item);
    }

    const final = [...latest.values()].sort((a, b) => (a.decayEpoch || '') < (b.decayEpoch || '') ? -1 : (a.decayEpoch || '') > (b.decayEpoch || '') ? 1 : 0);
    if (!final.length) {
      console.warn('Reentries: Space-Track no devolvió ninguna reentrada prevista. Se conserva el dato anterior.');
      return;
    }

    await putKvChecked(env, KV_KEY_REENTRIES, JSON.stringify({
      updated: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
      source: 'Space-Track.org (18th Space Defense Squadron)',
      count: final.length,
      reentries: final,
    }));
    console.log(`Reentries: ${final.length} reentradas previstas guardadas (${satcat.length} nombres nuevos)`);
  } catch (err) {
    console.error('Reentries: fallo, se conservan los datos anteriores —', err.message);
  }
}

// ── 4. Rastro de los rovers de Marte (update_rover_trail.py) ────
const ROVER_SOURCES = {
  perseverance: 'https://mars.nasa.gov/mmgis-maps/M20/Layers/json/M20_waypoints.json',
  curiosity:    'https://mars.nasa.gov/mmgis-maps/MSL/Layers/json/MSL_waypoints.json',
};

async function fetchRoverTrailsToKv(env) {
  for (const [roverId, url] of Object.entries(ROVER_SOURCES)) {
    try {
      const res = await fetch(url, {
        headers: { 'Accept': 'application/json', 'User-Agent': 'SatFleetLive-RoverProxy/1.0 (https://satfleetlive.com)' },
        signal: AbortSignal.timeout(60000),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();

      const points = [];
      for (const f of (data && data.features) || []) {
        const p = (f && f.properties) || {};
        if (p.lat == null || p.lon == null) continue;
        points.push({
          lat: p.lat,
          lng: p.lon,
          sol: p.sol ?? null,
          distTotalKm: p.dist_total_m ? Math.round(p.dist_total_m / 10) / 100 : null,
        });
      }
      if (!points.length) throw new Error('la NASA no devolvió ningún punto válido');

      await putKvChecked(env, KV_KEY_ROVER_PREFIX + roverId, JSON.stringify({
        points,
        updated: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
      }));
      console.log(`Rover ${roverId}: ${points.length} puntos guardados`);
    } catch (err) {
      // Un rover que falla no impide actualizar el otro, ni toca su KV anterior.
      console.error(`Rover ${roverId}: fallo, se conserva el dato anterior —`, err.message);
    }
  }
}

export default {
  async scheduled(event, env, ctx) {
    // Tres tareas comparten el mismo "despertador" de Cron Triggers,
    // diferenciadas por qué expresión cron ha disparado esta ejecución.
    // Colectores de datos (antes GitHub Actions). Cada uno captura sus propios
    // errores y NUNCA sobrescribe el KV si algo falla.
    switch (event.cron) {
      case TLE_CRON:       await fetchTleToKv(env);          return;
      case DEBRIS_CRON:    await fetchDebrisToKv(env);       return;
      case REENTRIES_CRON: await fetchReentriesToKv(env);    return;
      case ROVER_CRON:     await fetchRoverTrailsToKv(env);  return;
    }
    if (event.cron === TLE_ARCHIVE_CRON) {
      await archiveTleSnapshot(env);
      return;
    }
    if (event.cron === DEEP_SPACE_WARM_CRON) {
      // handleDeepSpace ya sabe no hacer nada si la caché sigue viva —
      // esto solo dispara trabajo de verdad cuando hace falta, pero lo
      // hace EL PROPIO WORKER, nunca un usuario real esperando en directo.
      const fakeCtx = { waitUntil: (p) => ctx.waitUntil(p) };
      await handleDeepSpace(fakeCtx, env, true);
      return;
    }
    const fakeCtx = { waitUntil: (p) => ctx.waitUntil(p) };
    await handleLaunches(fakeCtx, env, true); // fuerza refresh para ejecutar notificaciones
  },

  // ════════════════════════════════════════════════════════
  // Recoge los avisos de pases cuando les toca su hora — el "despertador"
  // ════════════════════════════════════════════════════════
  async queue(batch, env, ctx) {
    for (const message of batch.messages) {
      try {
        const { type, alertKey, token, title, body, fireAt, url } = message.body;

        // ¿Sigue activo el aviso, o el usuario lo canceló mientras esperaba?
        const stillActive = await env.LAUNCHES_KV.get(alertKey);

        if (type === 'fire') {
          if (stillActive) {
            await sendFcmMessage(env, token, 'token', title, body, {
              url: url || 'https://satfleetlive.com',
            }, 900); // 15 min — un aviso de "pasa en 10 min" no sirve de nada si llega horas tarde
          }
        } else if (type === 'recheck') {
          // Relevo de un aviso que estaba a más de 24h — ¿cuánto falta ya?
          const remainingMs = fireAt - Date.now();
          if (!stillActive) {
            // se canceló mientras esperábamos — no hacemos nada más
          } else if (remainingMs <= 0) {
            await sendFcmMessage(env, token, 'token', title, body, {
              url: url || 'https://satfleetlive.com',
            }, 900); // mismo motivo que el otro aviso de pase: tarde, no sirve
          } else if (remainingMs <= 86_400_000) {
            // ya cabe en un solo tramo — mandamos el aviso final, preciso
            await env.PASS_ALERT_QUEUE.send(
              { type: 'fire', alertKey, token, title, body, url },
              { delaySeconds: Math.ceil(remainingMs / 1000) }
            );
          } else {
            // todavía falta más de 24h — otro relevo
            await env.PASS_ALERT_QUEUE.send(
              { type: 'recheck', alertKey, token, title, body, fireAt, url },
              { delaySeconds: 86400 }
            );
          }
        }

        message.ack();
      } catch (err) {
        console.error('Error procesando aviso de la cola:', err.message);
        message.retry();
      }
    }
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
    if (pathname === '/api/check-premium' && request.method === 'GET') {
    const uid = extractUidFromJWT(request.headers.get('Authorization'));
    if (!uid) return new Response(JSON.stringify({ premium: false, reason: 'auth_required' }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });

    // ?live=1 se salta el caché — lo usa account.html justo después de pagar,
    // cuando necesita una respuesta 100% real, no una guardada de hace un rato.
    const skipCache = new URL(request.url).searchParams.get('live') === '1';

    // 1. Caché corta: si ya sabemos la respuesta de hace poco, la devolvemos
    // al instante, sin gastar ni una llamada a RevenueCat.
    const cpNow = Date.now();
    const cachedEntry = _checkPremiumCache.get(uid);
    if (!skipCache && cachedEntry && (cpNow - cachedEntry.cachedAt) < CHECK_PREMIUM_CACHE_TTL_MS) {
      return new Response(JSON.stringify({ premium: cachedEntry.premium, store: cachedEntry.store }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
    }

    const rcKey = env.REVENUECAT_API_KEY;
    if (!rcKey) return new Response(JSON.stringify({ premium: false }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
    try {
      const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(uid)}`, {
        headers: { 'Authorization': `Bearer ${rcKey}`, 'Content-Type': 'application/json' }
      });

      if (!rcRes.ok) {
        // RevenueCat no contestó bien (saturado, 429...) — si teníamos una
        // respuesta buena reciente de este usuario, la mantenemos en vez de
        // bajarle a "no premium" por un fallo que no es suyo.
        if (cachedEntry) {
          return new Response(JSON.stringify({ premium: cachedEntry.premium, store: cachedEntry.store }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
        }
        return new Response(JSON.stringify({ premium: false }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
      }

      const rcData = await rcRes.json();
      const ents = rcData?.subscriber?.entitlements || {};
      let isPremium = false, store = null;
      for (const key in ents) {
        const ent = ents[key];
        if (!ent.expires_date || new Date(ent.expires_date) > new Date()) {
          isPremium = true;
          const subInfo = rcData.subscriber.subscriptions?.[ent.product_identifier];
          store = subInfo?.store || 'stripe';
          break;
        }
      }

      // 2. Guardamos la respuesta buena en la libreta — sirve para la
      // próxima petición Y como red de seguridad si RevenueCat falla luego.
      _checkPremiumCache.set(uid, { premium: isPremium, store, cachedAt: cpNow });
      if (_checkPremiumCache.size > 500) {
        for (const [key, val] of _checkPremiumCache) {
          if (cpNow - val.cachedAt > CHECK_PREMIUM_CACHE_TTL_MS) _checkPremiumCache.delete(key);
        }
      }

      return new Response(JSON.stringify({ premium: isPremium, store }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
    } catch(e) {
      if (cachedEntry) {
        return new Response(JSON.stringify({ premium: cachedEntry.premium, store: cachedEntry.store }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
      }
      return new Response(JSON.stringify({ premium: false }), { status: 200, headers: makeHeaders({ 'Content-Type': 'application/json' }) });
    }
  }

    if (pathname === '/api/fcm/subscribe' && request.method === 'POST') {
    return handleFcmSubscribe(request, env);
  }

  if (pathname === '/api/notify/pass' && request.method === 'POST') {
        // 🔒 ÚNICO ENDPOINT PREMIUM
        const uid       = extractUidFromJWT(request.headers.get('Authorization'));
        const isPremium = await checkPremiumStatus(uid, env);
        if (!isPremium) return premiumRequired();
        return handleNotifyPass(request, env);
    }

    if (pathname === '/api/stripe/checkout' && request.method === 'POST') {
        return handleStripeCheckout(request, env);
    }

    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', { status: 405, headers: makeHeaders() });
    }

    if (pathname === '/api/tle') return handleTle(ctx, env);
    if (pathname === '/api/tle-playback') {
      const url = new URL(request.url);
      const dateParam = url.searchParams.get('date');
      const yesterday = new Date(Date.now() - 24 * 3600 * 1000).toISOString().slice(0, 10);
      if (dateParam < yesterday) {
        // Ayer y hoy quedan libres, sin comprobar nada — cualquier fecha
        // MÁS ANTIGUA que ayer es de pago.
        const uid = extractUidFromJWT(request.headers.get('Authorization'));
        const isPremium = await checkPremiumStatus(uid, env);
        if (!isPremium) return premiumRequired();
      }
      return handleTlePlayback(request, env);
    }
    if (pathname === '/api/tle-archive-index') return handleTleArchiveIndex(env);
    if (pathname.startsWith('/api/tle/'))        return handleTleSingle(request, env);
    if (pathname.startsWith('/api/satellite/'))  return handleSatelliteInfo(request, env);
    if (pathname.startsWith('/api/launches/upcoming')) return handleLaunches(ctx, env);
    if (pathname === '/api/moon-orbiters') return handleMoonOrbiters(ctx, env);
    if (pathname === '/api/moon-orbiters-playback') {
      const uid = extractUidFromJWT(request.headers.get('Authorization'));
      const isPremium = await checkPremiumStatus(uid, env);
      if (!isPremium) return premiumRequired();
      return handleMoonOrbitersPlayback(request, ctx, env);
    }
    if (pathname === '/api/mars-orbiters') return handleMarsOrbiters(ctx, env);
    if (pathname === '/api/mars-orbiters-playback') {
      const uid = extractUidFromJWT(request.headers.get('Authorization'));
      const isPremium = await checkPremiumStatus(uid, env);
      if (!isPremium) return premiumRequired();
      return handleMarsOrbitersPlayback(request, ctx, env);
    }
    if (pathname === '/api/deep-space') return handleDeepSpace(ctx, env);
    if (pathname === '/api/deep-space-playback') {
      const uid = extractUidFromJWT(request.headers.get('Authorization'));
      const isPremium = await checkPremiumStatus(uid, env);
      if (!isPremium) return premiumRequired();
      return handleDeepSpacePlayback(request, ctx, env);
    }
    if (pathname.startsWith('/api/rover-trail/')) return handleRoverTrail(request, ctx, env);
    if (pathname.startsWith('/api/data/')) return handleDataFile(request, ctx, env);

    return new Response(JSON.stringify({ error: 'Not Found', path: pathname }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  },
};