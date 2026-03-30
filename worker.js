/**
 * SatFleet Live — Cloudflare Worker v2
 *
 * Endpoints
 * ─────────────────────────────────────────────
 *   GET /api/tle                  TLE activo de Celestrak   (caché 6 h)
 *   GET /api/launches/upcoming    Lanzamientos The Space Devs (caché 15 min)
 *
 * Features
 * ─────────────────────────────────────────────
 *   • CORS + Security headers en todas las respuestas
 *   • Cache API de Cloudflare (cero KV, cero base de datos)
 *   • Rate limiting in-memory por IP (60 req/min por aislado)
 *   • Compresión automática: Cloudflare comprime text/plain si el
 *     cliente envía Accept-Encoding: gzip/br (sin código extra)
 *
 * Notas de despliegue
 * ─────────────────────────────────────────────
 *   • wrangler.toml debe tener:   compatibility_date = "2024-01-01"
 *     y NO necesita KV bindings ni D1.
 *   • Si ya tienes un wrangler.toml con format = "esm", no hace falta
 *     cambiar nada más. Si usas el dashboard, pega este código tal cual.
 */

'use strict';

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════

const CELESTRAK_URL  = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle';
const SPACEDEVS_URL  = 'https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=100&mode=detailed&format=json';

const TLE_TTL        = 6 * 3600;   // 6 horas en segundos
const TLE_STALE      = 3600;       // stale-while-revalidate 1 h
const LAUNCHES_TTL   = 15 * 60;   // 15 min
const LAUNCHES_STALE = 5  * 60;   // stale-while-revalidate 5 min

// Claves sintéticas para la Cache API (deben ser URLs válidas)
const CACHE_KEY_TLE      = 'https://internal.satfleetlive/cache/tle-v1';
const CACHE_KEY_LAUNCHES = 'https://internal.satfleetlive/cache/launches-v1';

// ═══════════════════════════════════════════════════════════════
// RATE LIMITING  (in-memory, por isolate de Cloudflare)
// ═══════════════════════════════════════════════════════════════
//
// Un "isolate" es la instancia del Worker que Cloudflare arranca.
// Puede haber varios en paralelo, por lo que esto no es global,
// pero sí bloquea ráfagas agresivas desde una sola IP en un mismo
// isolate. Para enforcement global usa Cloudflare Rate Limiting
// Rules en el dashboard (gratis hasta 10 k req/mes adicionales).

const RL_WINDOW_MS = 60_000;   // ventana de 1 minuto
const RL_MAX_REQ   = 60;       // peticiones máximas por IP por ventana

const _rl = new Map();         // { ip → { n: number, reset: timestamp } }

/**
 * Devuelve true si la IP ha superado el límite y debe ser bloqueada.
 */
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

/**
 * Elimina entradas antiguas del mapa para evitar fugas de memoria.
 * Se llama en ~1 % de las peticiones vía ctx.waitUntil().
 */
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
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Accept',
};

const SEC_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options':        'DENY',
  'X-XSS-Protection':       '1; mode=block',
  'Referrer-Policy':        'strict-origin-when-cross-origin',
  'Permissions-Policy':     'geolocation=(), microphone=()',
};

/**
 * Devuelve un objeto plano con CORS + SEC + cualquier header extra.
 */
function makeHeaders(extra = {}) {
  return { ...CORS_HEADERS, ...SEC_HEADERS, ...extra };
}

/**
 * Recrea una Response cacheada garantizando que lleva CORS headers frescos.
 * Necesario porque los headers de la Response cacheada son inmutables.
 */
function wrapCached(cached) {
  const h = new Headers(cached.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) h.set(k, v);
  return new Response(cached.body, { status: cached.status, headers: h });
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/tle
// ═══════════════════════════════════════════════════════════════

async function handleTle(ctx) {
  const cache = caches.default;

  // 1. Servir desde caché si existe (hit instantáneo, ~0 ms)
  const hit = await cache.match(CACHE_KEY_TLE);
  if (hit) return wrapCached(hit);

  // 2. Fetch de Celestrak (solo cuando el caché ha caducado)
  let upstream;
  try {
    upstream = await fetch(CELESTRAK_URL, {
      headers: {
        // Identificamos el bot con una cadena amigable para Celestrak
        'User-Agent': 'SatFleetLive/2.0 (https://satfleetlive.com; contact: jaime.automatiza@gmail.com)',
        'Accept':     'text/plain',
      },
      // También pedimos al edge de Cloudflare que cachee este leg
      cf: { cacheTtl: TLE_TTL, cacheEverything: true },
    });
  } catch (err) {
    return new Response('Celestrak unreachable: ' + err.message, {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'text/plain' }),
    });
  }

  if (!upstream.ok) {
    return new Response(`Celestrak returned HTTP ${upstream.status}`, {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'text/plain' }),
    });
  }

  const tleText = await upstream.text();

  // Añadimos el timestamp igual que hacía el GitHub Action
  // (los parsers HTML ya ignoran líneas que empiezan por '#')
  const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  const body  = `# Last update: ${stamp}\n${tleText}`;

  const resp = new Response(body, {
    status: 200,
    headers: makeHeaders({
      'Content-Type':  'text/plain; charset=utf-8',
      // Cloudflare comprime automáticamente text/plain con gzip o brotli
      // según el Accept-Encoding del cliente. El header Vary lo indica.
      'Cache-Control': `public, max-age=${TLE_TTL}, stale-while-revalidate=${TLE_STALE}`,
      'Vary':          'Accept-Encoding',
    }),
  });

  // Guardamos en la Cache API sin bloquear la respuesta al cliente
  ctx.waitUntil(cache.put(CACHE_KEY_TLE, resp.clone()));
  return resp;
}

// ═══════════════════════════════════════════════════════════════
// HANDLER: /api/launches/upcoming
// ═══════════════════════════════════════════════════════════════

async function handleLaunches(ctx) {
  const cache = caches.default;

  const hit = await cache.match(CACHE_KEY_LAUNCHES);
  if (hit) return wrapCached(hit);

  let upstream;
  try {
    upstream = await fetch(SPACEDEVS_URL, {
      headers: {
        'User-Agent': 'SatFleetLive/2.0 (https://satfleetlive.com)',
        'Accept':     'application/json',
      },
      cf: { cacheTtl: LAUNCHES_TTL, cacheEverything: true },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Space Devs unreachable: ' + err.message }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  if (!upstream.ok) {
    return new Response(JSON.stringify({ error: `Space Devs error ${upstream.status}` }), {
      status: 502,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  }

  const data = await upstream.json();

  const resp = new Response(JSON.stringify(data), {
    status: 200,
    headers: makeHeaders({
      'Content-Type':  'application/json; charset=utf-8',
      'Cache-Control': `public, max-age=${LAUNCHES_TTL}, stale-while-revalidate=${LAUNCHES_STALE}`,
    }),
  });

  ctx.waitUntil(cache.put(CACHE_KEY_LAUNCHES, resp.clone()));
  return resp;
}

// ═══════════════════════════════════════════════════════════════
// ROUTER PRINCIPAL
// ═══════════════════════════════════════════════════════════════

export default {
  async fetch(request, env, ctx) {
    const { pathname } = new URL(request.url);

    // ── CORS preflight ──────────────────────────────────────────
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: makeHeaders() });
    }

    // ── Solo GET ────────────────────────────────────────────────
    if (request.method !== 'GET') {
      return new Response('Method Not Allowed', {
        status: 405,
        headers: makeHeaders(),
      });
    }

    // ── Rate limiting ───────────────────────────────────────────
    const ip = request.headers.get('CF-Connecting-IP') ?? '0.0.0.0';
    if (rateLimited(ip)) {
      return new Response(JSON.stringify({ error: 'Too Many Requests' }), {
        status: 429,
        headers: makeHeaders({
          'Content-Type': 'application/json',
          'Retry-After':  '60',
        }),
      });
    }

    // Limpieza del mapa RL ~1 % de las veces para no acumular memoria
    if (Math.random() < 0.01) {
      ctx.waitUntil(Promise.resolve().then(pruneRL));
    }

    // ── Rutas ───────────────────────────────────────────────────
    if (pathname === '/api/tle') {
      return handleTle(ctx);
    }

    if (pathname.startsWith('/api/launches/upcoming')) {
      return handleLaunches(ctx);
    }

    // 404 para cualquier otra ruta
    return new Response(JSON.stringify({ error: 'Not Found', path: pathname }), {
      status: 404,
      headers: makeHeaders({ 'Content-Type': 'application/json' }),
    });
  },
};