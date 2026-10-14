/* ============================================================
   SatFleet Live - Sun & Moon transit finder (Web Worker)
   Satellites: ISS, Tiangong (CSS) and Hubble
   ------------------------------------------------------------
   Runs in the background so the page never freezes.
   - Satellite positions: SGP4 (satellite.js, same file the site uses)
   - Sun and Moon: Astronomy Engine 2.1.19 (MIT), from jsDelivr
   All maths is done in an Earth-fixed frame (ECEF, km).
   ============================================================ */

/* global satellite, Astronomy, importScripts */

var SFT_IS_WORKER = (typeof importScripts === 'function' && typeof window === 'undefined');

if (SFT_IS_WORKER) {
  importScripts(
    '/js/satellite.min.js',
    'https://cdn.jsdelivr.net/npm/astronomy-engine@2.1.19/astronomy.browser.min.js'
  );
}

var SFT = (function () {
  'use strict';

  // ---------- Constants ----------
  var DEG = Math.PI / 180;
  var RAD = 180 / Math.PI;
  var WGS84_A = 6378.137;
  var WGS84_F = 1 / 298.257223563;
  var WGS84_B = WGS84_A * (1 - WGS84_F);
  var E2 = WGS84_F * (2 - WGS84_F);
  var AU_KM = 149597870.7;
  var EARTH_MEAN_R = 6371.0088;
  var BODY_RADIUS_KM = { Sun: 695700, Moon: 1737.4 };
  var SEARCH_RADIUS_KM = 200;       // always search the largest radius; the page filters
  var GRID_STEP_MS = 10 * 60000;    // Sun/Moon positions every 10 min, then interpolated
  var LINE_HALF_SPAN_KM = 280;      // centre line kept for the map: this far each side

  // ---------- Small vector helpers ----------
  function sub(a, b) { return [a[0] - b[0], a[1] - b[1], a[2] - b[2]]; }
  function add(a, b) { return [a[0] + b[0], a[1] + b[1], a[2] + b[2]]; }
  function mul(a, k) { return [a[0] * k, a[1] * k, a[2] * k]; }
  function dot(a, b) { return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]; }
  function cross(a, b) { return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]; }
  function norm(a) { return Math.sqrt(dot(a, a)); }
  function unit(a) { var n = norm(a); return [a[0] / n, a[1] / n, a[2] / n]; }
  function angleBetween(a, b) {
    // Robust for tiny angles
    return Math.atan2(norm(cross(a, b)), dot(a, b));
  }

  // ---------- Earth (WGS84) ----------
  function geodeticToEcef(latDeg, lonDeg, hKm) {
    var lat = latDeg * DEG, lon = lonDeg * DEG;
    var s = Math.sin(lat), c = Math.cos(lat);
    var N = WGS84_A / Math.sqrt(1 - E2 * s * s);
    return [(N + hKm) * c * Math.cos(lon), (N + hKm) * c * Math.sin(lon), (N * (1 - E2) + hKm) * s];
  }

  function ecefToGeodetic(p) {
    var x = p[0], y = p[1], z = p[2];
    var lon = Math.atan2(y, x);
    var r = Math.sqrt(x * x + y * y);
    var lat = Math.atan2(z, r * (1 - E2));
    var h = 0;
    for (var i = 0; i < 6; i++) {
      var s = Math.sin(lat);
      var N = WGS84_A / Math.sqrt(1 - E2 * s * s);
      h = r / Math.cos(lat) - N;
      lat = Math.atan2(z, r * (1 - E2 * N / (N + h)));
    }
    return { lat: lat * RAD, lon: lon * RAD, h: h };
  }

  // Local East / North / Up unit vectors at a geodetic point
  function enuBasis(latDeg, lonDeg) {
    var la = latDeg * DEG, lo = lonDeg * DEG;
    var sl = Math.sin(la), cl = Math.cos(la), so = Math.sin(lo), co = Math.cos(lo);
    return {
      e: [-so, co, 0],
      n: [-sl * co, -sl * so, cl],
      u: [cl * co, cl * so, sl]
    };
  }

  function altAz(fromEcef, basis, targetEcef) {
    var d = unit(sub(targetEcef, fromEcef));
    var alt = Math.asin(Math.max(-1, Math.min(1, dot(d, basis.u)))) * RAD;
    var az = Math.atan2(dot(d, basis.e), dot(d, basis.n)) * RAD;
    if (az < 0) az += 360;
    return { alt: alt, az: az };
  }

  // Great-circle distance (km) and initial bearing (deg)
  function haversineKm(lat1, lon1, lat2, lon2) {
    var p1 = lat1 * DEG, p2 = lat2 * DEG, dp = (lat2 - lat1) * DEG, dl = (lon2 - lon1) * DEG;
    var a = Math.sin(dp / 2) * Math.sin(dp / 2) + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
    return 2 * EARTH_MEAN_R * Math.asin(Math.min(1, Math.sqrt(a)));
  }
  function bearingDeg(lat1, lon1, lat2, lon2) {
    var p1 = lat1 * DEG, p2 = lat2 * DEG, dl = (lon2 - lon1) * DEG;
    var y = Math.sin(dl) * Math.cos(p2);
    var x = Math.cos(p1) * Math.sin(p2) - Math.sin(p1) * Math.cos(p2) * Math.cos(dl);
    var b = Math.atan2(y, x) * RAD;
    return (b + 360) % 360;
  }

  // ---------- ISS (SGP4) ----------
  function makeIss(input) {
    var satrec = input.satrec ? input.satrec : satellite.json2satrec(input.omm);
    return function issEcef(tMs) {
      var date = new Date(tMs);
      var pv = satellite.propagate(satrec, date);
      if (!pv || !pv.position || typeof pv.position === 'boolean') return null;
      var gmst = satellite.gstime(date);
      var e = satellite.eciToEcf(pv.position, gmst);
      if (!isFinite(e.x)) return null;
      return [e.x, e.y, e.z];
    };
  }

  // ---------- Sun & Moon (Astronomy Engine) ----------
  // Positions are computed exactly every 10 minutes (true equator of date)
  // and interpolated in between. Over 10 minutes the Moon moves about
  // 5.5 arcminutes, and the straight-line error is below 0.1 arcsecond.
  function wrapPi(x) {
    x = x % (2 * Math.PI);
    if (x > Math.PI) x -= 2 * Math.PI;
    if (x < -Math.PI) x += 2 * Math.PI;
    return x;
  }

  function makeBody(name, t0, t1) {
    var body = Astronomy.Body[name];
    var n = Math.ceil((t1 - t0) / GRID_STEP_MS) + 2;
    var vx = new Float64Array(n), vy = new Float64Array(n), vz = new Float64Array(n), eq = new Float64Array(n);
    for (var i = 0; i < n; i++) {
      var tm = t0 + i * GRID_STEP_MS;
      var at = Astronomy.MakeTime(new Date(tm));
      var vJ = Astronomy.GeoVector(body, at, true);
      var vD = Astronomy.RotateVector(Astronomy.Rotation_EQJ_EQD(at), vJ);
      vx[i] = vD.x * AU_KM; vy[i] = vD.y * AU_KM; vz[i] = vD.z * AU_KM;
      // Apparent sidereal time minus the GMST used for the ISS
      // (equation of the equinoxes, plus tiny model differences)
      var gast = Astronomy.SiderealTime(at) * 15 * DEG;
      eq[i] = wrapPi(gast - satellite.gstime(new Date(tm)));
    }
    return function bodyEcef(tMs) {
      var f = (tMs - t0) / GRID_STEP_MS;
      var i = Math.max(0, Math.min(n - 2, Math.floor(f)));
      var w = f - i;
      var x = vx[i] + (vx[i + 1] - vx[i]) * w;
      var y = vy[i] + (vy[i + 1] - vy[i]) * w;
      var z = vz[i] + (vz[i + 1] - vz[i]) * w;
      var th = satellite.gstime(new Date(tMs)) + eq[i] + (eq[i + 1] - eq[i]) * w;
      var c = Math.cos(th), s = Math.sin(th);
      return [x * c + y * s, -x * s + y * c, z];
    };
  }

  // ---------- Shadow projection ----------
  // A person sees the ISS centred on the Sun/Moon when they stand on the
  // straight line that goes from the Sun/Moon, through the ISS, down to
  // the ground. That ground point, moving over time, is the centre line.
  function rayToGround(origin, dir) {
    var a2 = WGS84_A * WGS84_A, b2 = WGS84_B * WGS84_B;
    var A = (dir[0] * dir[0] + dir[1] * dir[1]) / a2 + dir[2] * dir[2] / b2;
    var B = 2 * ((origin[0] * dir[0] + origin[1] * dir[1]) / a2 + origin[2] * dir[2] / b2);
    var C = (origin[0] * origin[0] + origin[1] * origin[1]) / a2 + origin[2] * origin[2] / b2 - 1;
    var disc = B * B - 4 * A * C;
    if (disc < 0) return null;
    var s = (-B - Math.sqrt(disc)) / (2 * A);
    if (s <= 0) return null;
    return add(origin, mul(dir, s));
  }

  // Note: "iss" here means "the satellite" (ISS, Tiangong or Hubble)
  function Ctx(issEcef, bodyEcef, bodyName, sat) {
    this.iss = issEcef;
    this.body = bodyEcef;
    this.name = bodyName;
    this.sat = sat;
  }
  Ctx.prototype.ground = function (tMs) {
    var iss = this.iss(tMs);
    if (!iss) return null;
    var b = this.body(tMs);
    var g = rayToGround(iss, unit(sub(iss, b)));
    if (!g) return null;
    return { g: g, iss: iss, b: b };
  };
  // Angle between ISS and body centre, seen from a ground point
  Ctx.prototype.sep = function (q, tMs) {
    var iss = this.iss(tMs);
    if (!iss) return Infinity;
    return angleBetween(sub(iss, q), sub(this.body(tMs), q));
  };
  Ctx.prototype.bodyRadius = function (q, tMs) {
    return Math.asin(BODY_RADIUS_KM[this.name] / norm(sub(this.body(tMs), q)));
  };

  // Golden-section minimum of f on [a, b]
  function goldenMin(f, a, b, iters) {
    var g = 0.6180339887498949;
    var c = b - g * (b - a), d = a + g * (b - a);
    var fc = f(c), fd = f(d);
    for (var i = 0; i < iters; i++) {
      if (fc < fd) { b = d; d = c; fd = fc; c = b - g * (b - a); fc = f(c); }
      else { a = c; c = d; fc = fd; d = a + g * (b - a); fd = f(d); }
    }
    var x = (a + b) / 2;
    return { x: x, f: f(x) };
  }

  function bisect(f, a, b, iters) {
    var fa = f(a);
    for (var i = 0; i < iters; i++) {
      var m = (a + b) / 2, fm = f(m);
      if ((fm > 0) === (fa > 0)) { a = m; fa = fm; } else { b = m; }
    }
    return (a + b) / 2;
  }

  // Point on the ground at `km` from point p (ECEF, on surface) along a compass bearing
  function offsetPoint(pGeo, bearing, km) {
    var B = enuBasis(pGeo.lat, pGeo.lon);
    var p = geodeticToEcef(pGeo.lat, pGeo.lon, 0);
    var dir = add(mul(B.n, Math.cos(bearing * DEG)), mul(B.e, Math.sin(bearing * DEG)));
    var q = add(p, mul(dir, km));
    var g = ecefToGeodetic(q);
    return geodeticToEcef(g.lat, g.lon, 0);
  }

  // ---------- Refine one candidate ----------
  // ---------- Everything seen from one ground point at one moment ----------
  // Same set of numbers Transit Finder gives, plus a few extras for cameras.
  function wrap360(x) { x = x % 360; return x < 0 ? x + 360 : x; }
  function toEquatorOfDate(v, tMs) {
    // Earth-fixed direction -> right ascension / declination (true equator and equinox of date)
    var th = Astronomy.SiderealTime(Astronomy.MakeTime(new Date(tMs))) * 15 * DEG;
    var c = Math.cos(th), s = Math.sin(th);
    var u = unit(v);
    var x = u[0] * c - u[1] * s, y = u[0] * s + u[1] * c, z = u[2];
    return { raH: wrap360(Math.atan2(y, x) * RAD) / 15, decDeg: Math.asin(Math.max(-1, Math.min(1, z))) * RAD, gastH: th * RAD / 15 };
  }
  function parallacticTF(raH, decDeg, gastH, latDeg, lonDeg) {
    // Standard parallactic angle q, then sign flipped to match Transit Finder
    var H = ((gastH * 15 + lonDeg - raH * 15) % 360 + 540) % 360 - 180;
    var h = H * DEG, d = decDeg * DEG, f = latDeg * DEG;
    var q = Math.atan2(Math.sin(h), Math.cos(d) * Math.tan(f) - Math.sin(d) * Math.cos(h)) * RAD;
    return -q;
  }
  function viewFrom(ctx, Q, QB, Qgeo, tMs) {
    var s = ctx.iss(tMs), b = ctx.body(tMs);
    if (!s) return null;
    var v = sub(s, Q), D = norm(v), u = unit(v);
    var satHz = altAz(Q, QB, s), bodyHz = altAz(Q, QB, b);
    var rB = ctx.bodyRadius(Q, tMs);
    var sep = angleBetween(v, sub(b, Q));
    // Velocity relative to the observer (both turn with the Earth)
    var sA = ctx.iss(tMs - 250), sB = ctx.iss(tMs + 250);
    var vel = mul(sub(sB, sA), 2);
    var vTot = norm(vel), vRad = dot(vel, u);
    var vTr = Math.sqrt(Math.max(0, vTot * vTot - vRad * vRad));
    var angSpeed = vTr / D; // rad/s
    // Directions on the sky around the satellite
    var up = unit(sub(QB.u, mul(u, dot(QB.u, u))));
    var right = cross(u, up);
    var pole = [0, 0, 1];
    var north = unit(sub(pole, mul(u, dot(pole, u))));
    var east = cross(north, u);
    var m = sub(vel, mul(u, vRad));
    var dirZenith = wrap360(Math.atan2(dot(m, right), dot(m, up)) * RAD);   // clockwise from straight up
    var dirPA = wrap360(Math.atan2(dot(m, east), dot(m, north)) * RAD);     // from north through east
    var satEq = toEquatorOfDate(v, tMs);
    var bodyEq = toEquatorOfDate(sub(b, Q), tMs);
    var sizeArcsec = (ctx.sat.sizeKm / D) * RAD * 3600;
    var out = {
      t: Math.round(tMs * 100) / 100,
      satAlt: satHz.alt, satAz: satHz.az,
      bodyAlt: bodyHz.alt, bodyAz: bodyHz.az,
      rangeKm: D,
      sizeArcsec: sizeArcsec,
      sepDeg: sep * RAD,
      bodyDiamArcmin: 2 * rB * RAD * 60,
      satRaH: satEq.raH, satDecDeg: satEq.decDeg,
      bodyRaH: bodyEq.raH, bodyDecDeg: bodyEq.decDeg,
      parallactic: parallacticTF(bodyEq.raH, bodyEq.decDeg, bodyEq.gastH, Qgeo.lat, Qgeo.lon),
      angSpeedArcminS: angSpeed * RAD * 60,
      vTransverse: vTr, vRadial: vRad, vTotal: vTot,
      dirZenith: dirZenith, dirPA: dirPA,
      isTransit: sep < rB
    };
    var sun = sunForMoon(tMs);
    out.satInShadow = !!rayToGround(s, unit(sub(sun, s)));
    if (ctx.name === 'Moon') {
      out.sunAlt = altAz(Q, QB, sun).alt;
      out.moonSunSepDeg = angleBetween(sub(b, Q), sub(sun, Q)) * RAD;
    }
    return out;
  }

  function refine(ctx, obs, ta, tb) {
    var O = obs.ecef;

    // 1) Sample the centre line every 0.2 s and find where it passes closest to the observer
    var best = null, step = 200;
    for (var t = ta; t <= tb; t += step) {
      var r = ctx.ground(t);
      if (!r) continue;
      var d = norm(sub(r.g, O));
      if (!best || d < best.d) best = { t: t, d: d };
    }
    if (!best) return null;

    var distF = function (tt) {
      var rr = ctx.ground(tt);
      return rr ? norm(sub(rr.g, O)) : 1e9;
    };
    var gm = goldenMin(distF, best.t - step, best.t + step, 40);
    var tc = gm.x;
    var center = ctx.ground(tc);
    if (!center) return null;

    var P = center.g;
    var Pgeo = ecefToGeodetic(P);
    var PB = enuBasis(Pgeo.lat, Pgeo.lon);
    var bodyHz = altAz(P, PB, center.b);
    var issHz = altAz(P, PB, center.iss);
    if (bodyHz.alt <= 0) return null;          // Sun / Moon must be above the horizon

    var distKm = haversineKm(obs.lat, obs.lon, Pgeo.lat, Pgeo.lon);
    if (distKm > SEARCH_RADIUS_KM) return null;

    // 2) Direction of the centre line on the ground
    var gA = ctx.ground(tc - 250), gB = ctx.ground(tc + 250);
    if (!gA || !gB) return null;
    var aGeo = ecefToGeodetic(gA.g), bGeo = ecefToGeodetic(gB.g);
    var track = bearingDeg(aGeo.lat, aGeo.lon, bGeo.lat, bGeo.lon);
    var groundSpeed = haversineKm(aGeo.lat, aGeo.lon, bGeo.lat, bGeo.lon) / 0.5; // km/s

    // 3) Duration at the centre line: time the ISS takes to cross the disc
    var rBody = ctx.bodyRadius(P, tc);
    var crossF = function (tt) { return ctx.sep(P, tt) - rBody; };
    var win = 3000;
    while (win < 60000 && (crossF(tc - win) < 0 || crossF(tc + win) < 0)) win *= 2;
    var t1 = bisect(crossF, tc - win, tc, 45);
    var t2 = bisect(crossF, tc, tc + win, 45);
    var duration = (t2 - t1) / 1000;

    // 4) Path width: walk sideways from the centre line until the ISS
    //    just touches the edge of the disc
    var minSepAt = function (q) {
      return goldenMin(function (tt) { return ctx.sep(q, tt); }, tc - win, tc + win, 45).f;
    };
    var halfWidth = function (side) {
      var brg = (track + side * 90 + 360) % 360;
      var edgeF = function (km) {
        var q = offsetPoint(Pgeo, brg, km);
        return minSepAt(q) - ctx.bodyRadius(q, tc);
      };
      var hi = 1;
      while (hi < 400 && edgeF(hi) < 0) hi *= 2;
      return bisect(edgeF, hi > 1 ? hi / 2 : 0, hi, 30);
    };
    var wRight = halfWidth(+1);
    var wLeft = halfWidth(-1);

    // 5) Drawing of the crossing as seen in the sky (top = straight up)
    var f = unit(sub(center.b, P));
    var upS = unit(sub(PB.u, mul(f, dot(PB.u, f))));
    var rightS = cross(f, upS);
    var d1 = unit(sub(ctx.iss(tc - 200), P)), d2 = unit(sub(ctx.iss(tc + 200), P));
    var m = sub(d2, d1);
    var mx = dot(m, rightS), my = dot(m, upS);
    var ml = Math.sqrt(mx * mx + my * my) || 1;
    mx /= ml; my /= ml;
    // Which side of the drawn path does the "right of track" ground side map to?
    var qR = offsetPoint(Pgeo, (track + 90) % 360, wRight * 0.5);
    var tq = goldenMin(function (tt) { return ctx.sep(qR, tt); }, tc - win, tc + win, 45).x;
    var bq = unit(sub(ctx.body(tq), qR)), iq = unit(sub(ctx.iss(tq), qR));
    var off = sub(iq, bq);
    var ox = dot(off, rightS), oy = dot(off, upS);
    var nx = -my, ny = mx; // 90 degrees counter-clockwise from the motion
    var rightSkySign = (ox * nx + oy * ny) >= 0 ? 1 : -1;
    // Celestial north on the drawing (for telescopes: "north up" view)
    var poleV = [0, 0, 1];
    var northS = unit(sub(poleV, mul(f, dot(poleV, f))));
    var na = dot(northS, rightS), nb = dot(northS, upS);

    // 6) Centre line for the map (with time at each point)
    var line = [];
    var dt = Math.max(40, Math.min(2000, 1000 * 2.0 / Math.max(groundSpeed, 0.5)));
    function pushDir(sign) {
      var pts = [];
      for (var k = 1; k < 800; k++) {
        var tt = tc + sign * k * dt;
        var rr = ctx.ground(tt);
        if (!rr) break;
        var gg = ecefToGeodetic(rr.g);
        if (haversineKm(Pgeo.lat, Pgeo.lon, gg.lat, gg.lon) > LINE_HALF_SPAN_KM) break;
        if (altAz(rr.g, enuBasis(gg.lat, gg.lon), rr.b).alt <= 0) break;
        pts.push([+gg.lat.toFixed(5), +gg.lon.toFixed(5), Math.round(tt - tc)]);
      }
      return pts;
    }
    var before = pushDir(-1).reverse();
    var after = pushDir(+1);
    line = before.concat([[+Pgeo.lat.toFixed(5), +Pgeo.lon.toFixed(5), 0]], after);

    var dIss = norm(sub(center.iss, P));
    var res = {
      sat: ctx.sat.id,
      body: ctx.name,
      t: Math.round(tc),
      lat: Pgeo.lat, lon: Pgeo.lon,
      distKm: distKm,
      bearing: bearingDeg(obs.lat, obs.lon, Pgeo.lat, Pgeo.lon),
      track: track,
      wLeft: wLeft, wRight: wRight,
      duration: duration,
      bodyAlt: bodyHz.alt, bodyAz: bodyHz.az,
      bodyRadiusArcsec: rBody * RAD * 3600,
      satAlt: issHz.alt,
      satRangeKm: dIss,
      satSizeArcsec: (ctx.sat.sizeKm / dIss) * RAD * 3600,
      sky: { mx: mx, my: my, rightSign: rightSkySign, na: na, nb: nb },
      line: line
    };

    if (ctx.name === 'Moon') {
      var at = Astronomy.MakeTime(new Date(tc));
      res.moonIllum = Astronomy.Illumination(Astronomy.Body.Moon, at).phase_fraction;
      res.sunAlt = altAz(P, PB, sunForMoon(tc)).alt;
    }

    // 7) The same numbers at the centre line, and from the observer's own spot
    //    (closest approach as seen from there, like Transit Finder)
    res.centre = viewFrom(ctx, P, PB, Pgeo, tc);
    var sepO = function (tt) { return ctx.sep(O, tt); };
    var bestO = null;
    for (var to = tc - 150000; to <= tc + 150000; to += 250) {
      var sv = sepO(to);
      if (!bestO || sv < bestO.s) bestO = { t: to, s: sv };
    }
    var gO = goldenMin(sepO, bestO.t - 250, bestO.t + 250, 40);
    res.you = viewFrom(ctx, O, obs.basis, { lat: obs.lat, lon: obs.lon }, gO.x);
    if (res.you && res.you.isTransit) {
      var rO = ctx.bodyRadius(O, gO.x);
      var cO = function (tt) { return ctx.sep(O, tt) - rO; };
      res.you.duration = (bisect(cO, gO.x, gO.x + win, 45) - bisect(cO, gO.x - win, gO.x, 45)) / 1000;
    }
    return res;
  }

  var sunForMoon = null; // set in compute()

  // ---------- Main search ----------
  // params.sats = [{ id, sizeKm, satrec | omm }, ...]
  function compute(params, onProgress) {
    var obs = {
      lat: params.lat, lon: params.lon,
      ecef: geodeticToEcef(params.lat, params.lon, (params.heightM || 0) / 1000)
    };
    obs.basis = enuBasis(obs.lat, obs.lon);

    var t0 = params.start || Date.now();
    var t1 = t0 + (params.days || 7) * 86400000;
    var bodies = {
      Sun: makeBody('Sun', t0 - GRID_STEP_MS, t1 + GRID_STEP_MS),
      Moon: makeBody('Moon', t0 - GRID_STEP_MS, t1 + GRID_STEP_MS)
    };
    sunForMoon = bodies.Sun;
    var sats = params.sats || [];
    var all = [];
    for (var k = 0; k < sats.length; k++) {
      var share = 1 / sats.length;
      var base = k * share;
      var res = [];
      try {
        res = searchOne(sats[k], obs, bodies, t0, t1, function (p) {
          if (onProgress) onProgress(base + p * share);
        });
      } catch (e) { res = []; }
      all = all.concat(res);
    }
    all.sort(function (a, b) { return a.t - b.t; });
    return all;
  }

  function searchOne(sat, obs, bodies, t0, t1, onProgress) {
    var iss = makeIss(sat);
    onProgress(0.02);

    // Coarse search, stage A: every 20 s, find when the satellite is (nearly) above the horizon
    var windows = [];
    var inWin = false, wStart = 0;
    var STEP_A = 20000;
    var total = t1 - t0, lastReport = 0;
    for (var t = t0; t <= t1; t += STEP_A) {
      var p = iss(t);
      var up = false;
      if (p) up = altAz(obs.ecef, obs.basis, p).alt > -12;
      if (up && !inWin) { inWin = true; wStart = t - 2 * STEP_A; }
      if (!up && inWin) { inWin = false; windows.push([wStart, t + STEP_A]); }
      if (t - lastReport > total / 40) { lastReport = t; onProgress(0.02 + 0.43 * (t - t0) / total); }
    }
    if (inWin) windows.push([wStart, t1]);

    // Coarse search, stage B: every second inside those windows, look for moments
    // when the satellite and the Sun/Moon are close in the sky and both above the horizon.
    // "Close" is generous: moving 200 km on the ground shifts the satellite in the sky
    // by up to 200 km / (distance to the satellite).
    var candidates = [];
    var names = ['Sun', 'Moon'];
    var winTotal = windows.reduce(function (s, w) { return s + (w[1] - w[0]); }, 0) || 1;
    var winDone = 0;
    for (var wi = 0; wi < windows.length; wi++) {
      var w = windows[wi];
      var run = { Sun: null, Moon: null };
      for (var tt = w[0]; tt <= w[1]; tt += 1000) {
        var ip = iss(tt);
        for (var bi = 0; bi < 2; bi++) {
          var nm = names[bi];
          var hit = false;
          if (ip) {
            var bp = bodies[nm](tt);
            var bh = altAz(obs.ecef, obs.basis, bp);
            if (bh.alt > -3) {
              var toIss = sub(ip, obs.ecef);
              var D = norm(toIss);
              var sep = angleBetween(toIss, sub(bp, obs.ecef));
              var thr = Math.min(Math.PI / 2, (SEARCH_RADIUS_KM * 1.3 + 10) / D + 0.02);
              hit = sep < thr;
            }
          }
          if (hit) {
            if (!run[nm]) run[nm] = [tt, tt]; else run[nm][1] = tt;
          } else if (run[nm]) {
            candidates.push({ body: nm, ta: run[nm][0] - 3000, tb: run[nm][1] + 3000 });
            run[nm] = null;
          }
        }
      }
      for (var bj = 0; bj < 2; bj++) {
        if (run[names[bj]]) candidates.push({ body: names[bj], ta: run[names[bj]][0] - 3000, tb: run[names[bj]][1] + 3000 });
      }
      winDone += w[1] - w[0];
      onProgress(0.45 + 0.35 * winDone / winTotal);
    }

    // Fine refinement of every candidate
    var out = [];
    for (var ci = 0; ci < candidates.length; ci++) {
      var c = candidates[ci];
      var ctx = new Ctx(iss, bodies[c.body], c.body, sat);
      var r = null;
      try { r = refine(ctx, obs, c.ta, c.tb); } catch (e) { r = null; }
      if (r && r.t >= t0 && r.t <= t1) {
        var dup = out.some(function (o) { return o.body === r.body && Math.abs(o.t - r.t) < 60000; });
        if (!dup) out.push(r);
      }
      onProgress(0.8 + 0.2 * (ci + 1) / candidates.length);
    }
    return out;
  }

  return {
    compute: compute,
    _internal: {
      geodeticToEcef: geodeticToEcef, ecefToGeodetic: ecefToGeodetic, makeIss: makeIss,
      makeBody: makeBody, Ctx: Ctx, angleBetween: angleBetween, sub: sub
    }
  };
})();

if (SFT_IS_WORKER) {
  self.onmessage = function (ev) {
    var msg = ev.data || {};
    if (msg.type !== 'compute') return;
    try {
      var lastSent = -1;
      var results = SFT.compute(msg.params, function (p) {
        var pct = Math.round(p * 100);
        if (pct !== lastSent) { lastSent = pct; self.postMessage({ type: 'progress', id: msg.id, value: p }); }
      });
      self.postMessage({ type: 'done', id: msg.id, results: results });
    } catch (err) {
      self.postMessage({ type: 'error', id: msg.id, message: String(err && err.message || err) });
    }
  };
}

if (typeof module !== 'undefined' && module.exports) module.exports = SFT;