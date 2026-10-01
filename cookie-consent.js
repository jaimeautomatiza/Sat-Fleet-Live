/*!
 * SatFleet Live — shared cookie consent + Google Analytics loader.
 * Include ONCE per page, as high in <head> as possible:
 *     <script src="/cookie-consent.js"></script>
 * and remove any other Google tag / gtag snippet or cookie banner from that page.
 *
 * - Analytics (GA4) loads ONLY if localStorage.cookieConsent === 'yes'.
 * - Visitors who haven't chosen yet see the banner; the choice is shared by every page.
 * - window.openCookieSettings() reopens the banner (use it for a "Cookie settings" footer link).
 * - Safe to include on a page that still has its own banner/GA code: it won't duplicate either.
 */
(function () {
  'use strict';

  var GA_ID = 'G-THHVVZCJ4B';
  var KEY = 'cookieConsent';          // 'yes' | 'no' (same key the site already uses)
  var PRIVACY_URL = '/privacy-policy';

  // Keeps page code like `if (typeof gtag === 'function') gtag('event', ...)` working.
  window.dataLayer = window.dataLayer || [];
  if (typeof window.gtag !== 'function') {
    window.gtag = function () { window.dataLayer.push(arguments); };
  }

  function read() {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  }
  function write(v) {
    try { localStorage.setItem(KEY, v); } catch (e) { /* private mode: choice lasts for this page only */ }
  }

  function loadGA() {
    if (window.__satfleetGaLoaded) return;
    if (document.querySelector('script[src*="googletagmanager.com/gtag/js"]')) { window.__satfleetGaLoaded = true; return; }
    window.__satfleetGaLoaded = true;
    window['ga-disable-' + GA_ID] = false;
    var s = document.createElement('script');
    s.async = true;
    s.src = 'https://www.googletagmanager.com/gtag/js?id=' + GA_ID;
    document.head.appendChild(s);
    window.gtag('js', new Date());
    window.gtag('config', GA_ID);
  }

  function clearAnalyticsCookies() {
    window['ga-disable-' + GA_ID] = true;
    var host = location.hostname.split('.');
    var domains = ['', location.hostname];
    if (host.length > 1) domains.push('.' + host.slice(-2).join('.'));
    document.cookie.split(';').forEach(function (c) {
      var name = c.split('=')[0].trim();
      if (name === '_ga' || name.indexOf('_ga_') === 0 || name === '_gid') {
        domains.forEach(function (d) {
          document.cookie = name + '=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/' + (d ? '; domain=' + d : '');
        });
      }
    });
  }

  function hideBanner() {
    var b = document.getElementById('cookieBanner');
    if (b) b.style.display = 'none';
  }

  function choose(value) {
    write(value);
    hideBanner();
    if (value === 'yes') loadGA(); else clearAnalyticsCookies();
  }

  function buildBanner() {
    var b = document.createElement('div');
    b.id = 'cookieBanner';
    b.setAttribute('role', 'dialog');
    b.setAttribute('aria-label', 'Cookie consent');
    b.style.cssText = 'display:flex;position:fixed;bottom:0;left:0;right:0;' +
      'background:rgba(15,15,15,0.97);-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px);' +
      'color:#fff;padding:12px 20px;align-items:center;justify-content:space-between;gap:12px;' +
      'z-index:2147483000;font:0.8rem/1.5 "Segoe UI",system-ui,-apple-system,sans-serif;' +
      'border-top:1px solid rgba(156,39,176,0.3);flex-wrap:wrap;';

    var text = document.createElement('span');
    text.style.color = 'rgba(255,255,255,0.75)';
    text.appendChild(document.createTextNode('We use analytics cookies to improve the experience. '));
    var a = document.createElement('a');
    a.href = PRIVACY_URL;
    a.textContent = 'Privacy Policy';
    a.style.color = '#c06ad1';
    text.appendChild(a);

    var btns = document.createElement('div');
    btns.style.cssText = 'display:flex;gap:8px;flex-shrink:0;';
    // Both buttons deliberately share the same visual weight (rejecting must be as easy as accepting).
    var style = 'padding:7px 18px;background:transparent;border:1.5px solid #9c27b0;color:#fff;' +
      'border-radius:19px;cursor:pointer;font:600 0.78rem "Segoe UI",system-ui,sans-serif;';
    [['Reject', 'no'], ['Accept', 'yes']].forEach(function (p) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = p[0];
      btn.style.cssText = style;
      btn.addEventListener('click', function () { choose(p[1]); });
      btns.appendChild(btn);
    });

    b.appendChild(text);
    b.appendChild(btns);
    return b;
  }

  function showBanner() {
    var existing = document.getElementById('cookieBanner');
    if (existing && !existing.__satfleet) { return; } // page still has its own banner: leave it alone
    if (existing) { existing.style.display = 'flex'; return; }
    var b = buildBanner();
    b.__satfleet = true;
    document.body.appendChild(b);
  }

  // Reopen the banner so a visitor can change their mind (link it from the footer).
  window.openCookieSettings = function () {
    var existing = document.getElementById('cookieBanner');
    if (existing && !existing.__satfleet) { try { localStorage.removeItem(KEY); } catch (e) {} location.reload(); return; }
    showBanner();
  };

  // Compatibility with pages that still call the old inline handlers.
  if (typeof window.acceptCookies !== 'function') window.acceptCookies = function () { choose('yes'); };
  if (typeof window.rejectCookies !== 'function') window.rejectCookies = function () { choose('no'); };

  var consent = read();
  if (consent === 'yes') loadGA();

  function init() { if (consent !== 'yes' && consent !== 'no') showBanner(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();