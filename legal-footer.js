/* SatFleet Live - footer legal unico. Cambia aqui y cambia en todas las paginas. */
(function () {
  if (window.__legalFooterLoaded) return;
  window.__legalFooterLoaded = true;

  var LINKS = [
    ['Legal Notice', '/legal-notice'],
    ['Terms of Service', '/terms-of-service'],
    ['Privacy Policy', '/privacy-policy'],
    ['Cookie settings', '#cookies'],
    ['Contact', 'mailto:jaime.automatiza@gmail.com']
  ];

  function el(tag, props, text) {
    var e = document.createElement(tag);
    for (var k in props) e.setAttribute(k, props[k]);
    if (text) e.textContent = text;
    return e;
  }
  function sep() { return el('span', { 'class': 'footer-sep' }, '|'); }

  function init() {
    var css = el('style', { id: 'legal-footer-base-css' });
    css.textContent =
      '#legalFooter{position:fixed;bottom:0;left:0;right:0;height:22px;display:flex;align-items:center;justify-content:center;gap:8px;font-size:.62rem;color:rgba(255,255,255,.4);background:rgba(10,10,15,.82);-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);border-top:1px solid rgba(255,255,255,.06);z-index:400;white-space:nowrap}' +
      '#legalFooter a,#legalFooter button{color:rgba(125,184,255,.8);text-decoration:none;background:none;border:none;font:inherit;padding:0;cursor:pointer}' +
      '#legalPanel{position:fixed;left:50%;bottom:calc(30px + env(safe-area-inset-bottom,0px));transform:translateX(-50%);width:min(92vw,340px);max-height:70vh;overflow:auto;background:rgba(18,18,24,.96);color:#e8e8ee;border:1px solid rgba(255,255,255,.12);border-radius:12px;padding:10px 16px 12px;font:13px/1.4 system-ui,-apple-system,Segoe UI,sans-serif;z-index:100000;box-shadow:0 8px 30px rgba(0,0,0,.5)}' +
      '#legalPanel[hidden]{display:none}' +
      '#legalPanel a,#legalPanel .lp-link{display:block;width:100%;padding:7px 0;color:#7db8ff;text-decoration:none;background:none;border:0;font:inherit;text-align:left;cursor:pointer}' +
      '#legalPanel h4{margin:10px 0 2px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;opacity:.55;font-weight:600}';
    document.head.appendChild(css);

    var bar = document.getElementById('infoFooter') || document.getElementById('legalFooter');
    var sources = [], primary = [], about = null;

    if (bar) {
      Array.prototype.forEach.call(bar.querySelectorAll('a[href^="http"]'), function (a) {
        if (/satfleetlive\.com/.test(a.href)) return;
        sources.push({ t: a.textContent.trim(), h: a.href });
        if (!a.classList.contains('footer-secondary')) primary.push(a.cloneNode(true));
      });
      about = bar.querySelector('button[onclick*="toggleAboutPanel"]');
      if (about) about = about.cloneNode(true);
      bar.innerHTML = '';
    } else {
      bar = el('div', { id: 'legalFooter' });
      document.body.appendChild(bar);
      document.body.style.paddingBottom = '22px';
    }

    if (primary.length) {
      bar.appendChild(el('span', {}, 'Source:'));
      primary.forEach(function (a, i) {
        if (i) bar.appendChild(el('span', {}, '/'));
        bar.appendChild(a);
      });
      bar.appendChild(sep());
    }
    if (about) { bar.appendChild(about); bar.appendChild(sep()); }
    var btn = el('button', { type: 'button', 'class': 'about-link', 'aria-haspopup': 'dialog' }, 'Legal');
    bar.appendChild(btn);

    var panel = el('div', { id: 'legalPanel', role: 'dialog', 'aria-label': 'Legal' });
    panel.hidden = true;
    LINKS.forEach(function (l) {
      if (l[1] === '#cookies') {
        var b = el('button', { type: 'button', 'class': 'lp-link' }, l[0]);
        b.onclick = function () {
          panel.hidden = true;
          if (typeof openCookieSettings === 'function') openCookieSettings();
        };
        panel.appendChild(b);
      } else {
        panel.appendChild(el('a', { href: l[1] }, l[0]));
      }
    });
    if (sources.length) {
      panel.appendChild(el('h4', {}, 'Sources'));
      sources.forEach(function (s) {
        panel.appendChild(el('a', { href: s.h, target: '_blank', rel: 'noopener' }, s.t));
      });
    }
    document.body.appendChild(panel);

    btn.onclick = function (e) { e.stopPropagation(); panel.hidden = !panel.hidden; };
    document.addEventListener('click', function (e) {
      if (!panel.hidden && !panel.contains(e.target)) panel.hidden = true;
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') panel.hidden = true;
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
