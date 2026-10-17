/* SatFleet Live — unidades (km / millas)
 *
 * Por defecto todo sale en km y km/h. El usuario puede cambiar a millas y mph
 * desde el menú ⋯ ("Units") o tocando cualquier altitud, velocidad o distancia.
 * La elección se guarda en el navegador y se aplica en todos los mapas.
 *
 * Por dentro todo se sigue calculando en km: esto solo cambia lo que se ve.
 */
(function () {
    'use strict';

    var KEY = 'sfl-units';
    var KM_TO_MI = 0.621371;

    function read() {
        try { return localStorage.getItem(KEY) === 'imperial' ? 'imperial' : 'metric'; }
        catch (e) { return 'metric'; }
    }
    var system = read();

    function isImperial() { return system === 'imperial'; }

    function fmt(value, decimals) {
        if (decimals == null) return Math.round(value).toLocaleString();
        return value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    }

    // Distancia: recibe km, devuelve "12,345 km" o "7,671 mi"
    function dist(km, decimals) {
        if (km == null || isNaN(km)) return '—';
        return isImperial() ? fmt(km * KM_TO_MI, decimals) + ' mi' : fmt(km, decimals) + ' km';
    }

    // Velocidad: recibe km/h, devuelve "27,600 km/h" o "17,150 mph"
    function speed(kmh, decimals) {
        if (kmh == null || isNaN(kmh)) return '—';
        return isImperial() ? fmt(kmh * KM_TO_MI, decimals) + ' mph' : fmt(kmh, decimals) + ' km/h';
    }

    // Millones de km o de millas, para distancias muy grandes ("≈ 228 million km")
    function millions(km) {
        var v = (isImperial() ? km * KM_TO_MI : km) / 1000000;
        return v.toLocaleString(undefined, { maximumFractionDigits: 0 }) + (isImperial() ? ' million mi' : ' million km');
    }

    // Escribe el valor en un elemento y lo recuerda, para poder repintarlo
    // al cambiar de unidades aunque esa parte de la página no se actualice sola.
    function show(el, kind, value, decimals) {
        if (!el) return;
        var text = kind === 'speed' ? speed(value, decimals) : dist(value, decimals);
        el.textContent = text;
        el.setAttribute('data-sfl-kind', kind);
        el.setAttribute('data-sfl-val', String(value));
        el.setAttribute('data-sfl-dec', decimals == null ? '' : String(decimals));
        el.setAttribute('data-sfl-text', text);
        el.classList.add('sfl-unit');
        el.title = isImperial() ? 'Tap to switch to km' : 'Tap to switch to miles';
    }

    function refreshAll() {
        var els = document.querySelectorAll('[data-sfl-kind]');
        for (var i = 0; i < els.length; i++) {
            var el = els[i];
            // Solo repintamos si nadie ha cambiado el texto desde que lo escribimos
            if (el.textContent !== el.getAttribute('data-sfl-text')) continue;
            var dec = el.getAttribute('data-sfl-dec');
            show(el, el.getAttribute('data-sfl-kind'), parseFloat(el.getAttribute('data-sfl-val')), dec === '' ? null : +dec);
        }
        var labels = document.querySelectorAll('.sfl-units-label');
        for (var j = 0; j < labels.length; j++) {
            labels[j].textContent = isImperial() ? 'Units: miles, mph' : 'Units: km, km/h';
        }
    }

    function set(next) {
        system = next === 'imperial' ? 'imperial' : 'metric';
        try { localStorage.setItem(KEY, system); } catch (e) {}
        refreshAll();
        try { document.dispatchEvent(new CustomEvent('sfl-units-change', { detail: system })); } catch (e) {}
        if (typeof gtag === 'function') { try { gtag('event', 'units_change', { units: system }); } catch (e) {} }
    }

    function toggle() { set(isImperial() ? 'metric' : 'imperial'); }

    window.SFUnits = {
        isImperial: isImperial,
        dist: dist,
        speed: speed,
        millions: millions,
        show: show,
        set: set,
        toggle: toggle,
        refresh: refreshAll
    };

    // Si cambia en otra pestaña, lo aplicamos aquí también
    window.addEventListener('storage', function (e) {
        if (e.key === KEY) { system = read(); refreshAll(); }
    });

    // Tocar cualquier valor con unidades cambia el sistema
    document.addEventListener('click', function (e) {
        var t = e.target;
        if (t && t.closest && t.closest('.sfl-unit')) toggle();
    });

    // Pequeña pista visual: subrayado de puntos en los valores que se pueden tocar
    function injectStyle() {
        var s = document.createElement('style');
        s.textContent = '.sfl-unit{cursor:pointer;text-decoration:underline dotted;text-decoration-color:rgba(150,150,170,.55);text-underline-offset:3px}';
        document.head.appendChild(s);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () { injectStyle(); refreshAll(); });
    } else {
        injectStyle(); refreshAll();
    }
})();