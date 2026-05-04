// satworker.js — VERSIÓN OPTIMIZADA
importScripts('js/satellite.min.js');

let satrecList = [];
let positionsBuf;

self.onmessage = function(e) {
    const { type, payload } = e.data;

    if (type === 'INIT') {
        satrecList = [];
        for (const obj of payload.gpData) {
            try {
                const satrec = satellite.json2satrec(obj);
                satrecList.push({
                    name:    obj.OBJECT_NAME,
                    noradId: String(obj.NORAD_CAT_ID),
                    satrec
                });
            } catch(e) {}
        }
        positionsBuf = new Float64Array(satrecList.length * 4);
        self.postMessage({ type: 'READY', count: satrecList.length });
        setInterval(() => calcAndSend(), 2000);

    } else if (type === 'CALC') {
        calcAndSend();
    }
};

function calcAndSend() {
    const now       = new Date();
    const timestamp = now.getTime();
    // ← CLAVE: gstime se calcula UNA sola vez para todos los satélites
    const gmst      = satellite.gstime(now);

    for (let i = 0; i < satrecList.length; i++) {
        const sat = satrecList[i];
        let lat = 0, lng = 0, alt = -1000, vel = 0;

        if (sat && sat.satrec) {
            try {
                const pv = satellite.propagate(sat.satrec, now);
                if (pv && pv.position && pv.velocity) {
                    const geo     = satellite.eciToGeodetic(pv.position, gmst);
                    const calcLat = satellite.degreesLat(geo.latitude);
                    const calcLng = satellite.degreesLong(geo.longitude);

                    if (!isNaN(calcLat) && !isNaN(calcLng) && geo.height > 0) {
                        lat = calcLat;
                        lng = calcLng;
                        alt = geo.height;
                        vel = Math.sqrt(
                            pv.velocity.x**2 + pv.velocity.y**2 + pv.velocity.z**2
                        ) * 3600;
                    }
                }
            } catch(e) {}
        }

        positionsBuf[i * 4]     = lat;
        positionsBuf[i * 4 + 1] = lng;
        positionsBuf[i * 4 + 2] = alt;
        positionsBuf[i * 4 + 3] = vel;
    }

    self.postMessage({ type: 'POSITIONS', buf: positionsBuf, timestamp });
}