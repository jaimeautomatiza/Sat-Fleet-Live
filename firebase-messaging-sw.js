importScripts('https://www.gstatic.com/firebasejs/10.8.1/firebase-app-compat.js');

firebase.initializeApp({
  apiKey: "AIzaSyCqQcTt6gK_220ZMbDIK9jSuOhCVLTZn4I",
  authDomain: "satfleet-live.firebaseapp.com",
  projectId: "satfleet-live",
  storageBucket: "satfleet-live.firebasestorage.app",
  messagingSenderId: "369952976257",
  appId: "1:369952976257:web:c7ba2050865524fca80c80"
});

// Escuchamos el aviso "en crudo" directamente, sin pasar por
// messaging.onBackgroundMessage() — así evitamos el fallo conocido de
// Firebase que duplica la notificación en algunos casos.
self.addEventListener('push', (event) => {
  if (!event.data) return;
  const payload = event.data.json();
  const data = payload.data || {};

  const title = data.title || 'SatFleet Live';
  const body  = data.body || '';
  const url   = data.url || 'https://satfleetlive.com';

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      icon: 'https://satfleetlive.com/images/logo.png',
      badge: 'https://satfleetlive.com/images/logo.png',
      data: { url },
    })
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || 'https://satfleetlive.com';
  event.waitUntil(
    clients.matchAll({ type: 'window' }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes(self.location.origin) && 'focus' in client) return client.focus();
      }
      return clients.openWindow(url);
    })
  );
});