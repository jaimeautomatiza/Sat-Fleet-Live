importScripts('https://www.gstatic.com/firebasejs/10.8.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.8.1/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: "AIzaSyCqQcTt6gK_220ZMbDIK9jSuOhCVLTZn4I",
  authDomain: "satfleet-live.firebaseapp.com",
  projectId: "satfleet-live",
  storageBucket: "satfleet-live.firebasestorage.app",
  messagingSenderId: "369952976257",
  appId: "1:369952976257:web:c7ba2050865524fca80c80"
});

const messaging = firebase.messaging();

// Se dispara cuando llega un aviso y la pestaña NO está abierta/activa

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