self.addEventListener('fetch', function(event) {
  var url = event.request.url;
  if (event.request.method !== 'GET') return;
  // Only intercept the main dashboard page
  if (!url.match(/localhost:8000\/(\?|$)/)) return;

  event.respondWith(
    fetch(event.request).then(function(response) {
      return response.text().then(function(html) {
        var patched = html.replace('</body>',
          '<script src="/static/mode-patch.js?v=2"><\/script></body>');
        return new Response(patched, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers
        });
      });
    })
  );
});
