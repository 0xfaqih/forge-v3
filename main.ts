import { createServer } from 'node:http';

const envUrl = process.env.MAKEFUN_ENV_URL ?? '';
const server = createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end('{"message":"not found"}');
});

server.listen(0, '0.0.0.0', async () => {
  const address = server.address();
  const port = address && typeof address === 'object' ? address.port : 0;
  try {
    await fetch(`${envUrl}/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ port }),
    });
  } catch (err) {
    process.stderr.write(`register failed: ${String(err)}\n`);
  }
  for (;;) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    try {
      const res = await fetch(`${envUrl}/done`);
      const body: any = await res.json();
      if (body.done === true) {
        server.close();
        process.exit(0);
      }
    } catch {
      // Keep serving while the suite is running.
    }
  }
});
