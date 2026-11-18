// Raw TCP HTTP/1.1 server for forge-v3: zero-dependency, keep-alive, pipelined-safe.
// Replaces node:http to remove its per-request allocation overhead on the hot paths.

import { createServer as createTcpServer, type Socket } from 'node:net';

export interface RawRequest {
  method: string;
  target: string;
  headers: Map<string, string>;
  body: Buffer;
  socket: Socket;
}

type Handler = (req: RawRequest) => void | Promise<void>;

const STATUS_TEXT: Record<number, string> = {
  200: 'OK', 201: 'Created', 204: 'No Content', 400: 'Bad Request',
  401: 'Unauthorized', 404: 'Not Found', 405: 'Method Not Allowed',
  409: 'Conflict', 422: 'Unprocessable Entity', 500: 'Internal Server Error',
};

const DATE_CACHE = { value: '', second: -1 };
function httpDate(): string {
  const second = Math.floor(Date.now() / 1000);
  if (second !== DATE_CACHE.second) {
    DATE_CACHE.second = second;
    DATE_CACHE.value = new Date(second * 1000).toUTCString();
  }
  return DATE_CACHE.value;
}

export function sendRawResponse(socket: Socket, status: number, contentType: string | null, body: Buffer | string, extra = ''): void {
  const text = typeof body === 'string' ? body : null;
  const bytes = text === null ? (body as Buffer) : null;
  const length = text === null ? (bytes as Buffer).length : Buffer.byteLength(text as string);
  let head = `HTTP/1.1 ${status} ${STATUS_TEXT[status] ?? 'OK'}\r\ncontent-length: ${length}\r\ndate: ${httpDate()}\r\nconnection: keep-alive\r\n`;
  if (contentType !== null) head += `content-type: ${contentType}\r\n`;
  if (extra) head += extra;
  head += '\r\n';
  if (text === null) {
    socket.write(head);
    socket.write(bytes as Buffer);
  } else {
    socket.write(head + (text as string));
  }
}

export function sendJsonResponse(socket: Socket, status: number, text: string, extra = ''): void {
  const requestId = `x-github-request-id: req-${++sendJsonResponse.counter}\r\n`;
  sendRawResponse(socket, status, 'application/json; charset=utf-8', text, `x-oauth-scopes: repo, read:org\r\n${requestId}${extra}`);
}
sendJsonResponse.counter = 0;

function parseHeaders(block: string): Map<string, string> | null {
  const lines = block.split('\r\n');
  const first = lines[0] ?? '';
  if (!/^[A-Z]+ \S+ HTTP\/1\.[01]$/.test(first)) return null;
  const headers = new Map<string, string>();
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i] ?? '';
    if (line === '') continue;
    const colon = line.indexOf(':');
    if (colon < 0) return null;
    headers.set(line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim());
  }
  return headers;
}

interface ConnectionState {
  socket: Socket;
  buffer: Buffer;
  handler: Handler;
  alive: boolean;
}

function drain(state: ConnectionState): void {
  const { socket, handler } = state;
  for (;;) {
    const headerEnd = state.buffer.indexOf('\r\n\r\n');
    if (headerEnd < 0) return;
    const block = state.buffer.toString('latin1', 0, headerEnd);
    const headers = parseHeaders(block);
    if (headers === null) {
      socket.destroy();
      return;
    }
    const firstLine = block.slice(0, block.indexOf('\r\n'));
    const space1 = firstLine.indexOf(' ');
    const space2 = firstLine.indexOf(' ', space1 + 1);
    const method = firstLine.slice(0, space1);
    const target = firstLine.slice(space1 + 1, space2);
    let contentLength = 0;
    const rawLength = headers.get('content-length');
    if (rawLength !== undefined) {
      contentLength = parseInt(rawLength, 10);
      if (!Number.isFinite(contentLength) || contentLength < 0 || contentLength > 64 * 1024 * 1024) {
        socket.destroy();
        return;
      }
    }
    const total = headerEnd + 4 + contentLength;
    if (state.buffer.length < total) return;
    const body = state.buffer.subarray(headerEnd + 4, total);
    state.buffer = state.buffer.subarray(total);
    const req: RawRequest = { method, target, headers, body, socket };
    try {
      const result = handler(req);
      if (result instanceof Promise) {
        state.alive = false;
        void result.then(
          () => { state.alive = true; drain(state); },
          () => { try { socket.destroy(); } catch { /* closed */ } },
        );
        return;
      }
    } catch {
      socket.destroy();
      return;
    }
    if (!state.alive) return;
  }
}

export function serveRaw(handler: Handler, port: number, host: string, onListen: (port: number) => void): void {
  const server = createTcpServer((socket: Socket) => {
    socket.setNoDelay(true);
    const state: ConnectionState = { socket, buffer: Buffer.alloc(0), handler, alive: true };
    socket.on('data', (chunk: Buffer) => {
      state.buffer = state.buffer.length ? Buffer.concat([state.buffer, chunk]) : chunk;
      if (state.alive) drain(state);
    });
    socket.on('error', () => { /* client gone */ });
  });
  server.on('error', (err) => { process.stderr.write(`tcp server error: ${String(err)}\n`); });
  server.listen(port, host, () => {
    const address = server.address();
    onListen(address && typeof address === 'object' ? address.port : 0);
  });
}
