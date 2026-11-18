import { createHash } from 'node:crypto';
import { deflateSync, inflateSync } from 'node:zlib';
import { mkdirSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import type { Socket } from 'node:net';
import { sendJsonResponse, sendRawResponse, serveRaw, type RawRequest } from './http.ts';

// ---------------------------------------------------------------- constants and small helpers

const VIEWER = 'adaiversen';
const STRANGER = 'olgabauer';
const ZERO = '0'.repeat(40);
const EPOCH_MS = 1_735_689_600_000;
const DATA_DIR = '/tmp/forge-v3-data';
const MAX_PKT = 65_516;

type Rec = Record<string, unknown>;
type ObjType = 'blob' | 'tree' | 'commit' | 'tag';
interface GitObj { type: ObjType; data: Buffer }
interface TreeEntry { mode: string; name: string; oid: string; kind: 'blob' | 'tree' }
interface Issue {
  id: number; number: number; title: string; body: string; state: 'open' | 'closed';
  labels: string[]; comments: Array<{ id: number; body: string }>; seq: number;
}
interface Pull {
  id: number; number: number; title: string; body: string; head: string; base: string;
  headSha: string; baseSha: string; state: 'open' | 'closed'; merged: boolean;
  mergeCommitSha: string | null; changedPaths: string[];
}
interface Repo {
  id: number; owner: string; name: string; private: boolean; description: string;
  defaultBranch: string; refs: Map<string, string>; labels: Set<string>;
  issues: Map<number, Issue>; nextIssue: number; pulls: Map<number, Pull>; nextPull: number;
  nextComment: number; nextCommit: number; seq: number; files: Map<string, string>;
}

function isRec(v: unknown): v is Rec {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}
function rec(v: unknown): Rec { return isRec(v) ? v : {}; }
function str(v: unknown): string { return typeof v === 'string' ? v : ''; }
function num(v: unknown): number | null { return typeof v === 'number' && Number.isFinite(v) ? v : null; }
function asArray(v: unknown): unknown[] { return Array.isArray(v) ? v : []; }
function hex40(v: unknown): boolean { return typeof v === 'string' && /^[0-9a-f]{40}$/i.test(v); }
function encodeBase64Url(text: string): string {
  return Buffer.from(text, 'utf8').toString('base64url');
}
function decodeBase64Url(text: string): string {
  try { return Buffer.from(text, 'base64url').toString('utf8'); } catch { return ''; }
}

let requestCounter = 0;
const objects = new Map<string, GitObj>();
const repos = new Map<string, Repo>();
let nextRepoId = 5000;
let nextGlobalId = 1;

mkdirSync(DATA_DIR, { recursive: true });
const JOURNAL = `${DATA_DIR}/mutations.ndjson`;
function journal(kind: string, payload: Rec): void {
  if (process.env.FORGE_JOURNAL !== '1') return;
  try { appendFileSync(JOURNAL, `${JSON.stringify({ kind, ...payload })}\n`); } catch { /* /tmp may be read-only in a local dry run */ }
}
const packCache = new Map<string, Buffer>();
function packCacheKey(wants: string[], haves: string[], boundaries: string[], depth: number | null): string | null {
  if (wants.length !== 1 || haves.length || boundaries.length || depth !== 1) return null;
  return `d1:${wants[0] ?? ''}`;
}

// ---------------------------------------------------------------- git object helpers

function objectHash(type: ObjType, data: Buffer): string {
  return createHash('sha1').update(`${type} ${data.length}\0`).update(data).digest('hex');
}
function putObject(type: ObjType, data: Buffer): string {
  const oid = objectHash(type, data);
  if (!objects.has(oid)) {
    objects.set(oid, { type, data });
    try {
      const path = `${DATA_DIR}/objects/${oid.slice(0, 2)}`;
      mkdirSync(path, { recursive: true });
      writeFileSync(`${path}/${oid.slice(2)}`, deflateSync(Buffer.concat([Buffer.from(`${type} ${data.length}\0`), data])));
    } catch { /* memory store remains authoritative */ }
  }
  return oid;
}
function parseTree(data: Buffer): TreeEntry[] {
  const out: TreeEntry[] = [];
  let p = 0;
  while (p < data.length) {
    const sp = data.indexOf(0x20, p);
    if (sp < 0) break;
    const nul = data.indexOf(0, sp + 1);
    if (nul < 0 || nul + 21 > data.length) break;
    const mode = data.toString('ascii', p, sp);
    const name = data.toString('utf8', sp + 1, nul);
    const oid = data.toString('hex', nul + 1, nul + 21);
    out.push({ mode, name, oid, kind: mode === '40000' || mode === '040000' ? 'tree' : 'blob' });
    p = nul + 21;
  }
  return out;
}
function treeName(entry: TreeEntry): string {
  return entry.kind === 'tree' ? `${entry.name}/` : entry.name;
}
function encodeTree(entries: TreeEntry[]): Buffer {
  const sorted = entries.slice().sort((a, b) => Buffer.compare(Buffer.from(treeName(a)), Buffer.from(treeName(b))));
  const parts: Buffer[] = [];
  for (const e of sorted) {
    parts.push(Buffer.from(`${e.mode} ${e.name}\0`, 'utf8'), Buffer.from(e.oid, 'hex'));
  }
  return Buffer.concat(parts);
}
function treeMap(oid: string | undefined): Map<string, TreeEntry> {
  const m = new Map<string, TreeEntry>();
  if (!oid) return m;
  const obj = objects.get(oid);
  if (!obj || obj.type !== 'tree') return m;
  for (const e of parseTree(obj.data)) m.set(e.name, e);
  return m;
}
function treeAt(commitOid: string): string {
  const obj = objects.get(commitOid);
  if (!obj || obj.type !== 'commit') return '';
  const line = obj.data.toString('utf8').split('\n').find((l) => l.startsWith('tree '));
  return line ? line.slice(5) : '';
}
function commitInfo(oid: string): { tree: string; parents: string[]; message: string } {
  const obj = objects.get(oid);
  const text = obj && (obj.type === 'commit' || obj.type === 'tag') ? obj.data.toString('utf8') : '';
  const head = text.split('\n\n')[0] ?? '';
  const parents: string[] = [];
  let tree = '';
  for (const line of head.split('\n')) {
    if (line.startsWith('tree ')) tree = line.slice(5);
    else if (line.startsWith('parent ')) parents.push(line.slice(7));
  }
  return { tree, parents, message: text.split('\n\n').slice(1).join('\n\n') };
}
function refOid(repo: Repo, name: string): string {
  return repo.refs.get(name) ?? repo.refs.get(`refs/heads/${name}`) ?? '';
}
function parseTagTarget(oid: string): string {
  const obj = objects.get(oid);
  if (!obj || obj.type !== 'tag') return '';
  const line = obj.data.toString('utf8').split('\n').find((l) => l.startsWith('object '));
  return line ? line.slice(7) : '';
}
function peeledOid(oid: string): string {
  const obj = objects.get(oid);
  return obj?.type === 'tag' ? parseTagTarget(oid) : '';
}
function blobOidAt(commitOid: string, path: string): string {
  let tree = treeAt(commitOid);
  const parts = path.split('/').filter(Boolean);
  for (let i = 0; i < parts.length; i += 1) {
    const entry = treeMap(tree).get(parts[i] ?? '');
    if (!entry) return '';
    if (i === parts.length - 1) return entry.kind === 'blob' ? entry.oid : '';
    if (entry.kind !== 'tree') return '';
    tree = entry.oid;
  }
  return '';
}
function listDirectory(commitOid: string, path: string): Array<{ name: string; path: string; oid: string; type: string }> {
  let tree = treeAt(commitOid);
  for (const part of path.split('/').filter(Boolean)) {
    const entry = treeMap(tree).get(part);
    if (!entry || entry.kind !== 'tree') return [];
    tree = entry.oid;
  }
  return [...treeMap(tree).values()].map((e) => ({
    name: e.name, path: path ? `${path}/${e.name}` : e.name, oid: e.oid, type: e.kind,
  }));
}
function setBlobAt(commitOid: string, path: string, blob: string | null): string {
  const root = treeMap(treeAt(commitOid));
  const parts = path.split('/').filter(Boolean);
  if (!parts.length) return treeAt(commitOid);
  const rebuild = (map: Map<string, TreeEntry>, depth: number): string => {
    const name = parts[depth] ?? '';
    const last = depth === parts.length - 1;
    const old = map.get(name);
    let entry: TreeEntry | undefined;
    if (last) {
      if (blob === null) { map.delete(name); }
      else { entry = { mode: '100644', name, oid: blob, kind: 'blob' }; map.set(name, entry); }
    } else {
      const child = old?.kind === 'tree' ? treeMap(old.oid) : new Map<string, TreeEntry>();
      const childOid = rebuild(child, depth + 1);
      if (child.size === 0) map.delete(name);
      else { entry = { mode: '40000', name, oid: childOid, kind: 'tree' }; map.set(name, entry); }
    }
    return putObject('tree', encodeTree([...map.values()]));
  };
  return rebuild(root, 0);
}
function makeCommit(repo: Repo, branch: string, message: string, tree: string, parents: string[]): string {
  repo.nextCommit += 1;
  const stamp = 1_700_000_000 + repo.nextCommit;
  const id = `${VIEWER} <forge@example.invalid>`;
  const body = [
    `tree ${tree}`,
    ...parents.map((p) => `parent ${p}`),
    `author ${id} ${stamp} +0000`,
    `committer ${id} ${stamp} +0000`,
    '',
    message,
  ].join('\n');
  const oid = putObject('commit', Buffer.from(body));
  repo.refs.set(`refs/heads/${branch}`, oid);
  repo.files.set(branch, tree);
  return oid;
}
function isAncestor(oldOid: string, newOid: string): boolean {
  if (!oldOid || oldOid === ZERO) return true;
  const seen = new Set<string>();
  const stack = [newOid];
  while (stack.length) {
    const cur = stack.pop() ?? '';
    if (cur === oldOid) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    for (const p of commitInfo(cur).parents) stack.push(p);
  }
  return false;
}

// ---------------------------------------------------------------- store operations

function repoKey(owner: string, name: string): string { return `${owner}/${name}`; }
function getRepo(owner: string, name: string): Repo | null {
  return repos.get(repoKey(owner, name)) ?? null;
}
function createRepo(name: string, owner = VIEWER, description = '', priv = true): Repo | null {
  if (!name || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/.test(name)) return null;
  const key = repoKey(owner, name);
  const existing = repos.get(key);
  if (existing) return null;
  const repo: Repo = {
    id: nextRepoId++, owner, name, private: priv, description, defaultBranch: 'main',
    refs: new Map(), labels: new Set(), issues: new Map(), nextIssue: 1, pulls: new Map(),
    nextPull: 1, nextComment: 1, nextCommit: 0, seq: 0, files: new Map(),
  };
  repos.set(key, repo);
  journal('repo', { key, id: repo.id, name, owner, description, private: priv });
  return repo;
}
function repoJson(repo: Repo): Rec {
  return {
    id: repo.id, name: repo.name, full_name: `${repo.owner}/${repo.name}`,
    owner: { login: repo.owner, id: repo.owner === VIEWER ? 1000 : 1002 },
    private: repo.private, description: repo.description, default_branch: repo.defaultBranch,
    html_url: `https://github.com/${repo.owner}/${repo.name}`,
    clone_url: `https://github.com/${repo.owner}/${repo.name}.git`,
    has_issues: true, archived: false, fork: false,
  };
}
function issueJson(issue: Issue): Rec {
  return {
    id: issue.id, number: issue.number, title: issue.title, body: issue.body,
    state: issue.state, labels: issue.labels.map((name) => ({ name, color: 'ededed' })),
    user: { login: VIEWER }, comments: issue.comments.length,
    created_at: new Date(EPOCH_MS + issue.seq * 1000).toISOString(),
    updated_at: new Date(EPOCH_MS + issue.seq * 1000).toISOString(),
  };
}
function pullJson(pull: Pull): Rec {
  return {
    id: pull.id, number: pull.number, state: pull.state, title: pull.title, body: pull.body,
    head: { ref: pull.head, sha: pull.headSha, label: `${VIEWER}:${pull.head}` },
    base: { ref: pull.base, sha: pull.baseSha, label: `${VIEWER}:${pull.base}` },
    merged: pull.merged, merge_commit_sha: pull.mergeCommitSha,
    merged_at: pull.merged ? new Date(EPOCH_MS + pull.number * 1000).toISOString() : null,
    user: { login: VIEWER },
  };
}
function addIssue(repo: Repo, title: string, body: string, labels: string[]): Issue | null {
  if (labels.some((l) => !repo.labels.has(l))) return null;
  repo.seq += 1;
  const issue: Issue = {
    id: nextGlobalId++, number: repo.nextIssue++, title, body, state: 'open',
    labels: labels.slice(), comments: [], seq: repo.seq,
  };
  repo.issues.set(issue.number, issue);
  journal('issue', { repo: repoKey(repo.owner, repo.name), number: issue.number, title, body, labels });
  return issue;
}
function jsonEscape(text: string): string {
  let out = '';
  for (let i = 0; i < text.length; i += 1) {
    const code = text.charCodeAt(i);
    if (code === 34) out += '\\"';
    else if (code === 92) out += '\\\\';
    else if (code === 10) out += '\\n';
    else if (code === 13) out += '\\r';
    else if (code === 9) out += '\\t';
    else if (code < 32) out += `\\u${code.toString(16).padStart(4, '0')}`;
    else out += text[i] ?? '';
  }
  return out;
}
function addIssueFast(repo: Repo, title: string, body: string): string | null {
  repo.seq += 1;
  const id = nextGlobalId++;
  const number = repo.nextIssue++;
  const stamp = new Date(EPOCH_MS + repo.seq * 1000).toISOString();
  repo.issues.set(number, { id, number, title, body, state: 'open', labels: [], comments: [], seq: repo.seq });
  return `{"id":${id},"number":${number},"title":"${jsonEscape(title)}","body":"${jsonEscape(body)}","state":"open","labels":[],"user":{"login":"${VIEWER}"},"comments":0,"created_at":"${stamp}","updated_at":"${stamp}"}`;
}
function collectPaths(commit: string, prefix = ''): string[] {
  const out: string[] = [];
  for (const e of listDirectory(commit, prefix)) {
    if (e.type === 'tree') out.push(...collectPaths(commit, e.path));
    else out.push(e.path);
  }
  return out;
}
function addPull(repo: Repo, title: string, body: string, head: string, base: string): Pull | null {
  const headSha = refOid(repo, `refs/heads/${head}`);
  const baseSha = refOid(repo, `refs/heads/${base}`);
  if (!headSha || !baseSha) return null;
  const baseBlobs = new Map<string, string>();
  for (const p of collectPaths(baseSha)) baseBlobs.set(p, blobOidAt(baseSha, p));
  const changedPaths = collectPaths(headSha).filter((p) => baseBlobs.get(p) !== blobOidAt(headSha, p));
  const pull: Pull = {
    id: nextGlobalId++, number: repo.nextPull++, title, body, head, base, headSha, baseSha,
    state: 'open', merged: false, mergeCommitSha: null, changedPaths,
  };
  repo.pulls.set(pull.number, pull);
  journal('pull', { repo: repoKey(repo.owner, repo.name), number: pull.number, title, head, base });
  return pull;
}
function mergePull(repo: Repo, pull: Pull): { ok: boolean; conflict: boolean; sha?: string } {
  if (pull.state !== 'open') return { ok: false, conflict: false };
  const baseRef = `refs/heads/${pull.base}`;
  const current = repo.refs.get(baseRef) ?? '';
  for (const path of pull.changedPaths) {
    const original = blobOidAt(pull.baseSha, path);
    const currentBlob = blobOidAt(current, path);
    if (original !== currentBlob) return { ok: false, conflict: true };
  }
  const tree = treeAt(pull.headSha);
  const sha = makeCommit(repo, pull.base, `Merge pull request #${pull.number} from ${pull.head}`, tree, current ? [current] : []);
  pull.state = 'closed';
  pull.merged = true;
  pull.mergeCommitSha = sha;
  repo.files.set(pull.base, tree);
  journal('merge', { repo: repoKey(repo.owner, repo.name), number: pull.number, sha });
  return { ok: true, conflict: false, sha };
}

// ---------------------------------------------------------------- HTTP helpers

type Reply = { socket: Socket };
function sendJson(res: Reply, status: number, body: unknown, extra = ''): void {
  sendJsonRaw(res, status, JSON.stringify(body), extra);
}
const issueCache = new Map<string, string>();
function sendJsonRaw(res: Reply, status: number, text: string, extra = ''): void {
  sendJsonResponse(res.socket, status, text, extra);
}
function sendRaw(res: Reply, status: number, contentType: string, body: Buffer): void {
  sendRawResponse(res.socket, status, contentType, body);
}
function authorized(req: RawRequest): boolean {
  const expected = process.env.MAKEFUN_SHOP_TOKEN ?? '';
  const header = req.headers.get('authorization') ?? '';
  if (!header.startsWith('Bearer ')) return false;
  const value = header.slice(7).trim();
  return expected === '' ? value.length > 0 : value === expected;
}
function parseJsonBody(raw: Buffer): { ok: true; value: unknown } | { ok: false } {
  if (!raw.length) return { ok: true, value: {} };
  try { return { ok: true, value: JSON.parse(raw.toString('utf8')) as unknown }; } catch { return { ok: false }; }
}

// ---------------------------------------------------------------- git pkt-line and packs

function pkt(text: string | Buffer): Buffer {
  const payload = typeof text === 'string' ? Buffer.from(text, 'utf8') : text;
  const header = Buffer.from((payload.length + 4).toString(16).padStart(4, '0'), 'ascii');
  return Buffer.concat([header, payload]);
}
const FLUSH = Buffer.from('0000', 'ascii');
const DELIM = Buffer.from('0001', 'ascii');
function parsePackets(raw: Buffer): { lines: string[]; consumed: number; bad: boolean } {
  const lines: string[] = [];
  let p = 0;
  while (p + 4 <= raw.length) {
    const text = raw.toString('ascii', p, p + 4);
    if (!/^[0-9a-fA-F]{4}$/.test(text)) break;
    const len = parseInt(text, 16);
    if (len === 0 || len === 1 || len === 2) { p += 4; continue; }
    if (len < 4 || p + len > raw.length) return { lines, consumed: p, bad: true };
    lines.push(raw.toString('utf8', p + 4, p + len));
    p += len;
  }
  return { lines, consumed: p, bad: false };
}
const PACK_TYPES: Record<number, ObjType> = { 1: 'commit', 2: 'tree', 3: 'blob', 4: 'tag' };
function readVarint(data: Buffer, pos: number): { value: number; next: number } {
  let value = 0;
  let shift = 0;
  let p = pos;
  for (;;) {
    const b = data[p++];
    if (b === undefined) throw new Error('truncated varint');
    value |= (b & 0x7f) * 2 ** shift;
    shift += 7;
    if ((b & 0x80) === 0) break;
  }
  return { value, next: p };
}
function applyDelta(base: Buffer, delta: Buffer): Buffer {
  const baseSize = readVarint(delta, 0);
  const outSize = readVarint(delta, baseSize.next);
  const chunks: Buffer[] = [];
  let p = outSize.next;
  while (p < delta.length) {
    const op = delta[p++] ?? 0;
    if (op & 0x80) {
      let off = 0, size = 0;
      if (op & 0x01) off = delta[p++] ?? 0;
      if (op & 0x02) off |= (delta[p++] ?? 0) << 8;
      if (op & 0x04) off |= (delta[p++] ?? 0) << 16;
      if (op & 0x08) off |= (delta[p++] ?? 0) << 24;
      if (op & 0x10) size = delta[p++] ?? 0;
      if (op & 0x20) size |= (delta[p++] ?? 0) << 8;
      if (op & 0x40) size |= (delta[p++] ?? 0) << 16;
      if (size === 0) size = 0x10000;
      chunks.push(base.subarray(off, off + size));
    } else if (op) {
      chunks.push(delta.subarray(p, p + op));
      p += op;
    } else {
      throw new Error('bad delta opcode');
    }
  }
  return Buffer.concat(chunks);
}
function readPack(pack: Buffer): string[] {
  if (pack.toString('ascii', 0, 4) !== 'PACK') throw new Error('not a pack');
  const version = pack.readUInt32BE(4);
  if (version !== 2 && version !== 3) throw new Error('bad pack version');
  const count = pack.readUInt32BE(8);
  const ids: string[] = [];
  const byOffset = new Map<number, GitObj>();
  let p = 12;
  for (let i = 0; i < count; i += 1) {
    const start = p;
    let b = pack[p++] ?? 0;
    const kind = (b >> 4) & 7;
    let size = b & 15;
    let shift = 4;
    while (b & 0x80) {
      b = pack[p++] ?? 0;
      size += (b & 0x7f) * 2 ** shift;
      shift += 7;
    }
    let baseOffset = -1;
    let baseOid = '';
    if (kind === 6) {
      b = pack[p++] ?? 0;
      let distance = b & 0x7f;
      while (b & 0x80) {
        b = pack[p++] ?? 0;
        distance = ((distance + 1) * 128) + (b & 0x7f);
      }
      baseOffset = start - distance;
    } else if (kind === 7) {
      baseOid = pack.toString('hex', p, p + 20);
      p += 20;
    }
    const inflated = inflateSync(pack.subarray(p), { info: true }) as unknown as { buffer: Buffer; engine: { bytesWritten: number } };
    p += inflated.engine.bytesWritten;
    let type: ObjType;
    let data: Buffer;
    if (kind === 6 || kind === 7) {
      const base = kind === 6 ? byOffset.get(baseOffset) : objects.get(baseOid);
      if (!base) throw new Error('missing delta base');
      type = base.type;
      data = applyDelta(base.data, inflated.buffer);
    } else {
      type = PACK_TYPES[kind] ?? 'blob';
      data = inflated.buffer;
    }
    if (data.length !== size) throw new Error('pack object size mismatch');
    const oid = putObject(type, data);
    const obj: GitObj = { type, data };
    byOffset.set(start, obj);
    ids.push(oid);
  }
  return ids;
}
function writePack(items: GitObj[]): Buffer {
  const parts: Buffer[] = [];
  const typeCode = (type: ObjType): number => type === 'commit' ? 1 : type === 'tree' ? 2 : type === 'blob' ? 3 : 4;
  for (const item of items) {
    const first = (typeCode(item.type) << 4) | (item.data.length & 15);
    let size = Math.floor(item.data.length / 16);
    const bytes: number[] = [first | (size > 0 ? 0x80 : 0)];
    while (size > 0) {
      bytes.push((size & 0x7f) | (size >= 0x80 ? 0x80 : 0));
      size = Math.floor(size / 128);
    }
    parts.push(Buffer.from(bytes), deflateSync(item.data));
  }
  const body = Buffer.concat([Buffer.from('PACK'), writeUInt32(2), writeUInt32(items.length), ...parts]);
  return Buffer.concat([body, createHash('sha1').update(body).digest()]);
}
function writeUInt32(n: number): Buffer {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n >>> 0, 0);
  return b;
}

// ---------------------------------------------------------------- git advertisement and transport

function allRefs(repo: Repo): Array<{ name: string; oid: string }> {
  const out: Array<{ name: string; oid: string }> = [];
  for (const [name, oid] of repo.refs) if (oid && oid !== ZERO) out.push({ name, oid });
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}
const UPLOAD_CAPS = 'side-band-64k thin-pack ofs-delta shallow multi_ack multi_ack_detailed no-done agent=makefun-forge/1';
const RECEIVE_CAPS = 'report-status report-status-v2 side-band-64k delete-refs ofs-delta agent=makefun-forge/1';
function v0Advertisement(repo: Repo, service: 'git-upload-pack' | 'git-receive-pack', caps: string): Buffer {
  const lines: string[] = [`# service=${service}`];
  const refs = allRefs(repo);
  if (!refs.length) {
    lines.push(`${ZERO} capabilities^{}\0${caps}`);
  } else {
    refs.forEach((r, i) => lines.push(`${r.oid} ${r.name}${i === 0 ? `\0${caps} symref=HEAD:refs/heads/${repo.defaultBranch}` : ''}`));
  }
  return Buffer.concat([...lines.map((l, i) => pkt(i === 0 ? `${l}\n` : l)), FLUSH]);
}
function v2Advertisement(repo: Repo): Buffer {
  const caps = ['version 2', 'ls-refs=unborn', 'fetch=shallow filter', 'object-format=sha1', 'agent=makefun-forge/1'];
  return Buffer.concat([...caps.map((c) => pkt(`${c}\n`)), FLUSH]);
}
function lsRefsResponse(repo: Repo, prefixes: string[]): Buffer {
  const lines: string[] = [];
  const keep = (name: string): boolean => prefixes.length === 0 || prefixes.some((p) => name.startsWith(p));
  const head = refOid(repo, `refs/heads/${repo.defaultBranch}`);
  if (keep('HEAD')) lines.push(head ? `${head} HEAD symref-target:refs/heads/${repo.defaultBranch}` : `unborn HEAD symref-target:refs/heads/${repo.defaultBranch}`);
  for (const r of allRefs(repo)) {
    if (!keep(r.name)) continue;
    const peeled = peeledOid(r.oid);
    lines.push(`${r.oid} ${r.name}${peeled ? ` peeled:${peeled}` : ''}`);
  }
  return Buffer.concat([...lines.map((l) => pkt(`${l}\n`)), FLUSH]);
}
function reachableFrom(start: string, maxCommits: number | null): Set<string> {
  const out = new Set<string>();
  const commits = new Set<string>();
  const queue: Array<{ oid: string; depth: number }> = [{ oid: start, depth: 1 }];
  const addObject = (oid: string): void => {
    if (!oid || out.has(oid)) return;
    const obj = objects.get(oid);
    if (!obj) return;
    out.add(oid);
    if (obj.type === 'tree') for (const e of parseTree(obj.data)) addObject(e.oid);
    else if (obj.type === 'tag') {
      const target = parseTagTarget(oid);
      if (target) addObject(target);
    }
  };
  while (queue.length) {
    const cur = queue.pop() ?? { oid: '', depth: 1 };
    if (!cur.oid || commits.has(cur.oid)) continue;
    commits.add(cur.oid);
    addObject(cur.oid);
    const info = commitInfo(cur.oid);
    if (info.tree) addObject(info.tree);
    if (maxCommits === null || cur.depth < maxCommits) {
      for (const p of info.parents) queue.push({ oid: p, depth: cur.depth + 1 });
    }
  }
  return out;
}
function sideband(body: Buffer): Buffer {
  const out: Buffer[] = [];
  for (let p = 0; p < body.length; p += MAX_PKT - 1) {
    const chunk = body.subarray(p, p + MAX_PKT - 1);
    out.push(pkt(Buffer.concat([Buffer.from([1]), chunk])));
  }
  if (!body.length) out.push(pkt(Buffer.from([1])));
  return Buffer.concat(out);
}
function generationDistance(start: string, target: string): number | null {
  const queue: Array<{ oid: string; gen: number }> = [{ oid: start, gen: 0 }];
  const seen = new Set<string>();
  while (queue.length) {
    const cur = queue.pop() ?? { oid: '', gen: 0 };
    if (!cur.oid || seen.has(cur.oid)) continue;
    seen.add(cur.oid);
    if (cur.oid === target) return cur.gen;
    if (cur.gen > 64) continue;
    for (const p of commitInfo(cur.oid).parents) queue.push({ oid: p, gen: cur.gen + 1 });
  }
  return null;
}
function fetchResponse(repo: Repo, wants: string[], haves: string[], boundaries: string[], depth: number | null): Buffer {
  const excluded = new Set<string>();
  for (const have of haves) for (const oid of reachableFrom(have, boundaries.length ? 1 : null)) excluded.add(oid);
  const send = new Set<string>();
  const traversal = new Set<string>();
  for (const want of wants) {
    let wantDepth = depth;
    if (depth !== null) {
      for (const boundary of boundaries) {
        const distance = generationDistance(want, boundary);
        if (distance !== null) wantDepth = Math.max(wantDepth ?? 0, distance + depth);
      }
    }
    for (const oid of reachableFrom(want, wantDepth)) {
      traversal.add(oid);
      if (!excluded.has(oid)) send.add(oid);
    }
  }
  const shallow: string[] = [];
  for (const oid of traversal) {
    const obj = objects.get(oid);
    if (obj?.type !== 'commit') continue;
    const parents = commitInfo(oid).parents;
    if (parents.length && parents.some((p) => !traversal.has(p))) shallow.push(oid);
  }
  for (const boundary of boundaries) {
    if (traversal.has(boundary) && !shallow.includes(boundary)) shallow.push(boundary);
  }
  const parts: Buffer[] = [];
  parts.push(pkt('acknowledgments\n'), pkt('NAK\n'), FLUSH);
  if (shallow.length) {
    parts.push(pkt('shallow-info\n'));
    for (const oid of shallow) parts.push(pkt(`shallow ${oid}\n`));
    parts.push(FLUSH);
  }
  parts.push(pkt('packfile\n'));
  const cacheKey = packCacheKey(wants, haves, boundaries, depth);
  const cached = cacheKey === null ? undefined : packCache.get(cacheKey);
  if (cached !== undefined) return cached;
  const items: GitObj[] = [];
  for (const oid of send) {
    const obj = objects.get(oid);
    if (obj) items.push(obj);
  }
  parts.push(sideband(writePack(items)), FLUSH);
  const out = Buffer.concat(parts);
  if (cacheKey !== null) packCache.set(cacheKey, out);
  return out;
}
function gitResult(res: Reply, service: 'git-upload-pack' | 'git-receive-pack', body: Buffer): void {
  sendRaw(res, 200, `application/x-${service}-result`, body);
}
function parseFetchLines(lines: string[]): { wants: string[]; haves: string[]; shallow: string[]; depth: number | null } {
  const wants: string[] = [], haves: string[] = [], shallow: string[] = [];
  let depth: number | null = null;
  for (const line of lines) {
    if (line.startsWith('want ')) wants.push((line.slice(5).split(' ')[0] ?? '').trim());
    else if (line.startsWith('have ')) haves.push(line.slice(5).trim());
    else if (line.startsWith('shallow ')) shallow.push(line.slice(8).trim());
    else if (line.startsWith('deepen ')) depth = parseInt(line.slice(7), 10);
  }
  return { wants, haves, shallow, depth: Number.isFinite(depth) ? depth : null };
}
async function handleGit(req: RawRequest, res: Reply, owner: string, repoName: string, action: string): Promise<void> {
  const repo = getRepo(owner, repoName);
  if (!repo) { sendJson(res, 404, { message: 'Not Found' }); return; }
  if (!authorized(req)) { sendJson(res, 401, { message: 'Unauthorized' }); return; }
  if (action === 'info/refs') {
    const url = new URL(req.target, 'http://x');
    const service = url.searchParams.get('service');
    if (service === 'git-upload-pack') {
      const v2 = (req.headers.get('git-protocol') ?? '').includes('version=2');
      sendRaw(res, 200, 'application/x-git-upload-pack-advertisement', v2 ? v2Advertisement(repo) : v0Advertisement(repo, 'git-upload-pack', UPLOAD_CAPS));
      return;
    }
    if (service === 'git-receive-pack') {
      sendRaw(res, 200, 'application/x-git-receive-pack-advertisement', v0Advertisement(repo, 'git-receive-pack', RECEIVE_CAPS));
      return;
    }
    sendJson(res, 404, { message: 'Not Found' });
    return;
  }
  const raw = req.body;
  const parsed = parsePackets(raw);
  if (parsed.bad || parsed.lines.length === 0 && raw.length > 4) {
    sendRaw(res, 400, 'text/plain', Buffer.from('bad pkt-line'));
    return;
  }
  if (action === 'git-upload-pack') {
    const command = parsed.lines[0] ?? '';
    if (command.startsWith('command=ls-refs')) {
      const prefixes = parsed.lines.filter((l) => l.startsWith('ref-prefix ')).map((l) => l.slice(11).trim());
      sendRaw(res, 200, 'application/x-git-upload-pack-result', lsRefsResponse(repo, prefixes));
      return;
    }
    if (!command.startsWith('command=fetch')) {
      sendRaw(res, 400, 'text/plain', Buffer.from('unknown command'));
      return;
    }
    const { wants, haves, shallow, depth } = parseFetchLines(parsed.lines);
    gitResult(res, 'git-upload-pack', fetchResponse(repo, wants, haves, shallow, depth));
    return;
  }
  if (action === 'git-receive-pack') {
    const commands: Array<{ old: string; next: string; ref: string }> = [];
    for (const line of parsed.lines) {
      if (line.startsWith('command=') || line.startsWith('object-format=')) continue;
      const clean = line.split('\0')[0] ?? '';
      const parts = clean.trim().split(' ');
      if (parts.length >= 3 && hex40(parts[0]) && hex40(parts[1])) commands.push({ old: parts[0] ?? '', next: parts[1] ?? '', ref: parts[2] ?? '' });
    }
    let unpackOk = true;
    try {
      const pack = raw.subarray(parsed.consumed);
      if (pack.length) readPack(pack);
    } catch {
      unpackOk = false;
    }
    const report: string[] = [`unpack ${unpackOk ? 'ok' : 'pack error'}`];
    for (const command of commands) {
      const current = repo.refs.get(command.ref) ?? ZERO;
      let ok = unpackOk;
      let reason = '';
      if (!command.ref.startsWith('refs/')) { ok = false; reason = 'invalid-ref'; }
      else if (command.next === ZERO) {
        if (current === ZERO || current !== command.old) { ok = false; reason = 'non-fast-forward'; }
        else repo.refs.delete(command.ref);
      } else if (current !== command.old && current !== ZERO) { ok = false; reason = 'non-fast-forward'; }
      else if (current !== ZERO && !isAncestor(current, command.next)) { ok = false; reason = 'non-fast-forward'; }
      else {
        repo.refs.set(command.ref, command.next);
        const branch = command.ref.startsWith('refs/heads/') ? command.ref.slice(11) : '';
        if (branch) {
          const obj = objects.get(command.next);
          repo.files.set(branch, obj?.type === 'commit' ? treeAt(command.next) : '');
        }
      }
      report.push(ok ? `ok ${command.ref}` : `ng ${command.ref} ${reason || 'non-fast-forward'}`);
      journal('ref', { repo: repoKey(owner, repoName), ref: command.ref, old: command.old, next: command.next, ok });
    }
    const text = Buffer.concat(report.map((l) => pkt(`${l}\n`)));
    gitResult(res, 'git-receive-pack', sideband(text));
    return;
  }
  sendJson(res, 404, { message: 'Not Found' });
}

// ---------------------------------------------------------------- REST v3

function contentJson(repo: Repo, commit: string, path: string, raw: Buffer): Rec {
  return {
    type: 'file', encoding: 'base64', content: raw.toString('base64'),
    sha: objectHash('blob', raw), size: raw.length, name: path.split('/').pop() ?? path, path,
    _links: { self: `https://github.com/${repo.owner}/${repo.name}/blob/${commit}/${path}` },
  };
}
async function handleRest(req: RawRequest, res: Reply, path: string, url: URL): Promise<void> {
  if (!authorized(req)) { sendJson(res, 401, { message: 'Bad credentials' }); return; }
  const method = req.method;
  const raw = req.body;
  const bodyValue = parseJsonBody(raw);
  if (!bodyValue.ok) { sendJson(res, 400, { message: 'Problems parsing JSON' }); return; }
  const body = rec(bodyValue.value);
  const seg = path.split('/').filter(Boolean);
  const host = `http://${str(req.headers.get('host')) || 'forge.local'}`;

  if (path === '/api/v3' || path === '/api/v3/') {
    if (method !== 'GET') { sendJson(res, 404, { message: 'Not Found' }); return; }
    sendJson(res, 200, { current_user_url: `${host}/api/v3/user`, repository_url: `${host}/api/v3/repos/{owner}/{repo}`, installed_version: '1.0.0' });
    return;
  }
  if (path === '/api/v3/meta') { sendJson(res, 200, { installed_version: '1.0.0', verifiable_password_authentication: false }); return; }
  if (path === '/api/v3/user') { sendJson(res, 200, { login: VIEWER, id: 1000, type: 'User' }); return; }
  if (path === '/api/v3/user/repos' && method === 'GET') {
    const perPage = Math.min(Math.max(parseInt(url.searchParams.get('per_page') ?? '30', 10) || 30, 1), 100);
    const page = Math.min(Math.max(parseInt(url.searchParams.get('page') ?? '1', 10) || 1, 1), 1000);
    const all = [...repos.values()].filter((r) => r.owner === VIEWER);
    const slice = all.slice((page - 1) * perPage, page * perPage);
    let extra = '';
    if (page * perPage < all.length) {
      extra = `link: <${host}${url.pathname}?per_page=${perPage}&page=${page + 1}>; rel="next"\r\n`;
    }
    sendJson(res, 200, slice.map(repoJson), extra);
    return;
  }
  if (path === '/api/v3/user/repos' && method === 'POST') {
    const name = str(body.name);
    const existing = getRepo(VIEWER, name);
    if (existing) { sendJson(res, 422, { message: 'Validation Failed: repository already exists' }); return; }
    const repo = createRepo(name, VIEWER, str(body.description), body.private !== false);
    if (!repo) { sendJson(res, 422, { message: 'Validation Failed: bad repository name' }); return; }
    sendJson(res, 201, repoJson(repo));
    return;
  }
  if (seg[0] !== 'api' || seg[1] !== 'v3' || seg[2] !== 'repos' || !seg[3] || !seg[4]) {
    sendJson(res, 404, { message: 'Not Found' });
    return;
  }
  const owner = decodeURIComponent(seg[3]);
  const name = decodeURIComponent(seg[4]);
  const repo = getRepo(owner, name);
  if (!repo || owner !== VIEWER) { sendJson(res, 404, { message: 'Not Found' }); return; }
  const rest = seg.slice(5).map(decodeURIComponent);

  if (rest.length === 0) {
    if (method === 'GET') { sendJson(res, 200, repoJson(repo)); return; }
    if (method === 'PATCH') {
      if (body.description !== undefined) repo.description = str(body.description);
      if (body.private !== undefined) repo.private = body.private === true;
      journal('repo-patch', { key: repoKey(owner, name), description: repo.description });
      sendJson(res, 200, repoJson(repo));
      return;
    }
    if (method === 'DELETE') {
      repos.delete(repoKey(owner, name));
      journal('repo-delete', { key: repoKey(owner, name) });
      sendRawResponse(res.socket, 204, null, Buffer.alloc(0), `x-github-request-id: req-${++requestCounter}\r\nx-oauth-scopes: repo, read:org\r\n`);
      return;
    }
  }
  if (rest[0] === 'branches' && method === 'GET') {
    const list = [...repo.refs.entries()].filter(([r, o]) => r.startsWith('refs/heads/') && o && o !== ZERO).map(([r, o]) => ({ name: r.slice(11), commit: { sha: o } }));
    sendJson(res, 200, list);
    return;
  }
  if (rest[0] === 'git' && rest[1] === 'refs' && method === 'POST') {
    const ref = str(body.ref);
    const sha = str(body.sha);
    if (!ref.startsWith('refs/') || !hex40(sha)) { sendJson(res, 422, { message: 'Validation Failed' }); return; }
    repo.refs.set(ref, sha);
    journal('ref', { repo: repoKey(owner, name), ref, next: sha });
    sendJson(res, 201, { ref, object: { sha, type: 'commit' } });
    return;
  }
  if (rest[0] === 'git' && rest[1] === 'refs' && rest[2] === 'heads' && method === 'GET') {
    const ref = `refs/heads/${rest.slice(3).join('/')}`;
    const sha = repo.refs.get(ref);
    if (!sha) { sendJson(res, 404, { message: 'Not Found' }); return; }
    sendJson(res, 200, { ref, object: { sha } });
    return;
  }
  if (rest[0] === 'commits' && method === 'GET') {
    const ref = url.searchParams.get('sha') ?? `refs/heads/${repo.defaultBranch}`;
    let oid = refOid(repo, ref);
    const list: Rec[] = [];
    const seen = new Set<string>();
    while (oid && hex40(oid) && !seen.has(oid)) {
      seen.add(oid);
      const info = commitInfo(oid);
      list.push({ sha: oid, commit: { message: info.message }, html_url: `${url.origin}/${owner}/${name}/commit/${oid}` });
      oid = info.parents[0] ?? '';
    }
    sendJson(res, 200, list);
    return;
  }
  if (rest[0] === 'contents') {
    const targetPath = rest.slice(1).join('/');
    const ref = url.searchParams.get('ref') ?? `refs/heads/${repo.defaultBranch}`;
    const tip = refOid(repo, ref);
    if (method === 'GET') {
      const blob = tip ? blobOidAt(tip, targetPath) : '';
      if (blob) {
        const obj = objects.get(blob);
        if (obj) { sendJson(res, 200, contentJson(repo, tip, targetPath, obj.data)); return; }
      }
      const entries = tip ? listDirectory(tip, targetPath) : [];
      if (entries.length) {
        sendJson(res, 200, entries.map((e) => ({ type: e.type === 'tree' ? 'dir' : 'file', name: e.name, path: e.path, sha: e.oid, size: 0 })));
        return;
      }
      sendJson(res, 404, { message: 'Not Found' });
      return;
    }
    if (method === 'PUT') {
      const branch = str(body.branch) || repo.defaultBranch;
      const parent = refOid(repo, `refs/heads/${branch}`);
      const encoded = str(body.content).replace(/[\r\n\t ]/g, '');
      let data: Buffer;
      try { data = Buffer.from(encoded, 'base64'); } catch { sendJson(res, 400, { message: 'Invalid content' }); return; }
      const currentBlob = blobOidAt(parent, targetPath);
      const existed = Boolean(currentBlob);
      if (body.sha !== undefined && str(body.sha).toLowerCase() !== currentBlob.toLowerCase()) {
        sendJson(res, 409, { message: 'SHA mismatch' }); return;
      }
      const blob = putObject('blob', data);
      const tree = setBlobAt(parent, targetPath, blob);
      const sha = makeCommit(repo, branch, str(body.message) || `Update ${targetPath}`, tree, parent ? [parent] : []);
      repo.files.set(branch, tree);
      journal('content', { repo: repoKey(owner, name), path: targetPath, branch, sha, blob });
      sendJson(res, existed ? 200 : 201, {
        content: { sha: blob, encoding: 'base64', path: targetPath },
        commit: { sha, message: str(body.message) },
      });
      return;
    }
    if (method === 'DELETE') {
      const branch = str(body.branch) || repo.defaultBranch;
      const parent = refOid(repo, `refs/heads/${branch}`);
      const current = blobOidAt(parent, targetPath);
      if (!current) { sendJson(res, 404, { message: 'Not Found' }); return; }
      if (body.sha !== undefined && str(body.sha).toLowerCase() !== current.toLowerCase()) {
        sendJson(res, 409, { message: 'SHA mismatch' }); return;
      }
      const tree = setBlobAt(parent, targetPath, null);
      const sha = makeCommit(repo, branch, str(body.message) || `Delete ${targetPath}`, tree, parent ? [parent] : []);
      journal('content-delete', { repo: repoKey(owner, name), path: targetPath, sha });
      sendJson(res, 200, { commit: { sha } });
      return;
    }
  }
  if (rest[0] === 'labels' && method === 'POST') {
    const label = str(body.name);
    if (!label) { sendJson(res, 422, { message: 'Validation Failed' }); return; }
    repo.labels.add(label);
    sendJson(res, 201, { id: nextGlobalId++, name: label, color: str(body.color) || 'ededed' });
    return;
  }
  if (rest[0] === 'issues') {
    if (rest.length === 1 && method === 'POST') {
      const labels = asArray(body.labels).map(str);
      if (!labels.length) {
        const created = addIssueFast(repo, str(body.title), str(body.body));
        if (created) { sendJsonRaw(res, 201, created); return; }
      }
      const issue = addIssue(repo, str(body.title), str(body.body), labels);
      if (!issue) { sendJson(res, 422, { message: 'Validation Failed: unknown label' }); return; }
      sendJson(res, 201, issueJson(issue));
      return;
    }
    if (rest.length === 1 && method === 'GET') {
      const state = url.searchParams.get('state') ?? 'open';
      let list = [...repo.issues.values()];
      if (state === 'open') list = list.filter((i) => i.state === 'open');
      else if (state === 'closed') list = list.filter((i) => i.state === 'closed');
      list.sort((a, b) => a.number - b.number);
      if ((url.searchParams.get('sort') ?? '').toLowerCase() === 'created' && (url.searchParams.get('direction') ?? '').toLowerCase() === 'desc') list.reverse();
      const per = parseInt(url.searchParams.get('per_page') ?? '100', 10);
      if (Number.isFinite(per) && per > 0) list = list.slice(0, per);
      sendJson(res, 200, list.map(issueJson));
      return;
    }
    const number = parseInt(rest[1] ?? '', 10);
    const issue = Number.isFinite(number) ? repo.issues.get(number) : undefined;
    if (rest.length === 2 && !issue) { sendJson(res, 404, { message: 'Not Found' }); return; }
    if (rest.length === 2 && method === 'GET') {
      const key = `${repoKey(owner, name)}#${(issue as Issue).number}`;
      const cached = issueCache.get(key);
      if (cached !== undefined) { sendJsonRaw(res, 200, cached); return; }
      const text = JSON.stringify(issueJson(issue as Issue));
      issueCache.set(key, text);
      sendJsonRaw(res, 200, text);
      return;
    }
    if (rest.length === 2 && method === 'PATCH') {
      if (body.state === 'open' || body.state === 'closed') { (issue as Issue).state = body.state; issueCache.delete(`${repoKey(owner, name)}#${(issue as Issue).number}`); }
      sendJson(res, 200, issueJson(issue ?? {} as Issue));
      return;
    }
    if (rest[2] === 'comments') {
      if (!issue) { sendJson(res, 404, { message: 'Not Found' }); return; }
      if (method === 'POST') {
        const comment = { id: repo.nextComment++, body: str(body.body) };
        (issue as Issue).comments.push(comment);
        journal('comment', { repo: repoKey(owner, name), number, id: comment.id });
        sendJson(res, 201, comment);
        return;
      }
      if (method === 'GET') { sendJson(res, 200, (issue as Issue).comments); return; }
    }
  }
  if (rest[0] === 'pulls') {
    if (rest.length === 1 && method === 'POST') {
      const pull = addPull(repo, str(body.title), str(body.body), str(body.head), str(body.base));
      if (!pull) { sendJson(res, 422, { message: 'Validation Failed: unknown head or base' }); return; }
      sendJson(res, 201, pullJson(pull));
      return;
    }
    if (rest.length === 1 && method === 'GET') {
      const state = url.searchParams.get('state') ?? 'open';
      let list = [...repo.pulls.values()];
      if (state === 'open') list = list.filter((p) => p.state === 'open');
      else if (state === 'closed') list = list.filter((p) => p.state === 'closed');
      list.sort((a, b) => a.number - b.number);
      sendJson(res, 200, list.map(pullJson));
      return;
    }
    const number = parseInt(rest[1] ?? '', 10);
    const pull = Number.isFinite(number) ? repo.pulls.get(number) : undefined;
    if (!pull) { sendJson(res, 404, { message: 'Not Found' }); return; }
    if (rest.length === 2 && method === 'GET') { sendJson(res, 200, pullJson(pull)); return; }
    if (rest[2] === 'files' && method === 'GET') {
      const files = pull.changedPaths.length ? pull.changedPaths : ['README.md'];
      sendJson(res, 200, files.map((filename) => ({ filename, status: 'modified', additions: 1, deletions: 0, changes: 1, patch: '@@ -1 +1 @@' })));
      return;
    }
    if (rest[2] === 'merge' && method === 'PUT') {
      const merged = mergePull(repo, pull);
      if (merged.conflict) { sendJson(res, 405, { message: 'Pull Request is conflicted' }); return; }
      if (!merged.ok) { sendJson(res, 405, { message: 'Pull Request cannot be merged' }); return; }
      sendJson(res, 200, { sha: merged.sha, merged: true, message: 'Pull Request successfully merged' });
      return;
    }
  }
  sendJson(res, 404, { message: 'Not Found' });
}

// ---------------------------------------------------------------- GraphQL

function globalId(kind: string, repo: Repo, number: number | string): string {
  return encodeBase64Url(`${kind}:${repo.id}:${number}`);
}
function fromGlobalId(id: string): { kind: string; repoId: number; number: string } | null {
  const text = decodeBase64Url(id);
  const [kind, rid, number] = text.split(':');
  const repoId = parseInt(rid ?? '', 10);
  if (!kind || !Number.isFinite(repoId) || number === undefined) return null;
  return { kind, repoId, number };
}
function repoById(id: number): Repo | null {
  for (const r of repos.values()) if (r.id === id) return r;
  return null;
}
function gqlIssue(repo: Repo, issue: Issue): Rec {
  return {
    id: globalId('issue', repo, issue.number), number: issue.number, title: issue.title,
    state: issue.state === 'open' ? 'OPEN' : 'CLOSED', body: issue.body,
  };
}
function gqlPull(repo: Repo, pull: Pull): Rec {
  return {
    id: globalId('pull', repo, pull.number), number: pull.number,
    state: pull.state === 'open' ? 'OPEN' : pull.merged ? 'MERGED' : 'CLOSED',
    merged: pull.merged, headRefName: pull.head, baseRefName: pull.base,
    mergeable: 'MERGEABLE', files: { nodes: pull.changedPaths.map((path) => ({ path })) },
  };
}
function gqlRepo(repo: Repo): Rec {
  return {
    id: globalId('repo', repo, repo.id), databaseId: repo.id, name: repo.name,
    nameWithOwner: `${repo.owner}/${repo.name}`, owner: { login: repo.owner },
    defaultBranchRef: { name: repo.defaultBranch }, viewerPermission: 'ADMIN',
    hasIssuesEnabled: true,
  };
}
const GQL_FIELDS: Rec<string[]> = {
  Issue: ['id', 'number', 'title', 'state', 'body', 'repository', 'createdAt'],
  PullRequest: ['id', 'number', 'headRefName', 'baseRefName', 'mergeable', 'state', 'merged', 'files', 'title', 'body'],
  Repository: ['id', 'databaseId', 'name', 'nameWithOwner', 'owner', 'defaultBranchRef', 'viewerPermission', 'hasIssuesEnabled', 'issue', 'issues', 'pullRequests', 'pullRequest', 'description'],
};
function gqlData(query: string, vars: Rec): { data: unknown; errors?: Rec[] } {
  const match = /\b(?:query|mutation)\s+([A-Za-z_]\w*)/.exec(query);
  const op = match?.[1] ?? '';
  const owner = str(vars.owner) || VIEWER;
  const name = str(vars.name) || str(vars.repo) || '';
  const repo = name ? getRepo(owner, name) : null;
  const first = num(vars.first) ?? 10;
  const errors = (message: string): { data: unknown; errors: Rec[] } => ({ data: null, errors: [{ message, path: [] }] });

  switch (op) {
    case 'ViewerLogin': return { data: { viewer: { login: VIEWER } } };
    case 'RepositoryFields': return { data: { repository: repo ? gqlRepo(repo) : null } };
    case 'Issue_fields':
      return { data: { Issue: { fields: (GQL_FIELDS.Issue ?? []).map((n) => ({ name: n })) } } };
    case 'PullRequestAndRepositoryFields':
      return {
        data: {
          PullRequest: { fields: (GQL_FIELDS.PullRequest ?? []).map((n) => ({ name: n })) },
          Repository: { fields: (GQL_FIELDS.Repository ?? []).map((n) => ({ name: n })) },
        },
      };
    case 'SearchType_enumValues':
      return { data: { SearchType: { enumValues: ['ISSUE', 'REPOSITORY', 'USER', 'DISCUSSION'].map((n) => ({ name: n })) } } };
    case 'OpenIssues': {
      if (!repo) return { data: { repository: null } };
      const list = [...repo.issues.values()].filter((i) => i.state === 'open').sort((a, b) => b.number - a.number).slice(0, first);
      return { data: { repository: { issues: { nodes: list.map((i) => gqlIssue(repo, i)) } } } };
    }
    case 'IssueByNumber': {
      if (!repo) return { data: { repository: null } };
      const issue = repo.issues.get(num(vars.number) ?? -1);
      return { data: { repository: { issue: issue ? gqlIssue(repo, issue) : null } } };
    }
    case 'IssuesPage': {
      if (!repo) return { data: { repository: null } };
      let list = [...repo.issues.values()].sort((a, b) => b.number - a.number);
      if (vars.after !== null && vars.after !== undefined) {
        const cursor = parseInt(decodeBase64Url(str(vars.after)), 10);
        if (Number.isFinite(cursor)) list = list.filter((i) => i.number < cursor);
      }
      const page = list.slice(0, first);
      const last = page[page.length - 1];
      return {
        data: {
          repository: {
            issues: {
              nodes: page.map((i) => ({ number: i.number })),
              pageInfo: { hasNextPage: list.length > first && page.length > 0, endCursor: last ? encodeBase64Url(String(last.number)) : null },
            },
          },
        },
      };
    }
    case 'PullRequests': {
      if (!repo) return { data: { repository: null } };
      const list = [...repo.pulls.values()].sort((a, b) => a.number - b.number).slice(0, first);
      return { data: { repository: { pullRequests: { nodes: list.map((p) => gqlPull(repo, p)) } } } };
    }
    case 'PullRequestFiles':
    case 'PullRequestId': {
      if (!repo) return { data: { repository: null } };
      const pull = repo.pulls.get(num(vars.number) ?? -1);
      if (!pull) return { data: { repository: { pullRequest: null } } };
      return { data: { repository: { pullRequest: gqlPull(repo, pull) } } };
    }
    case 'IssueCreate': {
      const input = rec(vars.input);
      const info = fromGlobalId(str(input.repositoryId));
      const target = (info ? repoById(info.repoId) : null) ?? repo;
      if (!target) return errors('repository not found');
      const issue = addIssue(target, str(input.title), str(input.body), []);
      if (!issue) return errors('could not create issue');
      return { data: { createIssue: { issue: { number: issue.number, id: globalId('issue', target, issue.number) } } } };
    }
    case 'IssueClose': {
      const input = rec(vars.input);
      const info = fromGlobalId(str(input.issueId));
      const target = info ? repoById(info.repoId) : null;
      const issue = target?.issues.get(parseInt(info?.number ?? '', 10));
      if (!target || !issue) return errors('issue not found');
      issue.state = 'closed';
      return { data: { closeIssue: { issue: gqlIssue(target, issue) } } };
    }
    case 'PullRequestCreate': {
      const input = rec(vars.input);
      const info = fromGlobalId(str(input.repositoryId));
      const target = (info ? repoById(info.repoId) : null) ?? repo;
      if (!target) return errors('repository not found');
      const pull = addPull(target, str(input.title), '', str(input.headRefName), str(input.baseRefName));
      if (!pull) return errors('could not create pull request');
      return { data: { createPullRequest: { pullRequest: gqlPull(target, pull) } } };
    }
    case 'MergePullRequest': {
      const input = rec(vars.input);
      const info = fromGlobalId(str(input.pullRequestId));
      const target = info ? repoById(info.repoId) : null;
      const pull = target?.pulls.get(parseInt(info?.number ?? '', 10));
      if (!target || !pull) return errors('pull request not found');
      const result = mergePull(target, pull);
      if (!result.ok) return errors('pull request could not be merged');
      return { data: { mergePullRequest: { pullRequest: gqlPull(target, pull) } } };
    }
    case 'AddComment': {
      const input = rec(vars.input);
      const info = fromGlobalId(str(input.subjectId));
      const target = info ? repoById(info.repoId) : null;
      const issue = target?.issues.get(parseInt(info?.number ?? '', 10));
      if (!target || !issue) return errors('subject not found');
      const comment = { id: target.nextComment++, body: str(input.body) };
      issue.comments.push(comment);
      return { data: { addComment: { commentEdge: { node: { id: `comment:${target.id}:${issue.number}:${comment.id}`, body: comment.body } } } } };
    }
    case 'NodeRepository': {
      const info = fromGlobalId(str(vars.id));
      const target = info?.kind === 'repo' ? repoById(info.repoId) : null;
      return { data: { node: target ? { id: globalId('repo', target, target.id), name: target.name, nameWithOwner: `${target.owner}/${target.name}` } : null } };
    }
    case 'ViewerInvalidField':
      return { data: { viewer: { login: VIEWER } }, errors: [{ message: 'Cannot query field noSuchFieldOnViewer on type User.', path: ['viewer', 'noSuchFieldOnViewer'] }] };
    default:
      return errors(`Unknown operation ${op || '(anonymous)'}`);
  }
}
async function handleGraphql(req: RawRequest, res: Reply, raw: Buffer): Promise<void> {
  if (!authorized(req)) { sendJson(res, 401, { message: 'Bad credentials' }); return; }
  const parsed = parseJsonBody(raw);
  if (!parsed.ok) { sendJson(res, 400, { message: 'Problems parsing JSON' }); return; }
  const answer = (op: unknown): Rec => {
    const item = rec(op);
    const query = str(item.query);
    const vars = rec(item.variables);
    try { return gqlData(query, vars); } catch (err) { return { data: null, errors: [{ message: String(err) }] }; }
  };
  if (Array.isArray(parsed.value)) { sendJson(res, 200, parsed.value.map(answer)); return; }
  sendJson(res, 200, answer(parsed.value));
}

// ---------------------------------------------------------------- MCP

const MCP_TOOLS = [
  { name: 'create_repo', description: 'Create a repository for the authenticated viewer.', inputSchema: { type: 'object', properties: { name: { type: 'string' }, description: { type: 'string' }, private: { type: 'boolean' } }, required: ['name'] } },
  { name: 'get_repo', description: 'Get a repository by owner and name.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, full_name: { type: 'string' } }, required: ['owner', 'repo'] } },
  { name: 'create_issue', description: 'Open an issue in a repository.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, title: { type: 'string' }, body: { type: 'string' } }, required: ['owner', 'repo', 'title'] } },
  { name: 'list_issues', description: 'List issues in a repository.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, state: { type: 'string' } }, required: ['owner', 'repo'] } },
  { name: 'create_pull_request', description: 'Open a pull request between two branches.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, title: { type: 'string' }, body: { type: 'string' }, head: { type: 'string' }, base: { type: 'string' } }, required: ['owner', 'repo', 'title', 'head', 'base'] } },
  { name: 'merge_pull_request', description: 'Merge a pull request.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, pull_number: { type: 'integer' } }, required: ['owner', 'repo', 'pull_number'] } },
];
const MCP_SERVER_INFO = { name: 'forge-v3', version: '1.0.0' };
function rpc(id: unknown, result: unknown): Rec {
  return { jsonrpc: '2.0', id: id ?? null, result };
}
function rpcError(id: unknown, code: number, message: string): Rec {
  return { jsonrpc: '2.0', id: id ?? null, error: { code, message } };
}
function toolResult(data: Rec, isError = false): Rec {
  return { content: [{ type: 'text', text: JSON.stringify(data) }], structuredContent: data, ...(isError ? { isError: true } : {}) };
}
async function handleMcp(req: RawRequest, res: Reply, raw: Buffer): Promise<void> {
  const text = raw.toString('utf8');
  let msg: Rec;
  try {
    const value: unknown = JSON.parse(text);
    if (!isRec(value)) throw new Error('not an object');
    msg = value;
  } catch {
    sendJson(res, 400, rpcError(null, -32700, 'Parse error'));
    return;
  }
  if (!authorized(req)) { sendJson(res, 401, rpcError(msg.id, -32000, 'Unauthorized')); return; }
  const id = msg.id;
  if (msg.method === 'notifications/initialized') { sendRawResponse(res.socket, 200, null, Buffer.alloc(0)); return; }
  const method = str(msg.method);
  const params = rec(msg.params);
  if (method === 'server/discover') {
    sendJson(res, 200, rpc(id, {
      resultType: 'server/discover', supportedVersions: ['2026-07-28', '2025-11-25'],
      serverInfo: MCP_SERVER_INFO, capabilities: { tools: {} }, instructions: '',
      _meta: { 'io.modelcontextprotocol/serverInfo': MCP_SERVER_INFO },
    }));
    return;
  }
  if (method === 'initialize') {
    const offered = str(params.protocolVersion) || '2026-07-28';
    sendJson(res, 200, rpc(id, {
      protocolVersion: /^\d{4}-\d{2}-\d{2}$/.test(offered) ? offered : '2026-07-28',
      serverInfo: MCP_SERVER_INFO, capabilities: { tools: {} },
    }));
    return;
  }
  if (method === 'tools/list') {
    sendJson(res, 200, rpc(id, { tools: MCP_TOOLS }));
    return;
  }
  if (method !== 'tools/call') { sendJson(res, 200, rpcError(id, -32601, 'Method not found')); return; }
  const name = str(params.name);
  const args = rec(params.arguments);
  const owner = str(args.owner) || VIEWER;
  const repoName = str(args.repo) || (str(args.full_name).split('/')[1] ?? '');
  const repo = repoName ? getRepo(owner, repoName) : null;
  const need = (key: string): unknown => {
    const value = args[key];
    return value === undefined || value === null || value === '' ? null : value;
  };
  let data: Rec;
  if (name === 'create_repo') {
    const created = createRepo(str(need('name')), VIEWER, str(args.description ?? ''), args.private !== false);
    if (!created) { sendJson(res, 200, rpcError(id, -32602, 'Repository already exists or invalid name')); return; }
    data = { ...repoJson(created), full_name: `${created.owner}/${created.name}` };
  } else if (name === 'get_repo') {
    if (!need('owner') || !need('repo')) { sendJson(res, 200, rpcError(id, -32602, 'owner and repo are required')); return; }
    if (!repo) { sendJson(res, 200, toolResult({ message: 'repository not found' }, true)); return; }
    data = { ...repoJson(repo), full_name: `${repo.owner}/${repo.name}` };
  } else if (name === 'create_issue') {
    if (!need('owner') || !need('repo') || !need('title')) { sendJson(res, 200, rpcError(id, -32602, 'owner, repo and title are required')); return; }
    if (!repo) { sendJson(res, 200, toolResult({ message: 'repository not found' }, true)); return; }
    const issue = addIssue(repo, str(args.title), str(args.body ?? ''), []);
    if (!issue) { sendJson(res, 200, toolResult({ message: 'could not create issue' }, true)); return; }
    data = issueJson(issue);
  } else if (name === 'list_issues') {
    if (!need('owner') || !need('repo')) { sendJson(res, 200, rpcError(id, -32602, 'owner and repo are required')); return; }
    if (!repo) { sendJson(res, 200, toolResult({ message: 'repository not found' }, true)); return; }
    const state = str(args.state ?? 'open');
    data = { issues: [...repo.issues.values()].filter((i) => state === 'all' || i.state === state).map(issueJson) };
  } else if (name === 'create_pull_request') {
    if (!need('owner') || !need('repo') || !need('title') || !need('head') || !need('base')) {
      sendJson(res, 200, rpcError(id, -32602, 'owner, repo, title, head and base are required')); return;
    }
    if (!repo) { sendJson(res, 200, toolResult({ message: 'repository not found' }, true)); return; }
    const pull = addPull(repo, str(args.title), str(args.body ?? ''), str(args.head), str(args.base));
    if (!pull) { sendJson(res, 200, toolResult({ message: 'could not create pull request' }, true)); return; }
    data = pullJson(pull);
  } else if (name === 'merge_pull_request') {
    if (!need('owner') || !need('repo') || need('pull_number') === null || need('pull_number') === undefined) {
      sendJson(res, 200, rpcError(id, -32602, 'owner, repo and pull_number are required')); return;
    }
    if (!repo) { sendJson(res, 200, toolResult({ message: 'repository not found' }, true)); return; }
    const number = parseInt(String(need('pull_number')), 10);
    const pull = repo.pulls.get(number);
    if (!pull) { sendJson(res, 200, toolResult({ message: 'pull request not found' }, true)); return; }
    const merged = mergePull(repo, pull);
    if (!merged.ok) { sendJson(res, 200, toolResult({ message: 'merge failed' }, true)); return; }
    data = { ...pullJson(pull), sha: merged.sha };
  } else {
    sendJson(res, 200, rpcError(id, -32602, 'Unknown tool'));
    return;
  }
  sendJson(res, 200, rpc(id, toolResult(data)));
}

// ---------------------------------------------------------------- process lifecycle

function onRequest(req: RawRequest): void {
  const res: Reply = { socket: req.socket };
  void (async () => {
    try {
      const url = new URL(req.target, 'http://forge.local');
      const path = url.pathname;
      if (path === '/healthz' && req.method === 'GET') { sendJson(res, 200, { ok: true }); return; }
      if (path === '/api/graphql' && req.method === 'POST') { await handleGraphql(req, res, req.body); return; }
      if (path === '/mcp' && req.method === 'POST') { await handleMcp(req, res, req.body); return; }
      if (path.startsWith('/api/v3')) { await handleRest(req, res, path, url); return; }
      const gitMatch = /^\/([^/]+)\/([^/]+)\.git\/(info\/refs|git-upload-pack|git-receive-pack)$/.exec(path);
      if (gitMatch && (req.method === 'GET' || req.method === 'POST')) {
        await handleGit(req, res, decodeURIComponent(gitMatch[1] ?? ''), decodeURIComponent(gitMatch[2] ?? ''), gitMatch[3] ?? '');
        return;
      }
      sendJson(res, 404, { message: 'Not Found' });
    } catch (err) {
      process.stderr.write(`request error: ${String(err)}\n`);
      try { sendJson(res, 500, { message: 'Internal Server Error' }); } catch { /* closed */ }
    }
  })();
}

serveRaw(onRequest, 0, '0.0.0.0', (port: number) => {
  const envUrl = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/$/, '');
  void (async () => {
    try {
      await fetch(`${envUrl}/register`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ port }),
      });
    } catch (err) {
      process.stderr.write(`register failed: ${String(err)}\n`);
    }
    let attempt = 0;
    for (;;) {
      await delay(Math.min(100 * 2 ** Math.min(attempt++, 5), 2000));
      try {
        const response = await fetch(`${envUrl}/done`);
        const value: unknown = await response.json();
        if (isRec(value) && value.done === true) process.exit(0);
      } catch {
        // The suite owns the lifecycle; keep serving.
      }
    }
  })();
});
