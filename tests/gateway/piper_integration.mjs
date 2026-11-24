/* Optional isolated integration. Requires existing SB dependencies and PGlite; installs nothing.
 * Set PIPER_TEST_SB_REPO, PIPER_TEST_PGLITE_MODULES, PIPER_TEST_PYTHON. */
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const sb = resolve(process.env.PIPER_TEST_SB_REPO || '');
const modules = resolve(process.env.PIPER_TEST_PGLITE_MODULES || '');
const python = process.env.PIPER_TEST_PYTHON;
if (!process.env.PIPER_TEST_SB_REPO || !process.env.PIPER_TEST_PGLITE_MODULES || !python)
  throw new Error('PIPER_TEST_SB_REPO, PIPER_TEST_PGLITE_MODULES and PIPER_TEST_PYTHON are required');
const {PGlite} = await import(pathToFileURL(join(modules, '@electric-sql/pglite/dist/index.js')));
const {PGLiteSocketServer} = await import(pathToFileURL(join(modules, '@electric-sql/pglite-socket/dist/index.js')));
const temp = await mkdtemp(join(repo, '.i-'));
async function availablePort() {
  const s = createServer();
  await new Promise((ok, reject) => {s.on('error', reject); s.listen(0, '127.0.0.1', ok);});
  const port = s.address().port;
  await new Promise(ok => s.close(ok));
  return port;
}
try {
  for (const recovery of [false, true]) {
    const {mkdir} = await import('node:fs/promises');
    const home = join(temp, recovery ? 'recovery' : 'normal');
    await mkdir(home);
    const db = new PGlite();
    await db.exec(`CREATE TABLE device_tokens (id TEXT PRIMARY KEY, "fcmToken" TEXT UNIQUE NOT NULL,
      "deviceId" TEXT, platform TEXT NOT NULL, "piperTurnPush" BOOLEAN NOT NULL DEFAULT false,
      "lastActive" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "createdAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      "updatedAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);`);
    for (const migration of ['20261123000000_piper_completion_inbox', '20261123000002_piper_delivery_terminal_at',
                             '20261123000003_piper_registration_fence']) {
      await db.exec(await readFile(join(sb, 'data/prisma/migrations', migration, 'migration.sql'), 'utf8'));
    }
    const port = await availablePort();
    const server = new PGLiteSocketServer({db, port, host:'127.0.0.1'});
    await server.start();
    try {
      const env = {PATH:process.env.PATH, HOME:home, TMPDIR:home, HERMES_HOME:home,
        HERMES_RUNTIME_DIR:join(home, 'runtime'), PIPER_TEST_PYTHON:python,
        PIPER_TEST_SB_REPO:sb, PIPER_TEST_HERMES_REPO:repo, PIPER_TEST_RECOVERY:recovery ? '1' : '0',
        POSTGRES_URL:`postgresql://postgres:postgres@127.0.0.1:${port}/postgres?sslmode=disable&connection_limit=1&pgbouncer=true`,
        PIPER_PUSH_RECEIVER:'1', PIPER_PUSH_INGEST_TOKEN:'x'.repeat(32)};
      const child = spawn(join(sb, 'node_modules/.bin/tsx'), ['--tsconfig', join(sb, 'tsconfig.json'),
        join(repo, 'tests/gateway/piper_integration_receiver.ts')], {cwd:sb, env, stdio:'inherit'});
      const code = await new Promise((ok, reject) => {child.on('error', reject); child.on('exit', ok);});
      assert.equal(code, 0);
    } finally {
      await server.stop();
      await db.close();
    }
  }
} finally {
  await rm(temp, {recursive:true, force:true});
}
