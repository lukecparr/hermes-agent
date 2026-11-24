/* Disposable integration: real Second Brain receiver/dispatcher, mocked Firebase. */
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { resolve } from 'node:path';

async function main() {
  const sb = process.env.PIPER_TEST_SB_REPO!;
  const repo = process.env.PIPER_TEST_HERMES_REPO!;
  const [{default: prisma}, {handlePiperCompletion}, {dispatchPiperBatch}] = await Promise.all([
    import(`${sb}/data/prisma/index.ts`),
    import(`${sb}/app/notifications/services/piper-completion-receiver.service.ts`),
    import(`${sb}/app/notifications/services/piper-completion-dispatch.service.ts`),
  ]);
  await prisma.deviceToken.create({data:{id:'synthetic-enabled',fcmToken:'synthetic-token',platform:'android',piperTurnPush:true}});
  await prisma.deviceToken.create({data:{id:'synthetic-disabled',fcmToken:'synthetic-disabled-token',platform:'android',piperTurnPush:false}});
  const statuses: number[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(Buffer.from(c));
    const response = await handlePiperCompletion(new Request('http://127.0.0.1/', {
      method:'POST', headers:{authorization:req.headers.authorization ?? ''}, body:Buffer.concat(chunks),
    }));
    statuses.push(response.status);
    res.writeHead(response.status); res.end();
  });
  await new Promise<void>((resolveListen) => server.listen(0, '127.0.0.1', resolveListen));
  try {
    const port = (server.address() as {port:number}).port;
    const child = spawn(process.env.PIPER_TEST_PYTHON!, [resolve(repo, 'tests/gateway/piper_integration_authority.py'),
      `http://127.0.0.1:${port}/`], {stdio:'inherit', env:process.env});
    const code = await new Promise<number | null>((resolveExit, reject) => {
      child.on('error', reject); child.on('exit', resolveExit);
    });
    assert.equal(code, 0);
    assert.deepEqual(statuses, [204]);
    const events = await prisma.piperCompletionEvent.findMany();
    assert.equal(events.length, 1);
    const deliveries = await prisma.piperCompletionDelivery.findMany();
    assert.equal(deliveries.length, 1);
    assert.equal(deliveries[0].deviceId, 'synthetic-enabled');
    const sends: any[] = [];
    const fake = {send: async (message: any) => { sends.push(message); return 'mock-accepted'; }};
    await dispatchPiperBatch(new Date(), fake as any);
    assert.equal(sends.length, 1);
    assert.deepEqual(sends[0].data, {type:'piper.turn.completed.v1', event_id:events[0].id});
    assert.equal((await prisma.piperCompletionDelivery.findFirstOrThrow()).state, 'sent');
    console.log('PASS authority SQLite -> HTTP -> real SB receiver -> disposable PGlite -> mock FCM', process.env.PIPER_TEST_RECOVERY === '1' ? 'captured-recovery' : 'normal');
  } finally {
    await new Promise<void>((done) => server.close(() => done()));
    await prisma.$disconnect();
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
