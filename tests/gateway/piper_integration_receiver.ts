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
  const sends: any[] = [];
  const retry = process.env.PIPER_TEST_RETRY === '1';
  let calls = 0;
  const fake = {send: async (message: any) => {
    calls++;
    if (retry && calls === 1) throw new Error('fixture transient FCM failure');
    sends.push(message); return 'mock-accepted';
  }};
  const dispatch = (id: string) => dispatchPiperBatch(new Date(), fake as any, id);
  const statuses: number[] = [];
  const server = createServer(async (req, res) => {
    if (req.url === '/wake') {
      assert.equal(req.headers.authorization, 'Bearer ' + 'x'.repeat(32));
      await dispatchPiperBatch(new Date(), fake as any);
      res.writeHead(204); res.end(); return;
    }
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(Buffer.from(c));
    const response = await handlePiperCompletion(new Request('http://127.0.0.1/', {
      method:'POST', headers:{authorization:req.headers.authorization ?? ''}, body:Buffer.concat(chunks),
    }), undefined, dispatch);
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
    assert.equal(events[0].preview, retry ? 'Hello Luke & hi' : null);
    assert.equal(sends.length, retry ? 0 : 1); // Ingest dispatched before any timer call.
    assert.equal(deliveries[0].state, retry ? 'pending' : 'sent');
    if (retry) await dispatchPiperBatch(new Date(Date.now() + 60_000), fake as any);
    assert.equal(sends.length, 1);
    assert.deepEqual(sends[0].data, {type:'piper.turn.completed.v1', event_id:events[0].id, preview:'Hello Luke & hi'});
    assert.equal((await prisma.piperCompletionDelivery.findFirstOrThrow()).state, 'sent');
    const replay = new Request('http://127.0.0.1/', {method:'POST',
      headers:{authorization:'Bearer ' + 'x'.repeat(32)},
      body:JSON.stringify({event_id:events[0].id,turn_id:events[0].turnId,type:'piper.turn.completed.v1',preview:'replacement'})});
    await Promise.all([handlePiperCompletion(replay, undefined, dispatch), dispatchPiperBatch(new Date(), fake as any)]);
    assert.equal(sends.length, 1);
    assert.equal(await prisma.piperCompletionEvent.count(), 1);
    assert.equal((await prisma.piperCompletionEvent.findFirstOrThrow()).preview, null);
    assert.equal(sends[0].notification, undefined);
    console.log('PASS immediate preview -> SQLite -> HTTP -> real SB -> disposable SQL -> mock FCM; replay/timer dedup;',
      retry ? '60s retry recovery' : process.env.PIPER_TEST_LOCAL_WAKE === '1' ? 'post-ACK local wake' : process.env.PIPER_TEST_RECOVERY === '1' ? 'captured-recovery' : 'streamed-normal');
  } finally {
    await new Promise<void>((done) => server.close(() => done()));
    await prisma.$disconnect();
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
