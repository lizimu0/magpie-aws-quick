import { test } from 'node:test'
import assert from 'node:assert/strict'
import { startFakeQuick } from './helpers/fake-quick.mjs'
import { QuickSocket, QuickWsError, classifyErrorKind } from '../lib/quickws.mjs'

test('classifyErrorKind maps auth, quota, and missing_thread errors', () => {
  assert.equal(classifyErrorKind('UnauthorizedException', 'nope'), 'auth')
  assert.equal(classifyErrorKind('', 'expired token'), 'auth')
  assert.equal(classifyErrorKind('ThrottlingException', 'slow down'), 'quota')
  assert.equal(classifyErrorKind('', 'Rate limit exceeded'), 'quota')
  assert.equal(classifyErrorKind('ThreadNotFoundException', ''), 'missing_thread')
  assert.equal(classifyErrorKind('', 'Thread does not exist'), 'missing_thread')
  assert.equal(classifyErrorKind('Other', 'boom', 'stream'), 'stream')
})

test('QuickSocket keeps active concurrent turn alive when a second turn arrives with a rotated token', async () => {
  let releaseFirst
  const firstReady = new Promise((r) => { releaseFirst = r })
  const fake = await startFakeQuick({
    script: async (payload) => {
      if (payload.text === 'first') {
        await firstReady
        return [
          { type: 'text_delta', payload: { delta: 'ans-1' } },
          { type: 'message_complete', payload: { text: 'ans-1' } },
        ]
      }
      return [
        { type: 'text_delta', payload: { delta: 'ans-2' } },
        { type: 'message_complete', payload: { text: 'ans-2' } },
      ]
    },
  })
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: fake.wsHost, id_token: 'tok-1' }),
    outpostId: 'dev-1',
    log: () => {},
  })
  let releaseFirstConnect
  let markFirstConnectReady
  const firstConnectGate = new Promise((r) => { releaseFirstConnect = r })
  const firstConnectReady = new Promise((r) => { markFirstConnectReady = r })
  const originalConnect = sock.connect.bind(sock)
  let pauseFirstConnect = true
  sock.connect = async (session) => {
    const ws = await originalConnect(session)
    if (pauseFirstConnect) {
      pauseFirstConnect = false
      markFirstConnectReady()
      await firstConnectGate
    }
    return ws
  }
  try {
    const collect = async (threadId, text, session) => {
      let out = ''
      for await (const evt of sock.turn(threadId, { text, agent_mode: 'fast' }, { session })) {
        if (evt.type === 'text_delta') out += evt.payload.delta
      }
      return out
    }
    const p1 = collect('t1', 'first', { ws_host: fake.wsHost, id_token: 'tok-1' }).then(
      (value) => ({ value }),
      (error) => ({ error }),
    )
    await firstConnectReady
    const p2 = collect('t2', 'second', { ws_host: fake.wsHost, id_token: 'tok-2' })
    assert.equal(await p2, 'ans-2')
    releaseFirstConnect()
    releaseFirst()
    const first = await p1
    if (first.error) throw first.error
    assert.equal(first.value, 'ans-1')
    assert.equal(fake.state.connections, 1)

    const ans3 = await collect('t3', 'third', { ws_host: fake.wsHost, id_token: 'tok-2' })
    assert.equal(ans3, 'ans-2')
    assert.equal(fake.state.connections, 2)
  } finally {
    releaseFirstConnect?.()
    await sock.close()
    await fake.close()
  }
})

test('QuickSocket preserves an active registered turn when a rotated token arrives', async () => {
  let releaseFirst
  const firstReady = new Promise((r) => { releaseFirst = r })
  let markFirstStarted
  const firstStarted = new Promise((r) => { markFirstStarted = r })
  const fake = await startFakeQuick({
    script: async (payload) => {
      if (payload.text === 'first') {
        markFirstStarted()
        await firstReady
      }
      return [
        { type: 'text_delta', payload: { delta: `ans:${payload.text}` } },
        { type: 'message_complete', payload: { text: `ans:${payload.text}` } },
      ]
    },
  })
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: fake.wsHost, id_token: 'tok-1' }),
    outpostId: 'dev-1-active',
    log: () => {},
  })
  const collect = async (threadId, text, session) => {
    let out = ''
    for await (const evt of sock.turn(threadId, { text, agent_mode: 'fast' }, { session })) {
      if (evt.type === 'text_delta') out += evt.payload.delta
    }
    return out
  }
  const p1 = collect('t1', 'first', { ws_host: fake.wsHost, id_token: 'tok-1' }).then(
    (value) => ({ value }),
    (error) => ({ error }),
  )
  try {
    await firstStarted
    assert.equal(sock.waiters.has('t1'), true)

    assert.equal(await collect('t2', 'second', { ws_host: fake.wsHost, id_token: 'tok-2' }), 'ans:second')
    assert.equal(sock.waiters.has('t1'), true)
    assert.equal(fake.state.connections, 1)

    releaseFirst()
    const first = await p1
    if (first.error) throw first.error
    assert.equal(first.value, 'ans:first')

    assert.equal(await collect('t3', 'third', { ws_host: fake.wsHost, id_token: 'tok-2' }), 'ans:third')
    assert.equal(fake.state.connections, 2)
  } finally {
    releaseFirst?.()
    await sock.close()
    await fake.close()
  }
})

test('QuickSocket releases acquiring count after a pending connect is aborted', async () => {
  const fake = await startFakeQuick()
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: fake.wsHost, id_token: 'tok-1' }),
    outpostId: 'dev-1-abort',
    log: () => {},
  })
  const originalConnect = sock.connect.bind(sock)
  const collect = async (threadId, text, session) => {
    let out = ''
    for await (const evt of sock.turn(threadId, { text, agent_mode: 'fast' }, { session })) {
      if (evt.type === 'text_delta') out += evt.payload.delta
    }
    return out
  }
  try {
    assert.equal(await collect('t1', 'first', { ws_host: fake.wsHost, id_token: 'tok-1' }), 'echo:first')
    assert.equal(fake.state.connections, 1)

    let markConnectStarted
    const connectStarted = new Promise((r) => { markConnectStarted = r })
    sock.connect = async () => {
      markConnectStarted()
      return new Promise(() => {})
    }
    const controller = new AbortController()
    const pending = sock.turn('t2', { text: 'aborted', agent_mode: 'fast' }, {
      session: { ws_host: fake.wsHost, id_token: 'tok-2' },
      signal: controller.signal,
    }).next()
    await connectStarted
    assert.equal(sock.acquiring, 1)
    controller.abort()
    await assert.rejects(pending, (err) => err instanceof QuickWsError && err.kind === 'abort')
    assert.equal(sock.acquiring, 0)

    sock.connect = originalConnect
    assert.equal(await collect('t2', 'recovered', { ws_host: fake.wsHost, id_token: 'tok-2' }), 'echo:recovered')
    assert.equal(fake.state.connections, 2)
  } finally {
    sock.connect = originalConnect
    await sock.close()
    await fake.close()
  }
})

test('QuickSocket releases acquiring count after connect rejects', async () => {
  const fake = await startFakeQuick()
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: fake.wsHost, id_token: 'tok-1' }),
    outpostId: 'dev-1-reject',
    log: () => {},
  })
  const originalConnect = sock.connect.bind(sock)
  const collect = async (threadId, text, session) => {
    let out = ''
    for await (const evt of sock.turn(threadId, { text, agent_mode: 'fast' }, { session })) {
      if (evt.type === 'text_delta') out += evt.payload.delta
    }
    return out
  }
  try {
    assert.equal(await collect('t1', 'first', { ws_host: fake.wsHost, id_token: 'tok-1' }), 'echo:first')
    assert.equal(fake.state.connections, 1)

    sock.connect = async () => { throw new QuickWsError('connect rejected', { kind: 'ws' }) }
    await assert.rejects(
      collect('t2', 'rejected', { ws_host: fake.wsHost, id_token: 'tok-2' }),
      (err) => err instanceof QuickWsError && err.kind === 'ws',
    )
    assert.equal(sock.acquiring, 0)

    sock.connect = originalConnect
    assert.equal(await collect('t2', 'recovered', { ws_host: fake.wsHost, id_token: 'tok-2' }), 'echo:recovered')
    assert.equal(fake.state.connections, 2)
  } finally {
    sock.connect = originalConnect
    await sock.close()
    await fake.close()
  }
})

test('QuickSocket disconnects zombie socket on silent idle timeout and reconnects on next turn', async () => {
  let count = 0
  const fake = await startFakeQuick({
    script: (payload) => {
      count++
      if (count === 1) return new Promise(() => {}) // simulate silent stall
      return [
        { type: 'text_delta', payload: { delta: `ok:${payload.text}` } },
        { type: 'message_complete', payload: { text: `ok:${payload.text}` } },
      ]
    },
  })
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: fake.wsHost, id_token: 'tok-1' }),
    outpostId: 'dev-2',
    extra: { idleMs: 150 },
    log: () => {},
  })
  try {
    await assert.rejects(async () => {
      for await (const _evt of sock.turn('t1', { text: 'stall', agent_mode: 'fast' })) {}
    }, (err) => err instanceof QuickWsError && err.kind === 'timeout')

    let text = ''
    for await (const evt of sock.turn('t2', { text: 'recovered', agent_mode: 'fast' })) {
      if (evt.type === 'text_delta') text += evt.payload.delta
    }
    assert.equal(text, 'ok:recovered')
    assert.equal(fake.state.connections, 2) // zombie socket was torn down and reconnected
  } finally {
    await sock.close()
    await fake.close()
  }
})

test('QuickSocket classifies handshake and stream error frames with code + kind', async () => {
  const sock = new QuickSocket({
    getSession: async () => ({ ws_host: 'ws://127.0.0.1:1', id_token: 't' }),
    outpostId: 'dev-3',
    log: () => {},
  })
  let caught
  sock._sessionFail = (e) => { caught = e }
  sock._dispatch({ type: 'error', code: 'UnauthorizedException', message: 'expired token' })
  assert.ok(caught instanceof QuickWsError)
  assert.equal(caught.kind, 'auth')
  assert.equal(caught.code, 'UnauthorizedException')
  assert.match(caught.message, /expired token/)
})
