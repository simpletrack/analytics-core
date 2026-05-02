import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';

const trackerSource = await readFile(new URL('./tracker.js', import.meta.url), 'utf8');

test('auto pageview sends the collect request shape', async () => {
  const { requests } = loadTracker();

  await flushTimers();

  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, 'https://collector.example/collect');
  assert.equal(requests[0].body.tenant_id, 'ten_demo');
  assert.equal(requests[0].body.project_id, 'prj_docs');
  assert.equal(requests[0].body.source_id, 'src_docs');
  assert.equal(requests[0].body.source_type, 'web');
  assert.equal(requests[0].body.event_name, 'pageview');
  assert.match(requests[0].body.id, /^evt_/);
  assert.match(requests[0].body.distinct_id, /^dst_/);
  assert.equal(requests[0].body.properties['page.path'], '/docs');
  assert.equal(requests[0].body.properties['page.hostname'], 'app.example');
  assert.equal(requests[0].body.properties.language, 'en-US');
});

test('manual track sends event properties without nested values', async () => {
  const { window, requests } = loadTracker({ 'data-auto-track': 'false' });

  await window.simpletrack.track('signup_started', {
    plan: 'pro',
    nested: { ignored: true },
    price: 29,
    trial: true,
  });

  assert.equal(requests.length, 1);
  assert.equal(requests[0].body.event_name, 'signup_started');
  assert.equal(requests[0].body.properties.plan, 'pro');
  assert.equal(requests[0].body.properties.price, 29);
  assert.equal(requests[0].body.properties.trial, true);
  assert.equal(requests[0].body.properties.nested, undefined);
});

test('identify updates the distinct id and sends user properties', async () => {
  const { window, requests } = loadTracker({ 'data-auto-track': 'false' });

  await window.simpletrack.identify('user_123', {
    role: 'admin',
    active: true,
  });

  assert.equal(requests.length, 1);
  assert.equal(requests[0].body.event_name, 'identify');
  assert.equal(requests[0].body.distinct_id, 'user_123');
  assert.deepEqual(requests[0].body.user_properties, {
    role: 'admin',
    active: true,
  });
});

test('identify persists the distinct id for later page loads', async () => {
  const storage = new Map();
  const firstLoad = loadTracker({ 'data-auto-track': 'false' }, {}, storage);
  await firstLoad.window.simpletrack.identify('user_123', { role: 'admin' });

  const secondLoad = loadTracker({}, {}, storage);
  await flushTimers();

  assert.equal(secondLoad.requests.length, 1);
  assert.equal(secondLoad.requests[0].body.event_name, 'pageview');
  assert.equal(secondLoad.requests[0].body.distinct_id, 'user_123');
});

test('storage failures fall back to an in-memory distinct id', async () => {
  const throwingStorage = {
    getItem() {
      throw new Error('blocked');
    },
    setItem() {
      throw new Error('blocked');
    },
  };

  const { requests } = loadTracker({}, { localStorage: throwingStorage });

  await flushTimers();

  assert.equal(requests.length, 1);
  assert.match(requests[0].body.distinct_id, /^dst_/);
});

test('invalid manual track event names are not converted to pageviews', async () => {
  const { window, requests } = loadTracker({ 'data-auto-track': 'false' });

  const result = await window.simpletrack.track('');
  await window.simpletrack.track('bad event name');

  assert.equal(result, null);
  assert.equal(requests.length, 0);
});

test('do not track suppresses sends only when explicitly enabled', async () => {
  const dntStorage = new Map();
  const optedOut = loadTracker(
    { 'data-do-not-track': 'true' },
    {
      navigator: {
        doNotTrack: '1',
        language: 'en-US',
      },
    },
    dntStorage,
  );

  await flushTimers();
  await optedOut.window.simpletrack.track('signup_started');
  await optedOut.window.simpletrack.identify('user_123', { role: 'admin' });

  const defaultBehavior = loadTracker(
    {},
    {
      navigator: {
        doNotTrack: '1',
        language: 'en-US',
      },
    },
  );

  await flushTimers();

  assert.equal(optedOut.requests.length, 0);
  assert.equal(dntStorage.size, 0);
  assert.equal(defaultBehavior.requests.length, 1);
  assert.equal(defaultBehavior.requests[0].body.event_name, 'pageview');
});

test('history changes trigger pageviews when the URL changes', async () => {
  const { window, requests } = loadTracker();

  await flushTimers();
  window.history.pushState({}, '', '/pricing');
  await flushTimers();

  assert.equal(requests.length, 2);
  assert.equal(requests[1].body.event_name, 'pageview');
  assert.equal(requests[1].body.properties['page.path'], '/pricing');
});

test('queued snippet calls replay after the SDK loads', async () => {
  const queued = { q: [['track', 'queued_event', { source: 'snippet' }]] };
  const { requests } = loadTracker({ 'data-auto-track': 'false' }, { simpletrack: queued });

  await flushTimers();

  assert.equal(requests.length, 1);
  assert.equal(requests[0].body.event_name, 'queued_event');
  assert.equal(requests[0].body.properties.source, 'snippet');
});

test('missing required identifiers disables sending without throwing', async () => {
  const { window, requests } = loadTracker({
    'data-tenant-id': '',
    'data-auto-track': 'false',
  });

  await window.simpletrack.track('signup_started');

  assert.equal(requests.length, 0);
});

function loadTracker(attrs = {}, overrides = {}, sharedStorage) {
  const scriptAttrs = {
    'data-tenant-id': 'ten_demo',
    'data-project-id': 'prj_docs',
    'data-source-id': 'src_docs',
    'data-collect-url': 'https://collector.example/collect',
    ...attrs,
  };
  const requests = [];
  const listeners = new Map();
  const storage = sharedStorage || new Map();
  const window = {
    console,
    crypto: {
      getRandomValues(bytes) {
        for (let index = 0; index < bytes.length; index += 1) {
          bytes[index] = (index + 1) % 256;
        }
        return bytes;
      },
    },
    document: {
      currentScript: {
        src: 'https://cdn.example/tracker.js',
        getAttribute(name) {
          return scriptAttrs[name] ?? '';
        },
      },
      referrer: 'https://referrer.example/',
      readyState: 'complete',
      title: 'Docs',
      addEventListener(name, callback) {
        listeners.set(name, callback);
      },
    },
    fetch(url, options) {
      requests.push({
        url,
        options,
        body: JSON.parse(options.body),
      });
      return Promise.resolve({ ok: true, status: 202 });
    },
    history: {
      pushState(_state, _title, url) {
        if (url) {
          window.location = new URL(url, window.location.href);
        }
      },
      replaceState(_state, _title, url) {
        if (url) {
          window.location = new URL(url, window.location.href);
        }
      },
    },
    localStorage: {
      getItem(key) {
        return storage.get(key) || null;
      },
      setItem(key, value) {
        storage.set(key, value);
      },
    },
    location: new URL('https://app.example/docs?utm=1#top'),
    navigator: {
      language: 'en-US',
    },
    screen: {
      width: 1440,
      height: 900,
    },
    setTimeout,
    addEventListener(name, callback) {
      listeners.set(name, callback);
    },
    URL,
    ...overrides,
  };
  window.window = window;

  vm.runInNewContext(trackerSource, { window, URL, Uint8Array, Math, Number, Date, Promise });
  return { listeners, requests, window };
}

function flushTimers() {
  return new Promise(resolve => setTimeout(resolve, 10));
}
