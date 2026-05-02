# SimpleTrack Browser Tracker

`sdk/browser/tracker.js` is the P1 browser SDK for the analytics-core collect
protocol. It is intentionally dependency-free and framework-neutral so product
apps can host it from a CDN, a static asset route, or a local development server.

## Install

```html
<script
  defer
  data-tenant-id="ten_demo"
  data-project-id="prj_docs"
  data-source-id="src_docs"
  data-collect-url="https://collector.example/collect"
  src="https://cdn.simpletrack.dev/tracker.js"
></script>
```

The SDK sends an automatic `pageview` unless `data-auto-track="false"` is set.
It patches `history.pushState`, `history.replaceState`, and `popstate` so SPA
route changes produce additional pageviews.

## Manual Events

```html
<button onclick="window.simpletrack?.track('signup_started', { plan: 'pro' })">
  Start signup
</button>
```

`track(name, properties)` sends a collect request using the stable P1 fields:
`tenant_id`, `project_id`, `source_id`, `source_type`, `event_name`,
`distinct_id`, `event_time`, `properties`, and `source`.

Pageview and event properties automatically include page metadata plus
allowlisted marketing attribution query parameters such as `utm.source`,
`utm.medium`, `utm.campaign`, `click.gclid`, `click.fbclid`, and
`click.msclkid`. `page.url` is stored without query or hash fragments, and the
SDK does not copy arbitrary query-string fields as separate properties.

## Identify

```html
<script>
  window.simpletrack?.identify('user_123', {
    plan: 'pro',
    role: 'admin'
  });
</script>
```

`identify(id, userProperties)` updates and persists the browser-side
`distinct_id`, then sends an `identify` event with `user_properties`. It does
not add cookies; the current id is stored in `localStorage` when available and
falls back to an in-memory id if storage is blocked.

## Options

| Attribute | Default | Purpose |
| --- | --- | --- |
| `data-tenant-id` | required | Tenant boundary sent to collect. |
| `data-project-id` | required | Project or website boundary sent to collect. |
| `data-source-id` | required | Source boundary inside the project. |
| `data-source-type` | `web` | Collect `source_type`. |
| `data-collect-url` | script origin + `/collect` | POST target for collect. |
| `data-auto-track` | `true` | Set to `false` to disable automatic pageviews. |
| `data-track-history` | `true` | Set to `false` to disable SPA route pageviews. |
| `data-do-not-track` | `false` | Set to `true` to suppress sends when the browser advertises DNT. |
| `data-debug` | `false` | Set to `true` to log send attempts and skipped fields. |
| `data-fetch-credentials` | `omit` | Fetch credentials mode for same-origin deployments. |

## Snippet Queue

Pages may queue calls before the SDK loads:

```html
<script>
  window.simpletrack = window.simpletrack || { q: [] };
  window.simpletrack.q.push(['track', 'signup_started', { plan: 'pro' }]);
</script>
```

When `tracker.js` loads, queued `track`, `pageview`, and `identify` calls are
replayed against the real SDK.
