# pool

A minimal nostr relay pool. **Zero dependencies. No build step.** One file: [`pool.js`](pool.js).

Part of [nostr-client](https://github.com/nostr-client) — a modular, composable
nostr client where each repo does one thing.

**Live demo:** https://nostr-client.github.io/pool/

## Use

```js
import { Pool, defaultPool, DEFAULT_RELAYS } from 'https://nostr-client.github.io/pool/pool.js'
```

```js
const pool = defaultPool() // shared page-wide singleton — components should prefer this

// live subscription, deduped across relays
const sub = pool.subscribe([{ kinds: [1], limit: 20 }], {
  onEvent: (event, relay) => console.log(event),
  onEose: () => console.log('caught up'),
})
sub.close()

// one-shot queries
const events = await pool.list([{ kinds: [1], authors: [pk] }])   // newest first
const newest = await pool.get({ kinds: [0], authors: [pk] })      // single event or null

// publish a signed event; per-relay results
const results = await pool.publish(signedEvent)
// [{ relay: 'wss://relay.damus.io', ok: true, message: '' }, …]

// custom relays
const mine = new Pool(['wss://relay.example.com'])
```

## Why a singleton?

A page composed from many independent components (login, feed, profile editor…)
should still open **one** websocket per relay, not one per component.
`defaultPool()` gives every component on the page the same pool. Swap the
implementation by setting `globalThis.__nostrClientPool` before components
load, or hand any component its own pool.

## What it does

- lazy connect, auto-reconnect with exponential backoff, auto-resubscribe
- `REQ`/`EVENT`/`EOSE`/`OK`/`CLOSED` handling per [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md)
- event dedup by id across relays
- per-relay publish acknowledgements with timeouts

## What it deliberately doesn't do

- **No signature verification** — that's a signer/verifier concern, not transport.
- **No caching or persistence** — a cache component can wrap the pool.
- **No key handling** — see [login](https://github.com/nostr-client/login).

## License

AGPL-3.0-or-later
