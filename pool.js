/**
 * pool.js — minimal nostr relay pool. Zero dependencies, no build step.
 *
 * Part of https://github.com/nostr-client — one repo, one thing.
 * License: AGPL-3.0-or-later
 *
 * Usage:
 *   import { Pool, defaultPool } from 'https://nostr-client.github.io/pool/pool.js'
 *
 *   const pool = defaultPool()                       // shared singleton
 *   const sub = pool.subscribe([{ kinds: [1], limit: 20 }], {
 *     onEvent: (ev, relay) => console.log(ev),
 *     onEose: () => console.log('caught up'),
 *   })
 *   sub.close()
 *
 *   const events = await pool.list([{ kinds: [0], authors: [pk] }])
 *   const newest = await pool.get({ kinds: [0], authors: [pk] })
 *   const results = await pool.publish(signedEvent)  // [{ relay, ok, message }]
 */

export const DEFAULT_RELAYS = [
  'wss://relay.primal.net',
  'wss://nos.lol',
  'wss://relay.nostr.band',
]

let nextSubId = 0
const makeSubId = () => 'nc' + (nextSubId++).toString(36) + Math.random().toString(36).slice(2, 8)

const normalizeUrl = (url) => {
  const u = new URL(url)
  if (u.protocol !== 'wss:' && u.protocol !== 'ws:') throw new Error('relay url must be ws(s)://: ' + url)
  return u.href.replace(/\/$/, '')
}

/** A single relay connection with auto-reconnect and resubscribe. */
export class Relay {
  constructor(url) {
    this.url = normalizeUrl(url)
    this.ws = null
    this.status = 'closed' // closed | connecting | open
    this.attempts = 0
    this.closedByUser = false
    this.subs = new Map()      // subId -> { filters, onEvent, onEose, onClosed }
    this.publishes = new Map() // eventId -> resolve({ ok, message })
    this.sendQueue = []
  }

  connect() {
    if (this.status !== 'closed' || this.closedByUser) return
    this.status = 'connecting'
    let ws
    try {
      ws = new WebSocket(this.url)
    } catch (err) {
      this.status = 'closed'
      this._scheduleReconnect()
      return
    }
    this.ws = ws
    ws.onopen = () => {
      if (this.closedByUser) { ws.close(); return }
      this.status = 'open'
      this.attempts = 0
      for (const [id, sub] of this.subs) this._send(['REQ', id, ...sub.filters])
      for (const msg of this.sendQueue.splice(0)) this._send(msg)
    }
    ws.onmessage = (e) => this._onMessage(e.data)
    ws.onerror = () => { /* onclose always follows */ }
    ws.onclose = () => {
      this.status = 'closed'
      this.ws = null
      if (!this.closedByUser) this._scheduleReconnect()
    }
  }

  _scheduleReconnect() {
    if (this.closedByUser) return
    if (this.subs.size === 0 && this.sendQueue.length === 0) return // nothing to do; reconnect lazily
    const delay = Math.min(30_000, 1000 * 2 ** Math.min(this.attempts++, 5))
    setTimeout(() => this.connect(), delay)
  }

  _send(msg) {
    if (this.status === 'open') this.ws.send(JSON.stringify(msg))
    else { this.sendQueue.push(msg); this.connect() }
  }

  _onMessage(data) {
    let msg
    try { msg = JSON.parse(data) } catch { return }
    if (!Array.isArray(msg)) return
    const [type, ...rest] = msg
    if (type === 'EVENT') {
      const [subId, event] = rest
      this.subs.get(subId)?.onEvent?.(event)
    } else if (type === 'EOSE') {
      this.subs.get(rest[0])?.onEose?.()
    } else if (type === 'OK') {
      const [eventId, ok, message = ''] = rest
      const resolve = this.publishes.get(eventId)
      if (resolve) { this.publishes.delete(eventId); resolve({ ok: !!ok, message }) }
    } else if (type === 'CLOSED') {
      const [subId, message = ''] = rest
      const sub = this.subs.get(subId)
      if (sub) { this.subs.delete(subId); sub.onClosed?.(message) }
    }
    // NOTICE and unknown types are ignored
  }

  subscribe(subId, filters, handlers) {
    this.subs.set(subId, { filters, ...handlers })
    this._send(['REQ', subId, ...filters])
  }

  unsubscribe(subId) {
    if (!this.subs.has(subId)) return
    this.subs.delete(subId)
    if (this.status === 'open') this.ws.send(JSON.stringify(['CLOSE', subId]))
  }

  publish(event, timeout = 5000) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.publishes.delete(event.id)
        resolve({ ok: false, message: 'timeout' })
      }, timeout)
      this.publishes.set(event.id, (result) => { clearTimeout(timer); resolve(result) })
      this._send(['EVENT', event])
    })
  }

  close() {
    this.closedByUser = true
    this.subs.clear()
    this.sendQueue.length = 0
    this.ws?.close()
  }
}

/** A pool of relays: subscribe/list/get/publish across all of them, deduped by event id. */
export class Pool {
  constructor(urls = DEFAULT_RELAYS) {
    this.relays = new Map()
    for (const url of urls) this.addRelay(url)
  }

  get urls() { return [...this.relays.keys()] }

  addRelay(url) {
    url = normalizeUrl(url)
    if (!this.relays.has(url)) this.relays.set(url, new Relay(url))
    return this.relays.get(url)
  }

  removeRelay(url) {
    url = normalizeUrl(url)
    this.relays.get(url)?.close()
    this.relays.delete(url)
  }

  _targets(relays) {
    if (!relays) return [...this.relays.values()]
    return relays.map((u) => this.addRelay(u))
  }

  /**
   * Subscribe to filters on all (or given) relays. Events are deduped by id.
   * Returns { close() }.
   */
  subscribe(filters, { onEvent, onEose, relays } = {}) {
    if (!Array.isArray(filters)) filters = [filters]
    const targets = this._targets(relays)
    const subId = makeSubId()
    const seen = new Set()
    let eosed = 0
    let eoseFired = false
    let graceTimer = null
    const fireEose = () => {
      if (eoseFired) return
      eoseFired = true
      clearTimeout(graceTimer)
      onEose?.()
    }
    const oneDone = () => {
      if (++eosed >= targets.length) fireEose()
      // don't let one slow/down relay hold everything hostage: once the first
      // relay finishes, give the rest a short grace period, then move on
      else if (!graceTimer) graceTimer = setTimeout(fireEose, 2500)
    }
    for (const relay of targets) {
      relay.subscribe(subId, filters, {
        onEvent: (event) => {
          if (seen.has(event.id)) return
          seen.add(event.id)
          onEvent?.(event, relay.url)
        },
        onEose: oneDone,
        onClosed: oneDone,
      })
    }
    // Fire EOSE even if no relay ever answers.
    setTimeout(fireEose, 8000)
    return { close: () => targets.forEach((r) => r.unsubscribe(subId)) }
  }

  /** Fetch matching events until EOSE (or timeout). Sorted newest first. */
  list(filters, { relays, timeout = 8000 } = {}) {
    return new Promise((resolve) => {
      const events = []
      let done = false
      const finish = () => {
        if (done) return
        done = true
        sub.close()
        resolve(events.sort((a, b) => b.created_at - a.created_at))
      }
      const sub = this.subscribe(filters, { relays, onEvent: (ev) => events.push(ev), onEose: finish })
      setTimeout(finish, timeout)
    })
  }

  /** Fetch the single newest event matching a filter, or null. */
  async get(filter, opts = {}) {
    const events = await this.list([{ ...filter, limit: filter.limit ?? 1 }], opts)
    return events[0] ?? null
  }

  /** Publish a signed event to all (or given) relays. Resolves per-relay results. */
  publish(event, { relays, timeout = 5000 } = {}) {
    const targets = this._targets(relays)
    return Promise.all(
      targets.map((relay) =>
        relay.publish(event, timeout).then((r) => ({ relay: relay.url, ...r }))
      )
    )
  }

  close() {
    for (const relay of this.relays.values()) relay.close()
    this.relays.clear()
  }
}

/**
 * Shared singleton pool. Components should default to this so a page full of
 * independent components still opens one connection per relay, not N.
 * Swap it wholesale by setting globalThis.__nostrClientPool before components load,
 * or per-component by passing your own pool/relays.
 */
export function defaultPool() {
  return (globalThis.__nostrClientPool ??= new Pool())
}
