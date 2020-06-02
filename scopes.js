const ram = require('random-access-memory')
const datEncoding = require('dat-encoding')
const sub = require('subleveldown')
const memdb = require('level-mem')
const collect = require('stream-collector')
const { Transform } = require('stream')
const hypercore = require('hypercore')
const debug = require('debug')('kappa-scope')
const inspect = require('inspect-custom-symbol')
const pretty = require('pretty-hash')
const pump = require('pump')
const mutex = require('mutexify')
const LRU = require('lru-cache')
const Bitfield = require('fast-bitfield')
const hcrypto = require('hypercore-crypto')
const Nanoresource = require('nanoresource/emitter')

const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const Corestore = require('corestore')

const { uuid, through, noop, once } = require('./lib/util')
const { Header } = require('./lib/messages')
const SyncMap = require('./lib/sync-map')

const LEN = Symbol('record-size')
const INFO = Symbol('feed-info')

const MAX_CACHE_SIZE = 16777216 // 16M
const DEFAULT_MAX_BATCH = 500
const DEFAULT_NAMESPACE = 'kappa-group'

const FEED_LOCAL = 'local'
const FEED_TYPE = 'hypercore'

module.exports = function (opts = {}) {
  const scopes = new Scopes(opts)
  const scope = scopes.createScopeWithRootFeed(opts)
  scope.replicate = function (...opts) {
    return scope.corestore.replicate(...opts)
  }
  return scope
}

class Scopes {
  constructor (opts = {}) {
    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    this.db = opts.db || memdb()

    this.scopes = new Map()

    this.scopeStore = new SyncMap(sub(this.db, 's'), { valueEncoding: 'json' })
    // this.feedStore = new SyncMap(sub(this.db, 's'), { valueEncoding: 'json' })
  }

  get (opts = {}) {
    debug('get', opts)
    if (!opts.key) {
      const key = this.corestore.namespace(opts.name).default().key
      opts.key = key
    }
    if (!opts.name) opts.name = encodeKey(opts.key)
    if (this.scopes.has(opts.name)) return this.scopes.get(opts.name)

    if (!opts.corestore) opts.corestore = this.corestore.namespace(opts.name)
    if (!opts.db) opts.db = sub(this.db, '_' + opts.name)

    const scope = new Scope(opts)
    this.scopes.set(opts.name, scope)
    const info = { name: opts.name, key: encodeKey(opts.key) }
    this.scopeStore.set(opts.name, info)
    if (opts.key) this.scopes.set(opts.key, info)
    return scope
  }

  list () {
    this.scopeStore.list()
  }
}

class Scope extends Nanoresource {
  static uuid () {
    return uuid()
  }

  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts
    this.handlers = opts.handlers || {}

    this._name = opts.name
    this._id = opts.id || uuid()

    this.key = opts.key || null

    this.kappa = opts.kappa || new Kappa()
    if (opts.corestore) {
      this.corestore = opts.corestore
      // TODO: Namespace corestore.
    } else {
      this.corestore = new Corestore(opts.storage || ram)
    }

    this.defaultFeedType = opts.defaultFeedType || FEED_TYPE
    this.defaultWriterName = opts.defaultWriterName || FEED_LOCAL


    const db = opts.db || memdb()
    this._feedStore = new SyncMap(sub(db, 's'), {
      valueEncoding: 'json'
    })
    this.indexer = new Indexer({
      name: this._name,
      db: sub(db, 'i'),
      // Load and decode value.
      loadValue (req, next) {
        self.load(req, (err, message) => {
          if (err) return next(null)
          next(message)
        })
      }
    })

    this.kappa.on('state-update', (name, state) => {
      this.emit('state-update', 'kappa', name, state)
    })

    this.lock = mutex()

    // Cache for records. Max cache size can be set as an option.
    // The length for each record is the buffer length of the serialized record,
    // so the actual cache size will be a bit higher.
    this._recordCache = new LRU({
      max: opts.maxCacheSize || MAX_CACHE_SIZE,
      length (record) {
        return record[LEN] || 64
      }
    })
    // Cache for query bitfields. This will usually take 4KB per bitfield.
    // We cache max 4096 bitfields, amounting to max 16MB bitfield cache size.
    this._queryBitfields = new LRU({
      max: 4096
    })

    this._feeds = new Map()
    this._feedNames = new Map()
    this._feedTypes = {}

    this.ready = this.open.bind(this)
  }

  registerFeedType (name, handlers) {
    this._feedTypes[name] = handlers
  }

  get view () {
    return this.kappa.view
  }

  get api () {
    return this.kappa.api
  }

  use (name, view, opts = {}) {
    const sourceOpts = {
      maxBatch: opts.maxBatch || DEFAULT_MAX_BATCH
    }
    if (opts.filterHeader !== false) {
      sourceOpts.transform = function (message, next) {
        if (message.seq === 0) next(null)
        else next(message)
        // return message
        // console.log(messages)
        // next(messages.filter(msg => msg.seq !== 0))
      }
    }
    if (opts.scopeFeed) {
      // TODO: Make async?
      sourceOpts.filterKey = function (key) {
        const feed = this.feed(key)
        if (!feed) return null
        return opts.scopeFeed(feed[INFO])
      }
    }
    if (!opts.context) opts.context = this
    this.kappa.use(name, this.indexer.source(sourceOpts), view, opts)
  }

  // TODO: Close feeds?
  _close (cb) {
    this.kappa.close(cb)
  }

  _open (cb) {
    const self = this
    cb = once(cb)
    this.corestore.ready(() => {
      this._feedStore.open(() => {
        initFeedTypes()
      })
    })

    function initFeedTypes (err) {
      if (err) return cb(err)
      let pending = 1
      for (const feedType of Object.values(self._feedTypes)) {
        if (feedType.onopen) ++pending && feedType.onopen(done)
      }
      done()
      function done () {
        if (err) return cb(err)
        if (--pending === 0) initFeeds()
      }
    }

    function initFeeds () {
      // Add opts.initialFeeds, if any.
      if (self.opts.initialFeeds) {
        for (const info of self.opts.initialFeeds) {
          self._addFeedInternally(info.key, info)
        }
      }

      // Add feeds from store, if any.
      for (const [key, info] of self._feedStore.entries()) {
        self._addFeedInternally(key, info)
      }

      finish()
    }

    function finish (err) {
      if (err) return cb(err)
      self.kappa.resume()
      self.opened = true
      self.emit('open')
      cb()
    }
  }

  _createFeed (key, opts) {
    let feed

    let { persist, name } = opts

    // In-memory feed requested, create outside of corestore.
    if (persist === false) {
      if (!key) {
        const keyPair = hcrypto.keyPair()
        key = keyPair.key
        opts.secretKey = keyPair.secretKey
      }
      feed = hypercore(ram, key, opts)
      // Feed created outside of corestore, inject into corestore replication
      trackMemoryCore(this.corestore, feed)

    // No key provided, create new writable feed from corestore
    } else if (!key) {
      // No key was given, create new feed.
      if (!name) name = hcrypto.randomBytes(32)
      feed = this.corestore.namespace(DEFAULT_NAMESPACE + ':' + name).default(opts)
      key = feed.key
      this.corestore.get({ ...opts, key })

    // Key provided, get from corestore.
    } else {
      feed = this.corestore.get({ ...opts, key })
    }

    return feed
  }

  _addFeedInternally (key, opts) {
    if (!opts.name) opts.name = uuid()
    if (!opts.type) opts.type = this.defaultFeedType
    const feed = this._createFeed(key, opts)
    upgrade(feed)

    const { name, type, info } = opts

    const idx = feed[INFO].idx()
    this._feeds.set(idx, feed)
    this._feedNames.set(name, idx)
    this._feedNames.set(encodeKey(feed.key), idx)
    this._feedNames.set(idx, idx)

    feed[INFO].setInfo({ name, type, ...info || {} })

    this.indexer.add(feed, { scan: true })

    feed.on('remote-update', () => this.emit('remote-update', feed))

    this.emit('feed', feed, { ...feed[INFO] })
    feed.ready(() => {
      this.emit('state-update', 'feed', feed[INFO].status())
    })

    debug('[%s] add feed key %s name %s type %s', this._name, pretty(feed.key), name, type)

    return feed
  }

  feedInfo (keyOrName) {
    const feed = this.feed(keyOrName)
    if (feed) return feed[INFO].info
    return null
  }

  feed (name) {
    if (Buffer.isBuffer(name)) name = encodeKey(name)
    if (!this._feedNames.has(name)) return null
    return this._feeds.get(this._feedNames.get(name))
  }

  _saveInfo (feed, cb = noop) {
    const info = feed[INFO].info
    const idx = feed[INFO].idx()
    this._feedStore.setFlush(idx, info, cb)
  }

  addFeed (opts, cb = noop) {
    if (!this.opened && !opts._allowPreOpen) {
      return this.open(() => this.addFeed(opts, cb))
    }

    const self = this
    let { name, key } = opts
    if (!name && !key) return cb(new Error('Either key or name is required'))
    if (key) key = encodeKey(key)

    if (key && this.feed(key)) {
      const feed = this.feed(key)
      if (name && feed[INFO].name !== name) {
        this._feedNames.set(name, feed[INFO].idx())
      }
      onready(feed, cb)
      return feed
    }

    if (name && this.feed(name)) {
      const feed = this.feed(name)
      if (key && key !== encodeKey(feed.key)) {
        return cb(new Error('Invalid key for name'))
      }
      onready(feed, cb)
      return feed
    }

    const feed = this._addFeedInternally(key, opts)
    this._saveInfo(feed)

    feed.ready(() => {
      if (feed.writable && !feed.length) {
        feed[INFO].writeHeader(finish)
      } else {
        finish()
      }
    })

    return feed

    function finish (err) {
      if (err) return cb(err)
      self._saveInfo(feed, err => {
        cb(err, feed)
      })
    }
  }

  list () {
    return Array.from(this._feeds.values())
  }

  status () {
    const feeds = this.list().map(feed => feed[INFO].status())
    return {
      name: this._name,
      key: this.key,
      feeds,
      kappa: this.kappa.getState()
    }
  }

  writer (opts, cb) {
    if (typeof opts === 'string') {
      opts = { name: opts }
    } else if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    // opts.writer = true
    if (!opts.name) opts.name = this.defaultWriterName
    this.addFeed(opts, (err, feed) => {
      if (err) return cb(err)
      if (!feed.writable) return cb(new Error('Feed is not writable'))
      cb(null, feed)
    })
  }

  append (message, opts, cb) {
    const self = this
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    if (!opts) opts = {}
    if (!cb) cb = noop

    this.lock(release => {
      self.writer(opts, (err, feed) => {
        if (err) return release(cb, err)
        opts.feedInfo = feed[INFO].info
        opts.feedType = feed[INFO].type
        self._onappend(message, opts, (err, buf, result) => {
          if (err) return release(cb, err)
          feed.append(buf, err => {
            if (err) return release(cb, err)
            // if (!result.key) result.key = feed.key
            // if (!result.seq) result.seq = feed.length - 1
            release(cb, err, result)
          })
        })
      })
    })
  }

  _onappend (message, opts, cb) {
    const { feedType } = opts
    if (this._feedTypes[feedType] && this._feedTypes[feedType].onappend) {
      this._feedTypes[feedType].onappend(message, opts, cb)
    } else if (this.handlers.onappend) {
      this.handlers.onappend(message, opts, cb)
    } else {
      cb(null, message, {})
    }
  }

  _onload (message, opts, cb) {
    const { feedType } = message
    if (this._feedTypes[feedType] && this._feedTypes[feedType].onload) {
      this._feedTypes[feedType].onload(message, opts, cb)
    } else if (this.handlers.onload) {
      this.handlers.onload(message, opts, cb)
    } else {
      cb(null, message)
    }
  }

  batch (messages, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const self = this
    this.lock(release => {
      const batch = []
      const errs = []
      const results = []
      let pending = messages.length
      self.writer(opts, (err, feed) => {
        if (err) return release(cb, err)
        opts.feedType = feed[INFO].type
        for (const message of messages) {
          process.nextTick(() => this._onappend(message, opts, done))
        }
        function done (err, buf, result) {
          if (err) errs.push(err)
          else {
            batch.push(buf)
            results.push(result)
          }
          if (--pending !== 0) return

          if (errs.length) {
            let err = new Error(`Batch failed with ${errs.length} errors. First error: ${errs[0].message}`)
            err.errors = errs
            release(cb, err)
            return
          }

          feed.append(batch, err => release(cb, err, results))
        }
      })
    })
  }

  get (keyOrName, seq, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    if (opts.wait === undefined) opts.wait = false
    const feed = this.feed(keyOrName)
    if (!feed) return cb(new Error('Feed does not exist: ' + keyOrName))
    feed.get(seq, opts, (err, value) => {
      if (err) return cb(err)
      const { type: feedType } = feed[INFO]
      const message = { key: keyOrName, seq, value, feedType }
      this._onload(message, opts, cb)
    })
  }

  load (req, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const self = this
    this._resolveRequest(req, (err, req) => {
      if (err) return cb(err)
      // TODO: Keep this?
      if (req.seq === 0) return cb(new Error('seq 0 is the header, not a record'))

      if (this._recordCache.has(req.lseq)) {
        return cb(null, this._recordCache.get(req.lseq))
      }

      this.get(req.key, req.seq, opts, finish)

      function finish (err, message) {
        if (err) return cb(err)
        message.lseq = req.lseq
        self._recordCache.set(req.lseq, message)
        if (req.meta) {
          message = { ...message, meta: req.meta }
        }
        cb(null, message)
      }
    })
  }

  _resolveRequest (req, cb) {
    if (!empty(req.lseq) && empty(req.seq)) {
      this.indexer.lseqToKeyseq(req.lseq, (err, keyseq) => {
        if (!err && keyseq) {
          req.key = keyseq.key
          req.seq = keyseq.seq
        }
        finish(req)
      })
    } else if (empty(req.lseq)) {
      this.indexer.keyseqToLseq(req.key, req.seq, (err, lseq) => {
        if (!err && lseq) req.lseq = lseq
        finish(req)
      })
    } else finish(req)

    function finish (req) {
      if (empty(req.key) || empty(req.seq)) return cb(new Error('Invalid get request'))
      req.seq = parseInt(req.seq)
      if (!empty(req.lseq)) req.lseq = parseInt(req.lseq)
      if (Buffer.isBuffer(req.key)) req.key = req.key.toString('hex')
      cb(null, req)
    }
  }

  loadRecord (req, cb) {
    this.load(req, cb)
  }

  createLoadStream (opts = {}) {
    const self = this

    const { cacheid } = opts

    let bitfield
    if (cacheid) {
      if (!this._queryBitfields.has(cacheid)) {
        this._queryBitfields.set(cacheid, Bitfield())
      }
      bitfield = this._queryBitfields.get(cacheid)
    }

    const transform = through(function (req, _enc, next) {
      self._resolveRequest(req, (err, req) => {
        if (err) return next()
        if (bitfield && bitfield.get(req.lseq)) {
          this.push({ lseq: req.lseq, meta: req.meta })
          return next()
        }
        self.load(req, (err, message) => {
          if (err) return next()
          if (bitfield) bitfield.set(req.lseq, 1)
          this.push(message)
          next()
        })
      })
    })
    return transform
  }

  createQueryStream (name, args, opts = {}) {
    const self = this
    if (typeof opts.load === 'undefined') opts.load = true

    const proxy = new Transform({
      objectMode: true,
      transform (chunk, enc, next) {
        this.push(chunk)
        next()
      }
    })

    if (!this.view[name] || !this.view[name].query) {
      proxy.destroy(new Error('Invalid query name: ' + name))
      return proxy
    }

    if (opts.waitForSync) {
      this.sync(createStream)
    } else {
      createStream()
    }

    return proxy

    function createStream () {
      const qs = self.view[name].query(args, opts)
      qs.once('sync', () => proxy.emit('sync'))
      qs.on('error', err => proxy.emit('error', err))
      if (opts.load !== false) pump(qs, self.createLoadStream(opts), proxy)
      else pump(qs, proxy)
    }
  }

  sync (views, cb) {
    process.nextTick(() => {
      this.lock(release => {
        this.kappa.ready(views, () => {
          cb()
        })
        release()
      })
    })
  }

  query (name, args, opts = {}, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }

    if (cb && opts.live) {
      return cb(new Error('Cannot use live mode with callbacks'))
    }

    const qs = this.createQueryStream(name, args, opts)
    return collect(qs, cb)
  }

  [inspect] (depth, opts) {
    const { stylize } = opts
    var indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    const feeds = this.list().map(feed => {
      let w = feed.writable ? '+' : ''
      return `[${pretty(feed.key)}${w} ${feed[INFO].name + ' ' || ''}@ ${feed.length}]`
    }).join(', ')

    return 'Scope(\n' +
          indent + '  key         : ' + stylize((this.key && pretty(this.key)), 'string') + '\n' +
          indent + '  discoveryKey: ' + stylize((this.discoveryKey && pretty(this.discoveryKey)), 'string') + '\n' +
          // indent + '  swarmMode:    ' + stylize(this._swarmMode) + '\n' +
          indent + '  feeds:      : ' + stylize(feeds) + '\n' +
          indent + '  opened      : ' + stylize(this.opened, 'boolean') + '\n' +
          indent + '  name        : ' + stylize(this._name, 'string') + '\n' +
          indent + ')'
  }
}

function empty (value) {
  return value === undefined || value === null
}

function onready (feed, cb) {
  if (feed.opened) cb(null, feed)
  else feed.ready(() => cb(null, feed))
}

function upgrade (feed) {
  if (feed[INFO]) return feed[INFO]
  feed[INFO] = new ManagedFeed(feed)
  return feed
}

class ManagedFeed {
  constructor (feed) {
    this.feed = feed
    this._info = {}

    // this[IDX] = encodeKey(this.feed.discoveryKey)
    // this[SERIALIZE] = () => ({ key: encodeKey(this.feed.key), ...this._info })
    // this[NAMES] = new Set()
    // this[NAMES].add(encodeKey(this.feed.key))
  }

  get type () {
    return this._info.type
  }

  get name () {
    return this._info.name
  }

  writeHeader (cb) {
    const { type, metadata } = this.info
    if (!type) return cb()
    const header = Header.encode({
      type,
      metadata: metadata && Buffer.from(JSON.stringify(metadata))
    })
    this.feed.append(header, cb)
  }

  get info () {
    return { ...this._info }
  }

  setInfo (info) {
    this._info = { ...this._info, ...info }
  }

  idx () {
    return encodeKey(this.feed.discoveryKey)
  }

  status () {
    const info = {
      key: encodeKey(this.feed.key),
      discoveryKey: encodeKey(this.feed.discoveryKey),
      writable: this.feed.writable,
      length: this.feed.length,
      byteLength: this.feed.byteLength,
      name: this._info.name,
      type: this._info.type,
      info: this._info
    }
    if (this.feed.opened) {
      info.downloadedBlocks = this.feed.downloaded(0, this.feed.length)
      info.stats = this.feed.stats
    }
    return info
  }
}

// Taken from dat-sdk:
// https://github.com/datproject/sdk/blob/0fb09ec9a88bf4700a94dff1aaa3e62709fce870/index.js#L296
function trackMemoryCore (corestore, core) {
  core.ready(() => {
    corestore._injectIntoReplicationStreams(core)
    corestore.emit('feed', core)
  })

  core.once('close', () => {
    corestore._uncacheCore(core, core.discoveryKey)
  })

  corestore._cacheCore(core, core.discoveryKey, { external: true })
}

function encodeKey (key) {
  return Buffer.isBuffer(key) ? datEncoding.encode(key) : key
}

function decodeKey (key) {
  return (typeof key === 'string') ? datEncoding.decode(key) : key
}

module.exports.Scopes = Scopes
module.exports.Scope = Scope
