const SyncMap = require('./sync-map')
const sub = require('subleveldown')

const IDX = Symbol('object-idx')
const SERIALIZE = Symbol('object-serialize')
const NAMES = Symbol('names')

class ObjectStore {
  constructor (db, opts = {}) {
    // Store object details.
    this.ostore = new SyncMap(sub(db, 'o'), { valueEncoding: opts.encoding || 'json' })
    // Store additional object names.
    this.nstore = new SyncMap(sub(db, 'n'), { valueEncoding: 'utf8' })
    this.set = new Set()
    this.map = new Map()
    this.names = new Map()
    this.tags = new Map()
    this._create = opts.create || ((info) => ({ ...info }))
  }

  open (cb) {
    this.store.open(() => {
      for (const [idx, info] of this.ostore.entries()) {
        const object = this._create(info)
        if (!object[IDX]) object[IDX] = idx
        if (object) this.add(object)
      }
    })
  }

  add (object) {
    if (this.set.has(object)) return
    if (!object[IDX]) object[IDX] = uuid()
    if (!object[NAMES]) object[NAMES] = new Set()
    // object[STORE] = this
    this.objects.set(object[IDX], object)
    this.set.add(object)
    this.save(object)
    return object[IDX]
  }

  save (object) {
    this.ostore.set(object[IDX], object[SERIALIZE]())
    this.nstore.set(object[IDX], object[IDX])
    for (const name of object[NAMES]) {
      this.nstore.set(name, object[IDX])
    }
  }

  set (name, object) {
    if (!this.set.has(object)) this.add(object)
    object[NAMES].add(name)
    this.nstore.set(name, object[IDX])
  }

  list () {
    return Array.from(this.set)
  }

  entries () {
    return Array.from(this.map.entries())
  }

  get (name) {
    if (this.map.has(name)) return this.map.get(name)
    if (this.nstore.has(name)) return this.map.get(this.nstore.get(name))
  }

  find (fn) {
    return Array.from(this.set).filter(object => fn(object[SERIALIZE]()))
  }

  flush (cb) {
    this.ostore.flush(() => {
      this.nstore.flush(cb)
    })
  }
}

module.exports = ObjectStore
module.exports.IDX = IDX
module.exports.NAMES = NAMES
module.exports.SERIALIZE = SERIALIZE
