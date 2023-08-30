let get = require('../get')
let set = require('../set')
let destroy = require('../destroy')
let page = require('../page')
let count = require('../count')
let validate = require('./validate')
let createKey = require('./create-key')

function fmt ({ schema, table, raw }) {
  // if a key is defined use it
  let id = schema[table].key
  let items = Array.isArray(raw) ? raw : [ raw ]
  // console.log('=====>', items)
  for (let item of items) {
    if (id) {
      item[id] = item.key
      delete item.key
    }
  }
  return Array.isArray(raw) ? items : items[0]
}

module.exports = function define (schema) {

  // fail hard if the schema has issues
  validate.schema(schema)

  // otherwise creates a crudl client
  let client = {}
  for (let table of Object.keys(schema)) {
    client[table] = {

      /** read row(s) */
      async get (params) {
        validate.get({ schema, table, item: params })

        // if we are parent/child we can query deep
        if (params.deep) {
          // query child rows
          let key = schema[table].key
          let pages = await page({ table, limit: 100, begin: params[key] })
          let result = []
          for await (let page of pages) {
            result = result.concat(page)
          }
          let childName = schema[table].child
          let notChild = k => k.key.includes(':') === false
          let isChild = k => k.key && k.key.includes(':')
          let par = fmt({ table, schema, raw: result.find(notChild) })
          par[childName] = result.filter(isChild).map(v => {
            return fmt({ schema, table: childName, raw: v })
          })
          return par
        }

        // otherwise normal get behavior
        let collection = Array.isArray(params)
        let items = collection ? params : [ params ]
        for (let item of items) {

          if (!item.table) {
            item.table = table
          }

          let key = schema[table].key
          if (item[key]) {
            item.key = item[key]
          }
        }

        if (collection) {
          let result = await get(items)
          return result.map(raw => fmt({ schema, table, raw }))
        }
        else {
          let raw = await get(items[0])
          return fmt({ schema, table, raw })
        }
      },

      /** write row(s) */
      async set (params) {
        validate.set({ schema, table, item: params })
        let collection = Array.isArray(params)
        let updated = Date.now()
        let items = collection ? params : [ params ]

        for (let item of items) {

          // add the table in
          if (!item.table) {
            item.table = table
          }

          // add the key back in from alias if they gave us one
          let key = schema[table].key
          if (item[key]) {
            item.key = item[key]
          }

          // always update ts
          item.updated = updated

          // if this is a child then we need to format the key
          let child = !!schema[table].parent
          if (child) {
            let name = schema[table].parent
            let id = schema[name].key
            let parent = item[id]
            let tmp = await createKey(table)
            item.table = name // write row to parent iable
            item.key = `${parent}:${table}:${tmp}`
          }
        }

        let result = (await set(items)).map(raw => {
          return fmt({ schema, table, raw })
        })
        return collection ? result : result[0]
      },

      /** update row(s) */
      update () {},

      /** remove row(s) */
      destroy (params) {
        validate.destroy({ schema, table, item: params })
        let collection = Array.isArray(params)
        let items = collection ? params : [ params ]
        for (let item of items) {
          if (!item.table) {
            item.table = table
          }
          let key = schema[table].key
          if (item[key]) {
            item.key = item[key]
            delete item[key]
          }
        }
        return destroy(items)
      },

      /** scan all rows */
      async scan () {
        let pages = await page({ table, limit: 100 })
        let res = []
        for await (let p of pages)
          for (let r of p)
            res.push(r)
        return res.map(v => fmt({ schema, table, raw: v }))
      },

      /** paginate rows */
      page ({ limit = 100 }) {
        return page({ table, limit })
      },

      /** return total number of items in collection */
      count () {
        return count({ table })
      }
    }
  }

  return client
}
