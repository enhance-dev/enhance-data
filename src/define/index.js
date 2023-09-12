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

        // direct get behavior
        // massage items to have correct underlying {table, key} (aka pk/sk)
        let collection = Array.isArray(params)
        let items = collection ? params : [ params ]
        for (let item of items) {

          if (!item.table) {
            let child = !!schema[table].parent// if child write row to parent table
            item.table = child ? schema[table].parent : table
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
        let joins = [] // add join rows into here

        for (let item of items) {

          // add the table in
          if (!item.table) {
            item.table = table
          }

          // add the key back in from alias if they gave us on
          let key = schema[table].key
          if (item[key]) {
            item.key = item[key]
          }

          // always update ts
          item.updated = updated

          // if this is a child then we need to re-format the key
          // parent/child is modelled as parentKey:childTableName:childKey
          let child = !!schema[table].parent
          if (child) {
            let name = schema[table].parent
            let id = schema[name].key
            let parent = item[id]
            let tmp = item.key || await createKey(table)
            item.table = name // write row to parent iable
            item.key = `${parent}:${table}:${tmp}`
          }

          // if there is a join we may need need to add extra rows
          let hasJoin = !!schema[table].join
          if (hasJoin) {
            let secondTableName = schema[table].join
            let secondTableKeyName = schema[secondTableName].key
            let secondTableKeyValue = item[secondTableKeyName]
            let joining = !!secondTableKeyValue
            if (joining) {
              let via = schema[table].via
              if (via) {
                let viaKey = item.key || await createKey(table)
                // through joins are modelled as THREE extra rows
                // - table: throughTableName key: throughTableName ...attrs
                // - table: throughTableName key: firstTableName:firstTableKey:secondTableName:secondTableKey
                // - table: throughTableName key: secondTableName:secondTableKey:firstTableName:firstTableKey
                joins.push({
                  table: via,
                  kind: 'join',
                  key: viaKey,
                  updated
                })
                joins.push({
                  table: via,
                  kind: 'join',
                  key: `${viaKey}:${secondTableName}:${secondTableKeyValue}:${table}:${item.key}`,
                  updated
                })
                joins.push({
                  table: via,
                  kind: 'join',
                  key: `${viaKey}:${table}:${item.key}:${secondTableName}:${secondTableKeyValue}`,
                  updated
                })
              }
              else {
                // basic joins are modelled as two rows
                // - table: firstTableName key: firstTableName:firstTableKey:secondTableName:secondTableKey
                // - table: secondTableName key: secondTableName:secondTableKey:firstTableName:firstTableKey
                // now we can query all secondTables items by firstTable and vice versa
                joins.push({
                  table,
                  kind: 'join',
                  key: `${table}:${item.key}:${secondTableName}:${secondTableKeyValue}`,
                  updated
                })
                joins.push({
                  table: secondTableName,
                  kind: 'join',
                  key: `${secondTableName}:${secondTableKeyValue}:${table}:${item.key}`,
                  updated
                })
              }
            }
          } // end hasJoin
        }

        // add join rows
        items = items.concat(joins)

        // write rows
        let result = (await set(items)).filter(i => i?.kind != 'join').map(raw => {
          return fmt({ schema, table, raw })
        })

        return collection ? result : result[0]
      },

      /** update row(s) */
      update () {},

      /** remove row(s) */
      async destroy (params) {
        validate.destroy({ schema, table, item: params })

        let collection = Array.isArray(params)
        let items = collection ? params : [ params ]
        let children = []

        for (let item of items) {

          if (!item.table) {
            item.table = table
          }

          let key = schema[table].key
          if (item[key]) {
            item.key = item[key]
            delete item[key]
          }

          // if childrows exist destroy those too
          let parent = !!schema[table].child
          if (parent) {
            // find all child rows and add those to the queue to destroy
            let pages = await page({ table, limit: 100, begin: item.key })
            for await (let p of pages)
              for (let r of p) children.push({ table, ...r })
          }
        }
        function dedup (a, b) {
          a[b.key] = b
          return a
        }
        let uniq = Object.values(items.concat(children).reduce(dedup, {}))
        return destroy(uniq)
      },

      /** paginate rows */
      page ({ limit = 100 }) {
        return page({ table, limit })
      },

      /** return total number of items in collection */
      count () {
        return count({ table })
      },

      // -- following methods only available to clients created with data.define -- //

      /** return parent and all child rows */
      async getAll (params) {
        validate.getAll({ schema, table, params })
        let key = schema[table].key
        let pages = await page({ table, limit: 100, begin: params[key] })
        let result = []
        for await (let page of pages) {
          result = result.concat(page)
        }
        if (result.length === 0) {
          throw Error('not_found')
        }
        let childName = schema[table].child
        let notChild = k => k.key.includes(':') === false
        let isChild = k => k.key && k.key.includes(':')
        let par = fmt({ table, schema, raw: result.find(notChild) })
        par[childName] = result.filter(isChild).map(v => {
          return fmt({ schema, table: childName, raw: v })
        })
        return par
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
    }
  }

  return client
}
