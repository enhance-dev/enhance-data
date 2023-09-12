let sandbox = require('@architect/sandbox')
let data = require('../')
let test = require('tape')

test('start sandbox', async t => {
  t.plan(1)
  await sandbox.start({ quiet: true, cwd: __dirname })
  t.pass('sandbox.start')
})

test('schemas with more than one table must have unique keys defined', async t => {
  t.plan(2)
  try {
    // eslint-disable-next-line
    let client = data.define({
      todos: {},
      orders: {},
    })
  }
  catch (e) {
    // eslint-disable-next-line
    let client = data.define({
      todos: { key: 'todoID' },
      orders: { key: 'orderID' },
    })
    t.pass('collections require unique keys')
  }
  try {
    // eslint-disable-next-line
    let client = data.define({
      todos: { key: 'id' },
      orders: { key: 'id' },
    })
  }
  catch (e) {
    t.pass('collections cannot have duplicate keys')
  }
})

test('schema that defines a parent child relationship', async t => {
  t.plan(2)

  try {
    // eslint-disable-next-line
    let { lists, items } = data.define({
      lists: {
        key: 'listID',
      },
      items: {
        key: 'itemID',
        parent: 'lists'
      }
    })
  }
  catch (e) {
    t.pass('parent must have child')
  }

  try {
    // eslint-disable-next-line
    let { lists, items } = data.define({
      lists: {
        key: 'listID',
        child: 'items'
      },
      items: {
        key: 'itemID',
      }
    })
  }
  catch (e) {
    t.pass('child must have parent')
  }
})

test('shutdown sandbox', async t => {
  t.plan(1)
  await sandbox.end()
  t.pass('sandbox.end')
})

// ensure clean exit even on hanging async work
process.on('unhandledRejection', (reason, p) => {
  console.log(reason)
  console.log(p)
  sandbox.end()
})
