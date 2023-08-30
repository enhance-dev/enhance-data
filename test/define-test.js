let sandbox = require('@architect/sandbox')
let data = require('../')
let test = require('tape')

test('start sandbox', async t => {
  t.plan(1)
  await sandbox.start({ quiet: true, cwd: __dirname })
  t.pass('sandbox.start')
})

/** basic crud: todo list */
test('todos', async t => {
  t.plan(6)

  // define a todos schema
  let { todos } = data.define({ todos: { } })

  // write a todo
  let result = await todos.set({ text: 'watch tv' })
  t.ok(result.key, 'query by key')

  // get a todo by key
  let query = await todos.get(result)
  t.ok(result.key === query.key, 'has same key')

  // update a todo
  query.done = true
  let updated = await todos.set(query)
  t.ok(updated.key === query.key && updated.done, 'is done')

  // batch add todos
  let res = await todos.set([ { text: 'make dinner' }, { text: 'feed the cat' } ])
  t.ok(res.length === 2, 'wrote two todo')

  // destroy a todo
  await todos.destroy(updated)
  let count = await todos.count()
  t.ok(count === 2, 'destroyed')

  // read all todos
  let all = await todos.scan()
  t.ok(count === all.length, 'scan()')
  console.log(all)
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

test('parent child relationship', async t => {
  t.plan(4)

  // can define a parent/child relationship
  let { lists, items } = data.define({
    lists: {
      key: 'listID',
      child: 'items'
    },
    items: {
      key: 'itemID',
      parent: 'lists'
    }
  })
  t.ok(lists, 'can list')

  // create a list
  let one = await lists.set({ name: 'mylist' })
  let two = await lists.get({ listID: one.listID })
  t.ok(one.listID === two.listID, 'listIDs match')

  // add a bunch of items:
  let listID = two.listID
  let res = await items.set([
    { text: 'one', listID },
    { text: 'two', listID },
    { text: 'three', listID },
  ])
  t.ok(res.length === 3, 'wrote three')

  // parent can deep query all children
  let three = await lists.get({
    listID: one.listID,
    deep: true
  })
  t.ok(three.items, 'got data in one request')
  console.log(three)
  // should we also be query items by listID for same thing??
})

/** associations: blog
test('blog', async t => {
  t.plan(1)

  // define is only for relationships or associations right now...
  // schema for validations is a sep problem! but it seems likely to go here
  // lifecycle is another thing we may want to consider (fire an even event after write)
  let { authors, posts, tags, comments } = data.define({
    //.get can be passed deep:true to have authors.posts property
    //.get can be queried by {table: 'authors', postID}
    //.set: if passed postID creates a relationship
    //.destroy can be passed deep:true to delete posts that belong to this author
    authors: {
      key: 'id',
      join: 'posts' // join basically means we can pass authors instance methods a postID
    },
    posts: {
      key: 'postID',
      join: ['authors', 'tags'], // you can join as many collections as you want
      child: 'comments' // you can also declare parent/child 1-to-N relationship
    },
    tags: {
      key: 'tag', // key name must be unique to the entire schema (tables are too)
      join: 'posts'
      //thru: 'taggables' // creates another collection! what to do here...
    },
    comments: {
      key: 'commentID',
      parent: 'posts' // whenever there is a child there must be a parent! likewise a join needs to be reciprocated
    }
  })

  // add a few authors
  let team = await authors.set([
    {name: 'ryan'},
    {name: 'ryan2'},
    {name: 'ryan3'}
  ])

  // each author writes a post

  let one = await posts.set({
    authors: [author], // id: author.id works too
    text: 'big blawg post text here',
    tags: ['cat', 'dog', 'bird'] // if scalars passed to array we assume they are the key value
  })

  // one post gets a lot of comments
  // await comments.set({ parent: posted[0], text }) // writes posts#postID#comments#commentID

  t.ok(team.length === 4, 'four ryans')

  // list all posts sorted by date (recent first)
  // let posts = await posts.get({ sort: 'created' })

  // get a post, its author and any comments
  // let post = await posts.get({ postID, deep: true })

  // get a list of posts by author
  //let post = await posts.get({ authorID: author })

  // get a list of posts by tag
  //let post = await posts.get({ tag: 'cat' })

  // await authors.update({id, email})
})*/


/*
chat program:
- accounts
- connections
- channels
- messages
*/
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
