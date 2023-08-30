module.exports = {
  schema (schema) {

    let problems = []

    // the first layer of the schema are the table names
    let tables = Object.keys(schema)

    if (tables.length === 0)
      problems.push('- no tables defined')

    // ensure if there more than tables and they all have keys that those keys are unique
    if (tables.length > 1) {
      let keys = []
      for (let table of tables) {
        if (!schema[table].key) {
          problems.push('- table missing key: ' + table)
        }
        else {
          keys.push(schema[table].key)
        }
      }
      let k = new Set(keys)
      let a = Array.from(k)
      if (a.length < tables.length) {
        problems.push('- duplicate key found')
        // TODO what key?!
      }
    }

    // walk tables to check relationships and basics like name
    for (let table of tables) {

      // table name must be a string > 2 characters (for now)
      if (!table || table.length <= 2) {
        problems.push('invalid table name: ' + table)
      }

      // ensure it has a child table and that table has 'child' defined to parent
      if (schema[table].parent) {
        if (Array.isArray(schema[table].parent))
          problems.push(table + ' can only have one parent')
        let exists = tables.find(t => t === schema[table].parent)
        if (exists) {
          let name = schema[table].parent
          let parent = schema[name]
          if (parent.child) {
            let isParent = Array.isArray(parent.child) ? parent.child.includes(table) : parent.child === table
            if (!isParent)
              problems.push(name + ' invalid child: expected ' + table + ' (got ' + parent.child  + ')')
          }
          else {
            problems.push(name + ' missing child: ' + table)
          }
        }
        else {
          problems.push(table + ' parent not found: ' + schema[table].parent)
        }
      }

      // ensure it has a parent table and that table has a parent value defined to child
      if (schema[table].child) {
        // convert to an array if it isn't one
        let children = (!Array.isArray(schema[table].child)) ? [ schema[table].child ] : schema[table].child
        for (let name of children) {
          let exists = tables.find(t => t === name)
          if (exists) {
            let child = schema[name]
            if (child.parent) {
              let isChild = child.parent === table
              if (!isChild) problems.push(name + ' invalid parent: expected ' + table + ' (got ' + child.parent  + ')')
            }
            else {
              problems.push(name + ' missing parent: ' + table)
            }
          }
          else {
            problems.push(table + ' child not found: ' + name)
          }
        }
      }

      if (schema[table].join) {
        // ensure the join table exists and the value of join for that table is same as this table
      }
    }
    if (problems.length > 0)
      throw Error('bad_schema: \n' + problems.join('\n'))
  },

  /** throws if it finds invalid input */
  set (/* { schema, table, item }*/) {
    // if this is a child table it must have a parentID
    // if this is a parent table its fiiine
    // if this is a join table it must? have a joinID
  },
  get () {},
  destroy () {}
}
