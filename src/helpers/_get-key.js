/**
 * @private
 * @module getKey
 *
 * Get the begin-data partition key (scopeID) and sort key (dataID)
 *
 * - the env var BEGIN_DATA_SCOPE_ID override always wins
 * - uses ARC_APP_NAME if it exists
 * - fallback to 'local' for running in the sandbox
 * - dataID is scoped to staging or production depending on NODE_ENV
 */
module.exports = function getKey ({ table, key }) {
  if (!table) throw Error('missing_table')
  if (!key) throw Error('missing_key')
  // let scopeID = 'sandbox'
  // let dataID = `#${table}#${key}`
  return { scopeID: table, dataID: `##${key}` }
}
