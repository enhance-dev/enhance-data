/**
 * @private
 * @module getKey
 *
 * Get the partition key (scopeID) and sort key (dataID)
 */
module.exports = function getKey ({ table, key }) {
  if (!table) throw Error('missing_table')
  if (!key) throw Error('missing_key')
  return { scopeID: table, dataID: `##${key}` }
}
