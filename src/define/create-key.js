let create = require('../helpers/_create-key')

module.exports = function createKey (table) {
  return new Promise((res, rej) => {
    create(table, function created (err, result) {
      if (err) rej(err)
      else res(result)
    })
  })
}
