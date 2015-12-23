var util = require('util')
var EventEmitter = require('events').EventEmitter
var exec = require('child_process').exec
var async = require('async')
var assert = require('assert')

function HiveClient(database, opts) {
  this._database = database
  if (!opts) {
    opts = {}
  }
  this._opts = opts
}


HiveClient.prototype = {

execute: function(cmd, callback) {
  var hive_cmd = util.format( 'use %s; %s;', this._database, cmd.replace(/'/g, '"'))
  if (this._opts.dry_run) {
    return callback(null, null)
  }
  exec(util.format( 'hive -e \'%s\'', hive_cmd), function(err, stdout, stderr) {
    if (err) {
      err = new Error('cmd:' + hive_cmd + ' err:' + err)
    }
    callback(err, stdout)
  })
},

loadData: function(cfg, callback) {
  this.execute(util.format(
    'load data local inpath "%s" into table %s.%s partition(%s="%s")',
      cfg.file, this._database, cfg.table, cfg.partition.name,
      cfg.partition.value),
    callback)
},

descTable: function(table, callback) {
  this.execute('desc ' + table, function(err, result) {
    if (err) {
      // suppose not exist
      return callback(null, null)
    }
    callback(null, result)
  })
},

getSchemaDesc: function(table, callback) {
  assert(callback.constructor === Function)
  var that = this
  async.waterfall([
  function(next) {
    that.descTable(table, next)
  },
  function(result, next) {
    if (!result) {
      return callback(null, "")
    }
    var lines = []
    result.split('\n').forEach(function(line) {
      if (line.match(/^\s*$/)) {
        return
      }
      if (line.match(/^# Partition/)) {
        line = '#Partition:'
      } else if (line.match(/^#/)) {
        return
      } else {
        var cols = line.replace(/^#.*/, '').split(/\s+/)
        line = cols[0] + ' ' + cols[1] + ' ' +
               ( cols[2] && cols.slice(2).join(' ') )
      }
      line = line.replace(/\s+$/, '')
      lines.push(line)
    })
    callback(null, lines.join('\n'))
  },
  ], callback)
},

getCreatePartitionsFromOld: function(table, callback) {
  assert(callback.constructor === Function)
  var that = this
  async.waterfall([
  function(next) {
    that.execute('show partitions ' + table, next)
  },
  function(partitions, next) {
    if (!partitions) {
      return callback(null, "")
    }
    callback(null, partitions.split('\n')
      .filter(function(line) {
        return !line.match(/^\s*$/)
      })
      .map(function(line) {
        return util.format(
          'alter table fetcher_keyword_daily add partition (%s)',
          line.replace(/=/, '="').replace(/$/, '"'))
      })
      .join(';')
    )
  },
  ], callback)
},


}

module.exports = HiveClient
