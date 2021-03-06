const util = require('util')
const EventEmitter = require('events').EventEmitter
const child_process = require('child_process')
const async = require('async')
const assert = require('assert')
const stream = require('stream')
const log = new (require('debug'))('hive-shell')
const _ = require('lodash')
const SET_PRINT_HEADER = 'set hive.cli.print.header=true; '

function HiveClient(database, _opts) {
  this._database = database
  _opts = _opts || {}
  var opts = _.clone(_opts)
  this._opts = opts

  putDefault(opts, 'hive.support.sql11.reserved.keywords', false)
  this._hive_params = _
    .chain(opts)
    .mapValues(function(v, k) {
      return 'set ' + k + '=' + v + ';'
    })
    .values()
    .join(' ')
    .value()
}

function putDefault(obj, key, default_value) {
  obj[key] = obj[key] || default_value
}


HiveClient.prototype = {

_getHiveCmd: function(cmd, callback) {
  return util.format( 'use %s; %s %s;', this._database, this._hive_params, cmd.replace(/'/g, '"'))
},

spawn: function(cmd) {
  if (this._opts.dry_run) {
    return null
  }
  log('spawn hive -e', this._getHiveCmd(cmd))
  // TODO: some env may interfere the hive command, filter these env
  return child_process.spawn(this._opts.hive_bin || 'hive',
                            ['-e', this._getHiveCmd(cmd)],
                            { env: _(process.env).omit('DEBUG').value() })
},

getCsvStream: function(table, where) {
  var stack = new Error().stack
  var hive = this.spawn(SET_PRINT_HEADER +
                        'select * from ' + table + ' where ' + where)
  var out = new stream.PassThrough()
  hive.stdout.pipe(out)
  var stderr = ''
  hive.stderr.on('data', function(data) {
    var stderr_line = data.toString().trim()
    log(table, 'stderr:', stderr_line)
    stderr += stderr_line + '\n'
  })
  hive.on('close', function(code) {
    log(table, 'closed with code', code)
    if (code !==0 ) {
      return out.emit('error',
        'code:' + code + ' msg:' + stderr + ' stack:' + stack)
    }
    out.end()
  })
  return out
},

getTableHeader: function(table, callback) {
  var stack = new Error().stack
  var hive = this.execute(
    SET_PRINT_HEADER + 'select * from ' + table + ' limit 0',
    callback)
},

execute: function(cmd, callback) {
  if (this._opts.dry_run) {
    return callback(null, null)
  }
  child_process.exec(
    util.format( 'hive -e \'%s\'', this._getHiveCmd(cmd)),
    function(err, stdout, stderr) {
      if (err) {
        err = new Error('cmd:' + cmd + ' err:' + err)
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

