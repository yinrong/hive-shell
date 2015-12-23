hive-shell
===========

Use Apache Hive by the shell interface. Depend on the shell command "hive".


## Usage Example
```javascript
var HiveClient = require('hive-shell').HiveClient

var hive = new HiveClient(conf.hive.database, conf.hive)

hive.getSchemaDesc(hive.table, function(err, desc) {
  // desc is the same as "hive -e 'desc TABLE'"
})

hive.execute('drop table ' + hive.table, function(err, stdout) {

})

```
