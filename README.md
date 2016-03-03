hive-shell
===========

Use Apache Hive by the shell interface. Depend on the shell command "hive".


## Usage Example
```javascript
var HiveClient = require('hive-shell').HiveClient

var opts = {dry_run: true}
var hive = new HiveClient('database_name', opts /* optional */ )

hive.getSchemaDesc(hive.table, function(err, desc) {
  // desc is the same as "hive -e 'desc TABLE'"
})

hive.execute('drop table ' + hive.table, function(err, stdout) {

})

```
