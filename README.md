chdump
======
This is a modified version of the [original chdump tool written by runreveal](https://github.com/runreveal/chdump), tuned specifically for the schema export of Clickhouse replicated clusters instead of single servers.  

This tool dumps the schema from all databases on a ClickHouse cluster (with an option to exclude the default/system databases). It does not dump or export any data contained in those
tables. It will gather all the databases in the cluster, crawl through them and generate a CREATE query for the database and all the tables and views present.

Since this version of the tool is tuned to work with replicated clusters, the option 'ON CLUSTER [cluster]' will be appended to all CREATE statements. The 'IF NOT EXISTS' option is also used for idempotence purposes. 

This is useful for cloning database schemas between instances or deployments of clickhouse.

There are existing tools like `clickhouse-backup` which read the metadata directories to achieve the same thing.  However, that tool (and other similar tools) require filesystem access to the deployed database which isn't the case if you're running clickhouse cloud or interacting with other hosted clickhouse databases.

## Usage

To dump the schema, run the command like so:

```
chdump --cluster-name new-cluster --exclude-default --conn-string 'clickhouse://username:password@hostname[:port]/[database][?setting=value][&setting2=value]'
```

It spits the schema to standard output, with table definitions separated by a
semicolon and SQL comment line to make the visual identification of tables
clear.

You can then apply this schema to a new database like so:

```
clickhouse client --host <hostname> --user <username> --database [database] -n < schema.sql
```



