# EPGL - Erlang PostgreSQL Logical 

EPGL streaming replication Erlang library replicates data from RDBMS PostgreSQL to an Erlang application. 
Based on PostgreSQL streaming replication protocol https://www.postgresql.org/docs/current/static/protocol-replication.html

And pglogical_output plugin https://2ndquadrant.com/en/resources/pglogical
Or pgoutput plugin (starting from PostgreSQL 10) https://www.postgresql.org/docs/10/static/protocol-logicalrep-message-formats.html

## Use cases
### Consistent cache
Erlang application uses in read-only mode data from PostgreSQL DB tables 
(e.g. some configuration, rate/price plans, subscriber’s profiles) 
and needs to store the data in some internal structure (e.g. ETS table). 
The data in PostgreSQL DB is updated by some third party applications. 
Erlang application needs to know about modifications in a near real-time mode. 
During start Erlang application uploads initial data from the tables 
and subscribes to receive any modifications for data in these tables 
and update internal ETS table accordingly.
Using special output plugins for replication slots (for example: pglogical_output) 
we can filter tables and receive only changes for some particular tables.

### Consistent internal DB
This case is similar to previous “Consistent cache”, 
but the difference is that amount of data in the table is huge 
and it is inefficient and takes too much time to upload data every time on startup, 
so Erlang application stores copy of data in some persistent internal storage (mnesia, riak, files). 
In this case application uploads initial data from the table only once during the first start. 
Afterwards it receives modifications and apply it in it’s storage. 
Even if application is down it will not lose any changes from DB 
and will receive all changes when it starts again.

### Gateway
Erlang application is a gateway between PostgreSQL DB and some third system. 
In this case the application will pass each replication message to an external system.

## Features
- Subscribers may monitor only particular tables and operations (insert/update/delete) for them.
- Completeness and consistency of the data between Erlang and PostgreSQL DB.
- Receive changes from PostgreSQL DB in a near real-time mode.
- No data loss during Erlang application downtime.
- Multiple subscribers for the same or different tables.
- Number of subscribers does not affect PostgreSQL DB performance (in case we use single replication slot).

## Usage
### Connect
Start subscriber and connect to DB. 
  
```erlang

    DBArgs = [{hostname, "localhost"}, {port, 5432}, {database, "test"}, {username, "username"}, {password, "password"}],
    Callbacks = #{"public.table1" => [?MODULE], "public.table2" => [?MODULE]},
     SubscriberId = epgl_subscriber_id1,
    %% start and connect to DB
    {ok, Pid} = epgl:start_subscriber(SubscriberId, DBArgs, Callbacks),
```

Callback modules should implement method `handle_replication_msg`

```erlang
  -callback handle_replication_msg(
      Metadata :: #{TableName :: string() => [ColumnName :: string()]},
      Rows :: [#row{}]) -> ok.
  
    -record(row, {
        table_name :: string(),
        change_type :: change_type(), %% insert | update | delete
        fields :: list()
    }).
  ```

### Create replication slot
You may need to execute this step every time you start you Erlang application, 
if you select initial data from tables when start your application (Use case Consistent cache). 
Or only once your case is Consistent internal DB.

```erlang
{ok, SnapshotName, XLogPosition} = epgl:create_replication_slot(Pid, "repl_slot"),
%% or temporary replication_slot for PG 10
{ok, SnapshotName, XLogPosition} = epgl:create_replication_slot(Pid, "repl_slot", #{temporary => true}),
```
Use `drop_replication_slot` to drop existing replication slot before creation.

From PostgreSQL 10 there is an option to create temporary replication slot 
which automatically dropped at the end of the session.
So if you reload all initial data from tables when start your application (Use case Consistent cache)
then it is **highly recommended** to create temporary replication_slot. 
In this case you do not need to drop it explicitly.

**Important Note**: Drop replication slots you do not need any more to stop it consuming server resources.


### Read initial data
When a new replication slot is created, a snapshot is exported, 
which will show exactly the state of the database 
after which all changes will be included in the change stream. 
This snapshot can be used to read the state of the database at the moment the slot was created. 
If you need initial data for all tables from the replication set(s) or publications (if pgoutput plugin is used) use `init_replication_set` 
with the Snapshot Name created in the previous step.
The callback module will receive initial data through callback method `handle_replication_msg` 
in the same format as a normal replication message.

If you need initial data only for some tables from replication set or publication (if pgoutput plugin is used) use `get_table_initial_state`.

If initial data is huge it is recommended to implement initial data loading yourself using *epgsqli*. 
Use theses commands to load data from snapshot exported on replication slot creation
```erlang
    %% Connect epgsql:connect
    {ok, _, _} = epgsql:squery(Conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
    {ok, _, _} = epgsql:squery(Conn, ["SET TRANSACTION SNAPSHOT '", SnapshotName, "'"]),
    %% parse
    %% load data epgsqli:equery
```

Note: The snapshot is only valid as long as the connection that issued the `CREATE_REPLICATION_SLOT` remains open and has not run another command.      

```erlang
{ok, Columns, RowValues} = epgl:get_table_initial_state(Pid, "table1", SnapshotName).

%% get initial table data. handle_replication_msg will be called for records  
ok = epgl:init_replication_set(Pid, "repl_set1,repl_set2", SnapshotName).
ok = epgl:init_replication_set(Pid, "repl_set1,repl_set2", SnapshotName, TablesOrder).
```

### Start replication
```erlang
%% start replication
ok = epgl:start_replication(Pid, "repl_slot", "set1,repl_set2").
```

On success, PostgreSQL server starts to stream modifications (WAL records) 
and callback method will be called for all changes for tables configured in replication sets or publications (if pgoutput plugin is used).

### Example of callback method handle_replication_msg input

```erlang
Metadata = #{"public.table1" => [<<"id">>,<<"column1">>],
           "public.table2" => [<<"id">>,<<"column1">>,<<"column2">>,<<"column3">>]},
Rows = [
            {row,"public.table1",insert,[<<"96">>,<<"value123">>]},
            {row,"public.table2",insert, [<<"157">>,<<"value1">>,null,<<"value3">>]},
            {row,"public.table1",insert,[<<"97">>,null]},
            {row,"public.table1",update,[<<"96">>,<<"value345">>]},
            {row,"public.table2",update,[<<"157">>,<<"value2">>,null,<<"value789">>]},
            {row,"public.table1",delete,[<<"97">>,null]},
            {row,"public.table2",delete,[<<"157">>,null],null],null]}]  
```

Note: DELETE has the same format as INSERT/UPDATE, but all columns except Primary Key columns have value null.

Note: The list of rows may be huge if in one transaction a lot of rows were updated 
(for example: update for huge table without any condition)    

### Reconnect
In case of the database going down or other problems epgl will keep trying to reconnect to the database 
and start replication again. 
After reconnection PostgreSQL server will continue streaming from the last processed change (LSN). 
But the current position of each replication slot is persisted by PostgreSQL server only at checkpoint, 
so in the case of a crash the slot may return to an earlier LSN, 
which will then cause recent changes to be resent when the server restarts. 
In order to prevent such cases epgl store also last processed LSN 
and may skip already processed changes (this is configurable feature).


### Check last LSN

```erlang
SubscriberId = epgl_subscriber_id1, %% Name or Pid 
LSN = epgl:get_last_lsn(SubscriberId).
LSN = epgl:format_lsn(epgl:get_last_lsn(SubscriberId)). %%Formats 64bit LSN into the XXX/XXX format
```

### Pglogical output formats
There are two Pglogical output formats supported:
  1) Textual output format (default option), pglogical_output returns values similar as epgsql:squery; 
  2) Binary format (only for pglogical plugin). pglogical_output returns values in a PostgreSQL's binary format,
     EPGL decodes values using epgsql_binary, the result is similar to epgsql:equery 
     (i.e return integers as Erlang integers, floats as floats end etc.).
     In order to enable binary format you should start_replication with options binary_mode => true 
     and the following parameters must be set
     - epgl.pglogical_config.binary.bigendian = true or false 
     - epgl.pglogical_config.binary.integer_datetimes = true or false. EPGL will try to detect itself if not set.
     - epgl.pglogical_config.pg_version = select PG_VERSION_NUM(), e.g 90506.

    
## Configuration
### PostgreSQL configuration for pglogical plugin
1. Install pglogical. See https://2ndquadrant.com/en/resources/pglogical/pglogical-installation-instructions/
2. Set server parameters (postgresql.conf) to support logical decoding (see https://2ndquadrant.com/en/resources/pglogical/pglogical-docs/)
    
    Example:
    - wal_level = 'logical'
    - max_replication_slots = 5
    - max_wal_senders = 5
    - shared_preload_libraries = 'pglogical'
3. pg_hba.conf has to allow replication connections from localhost, by a user with replication privilege.
Example: `host    replication     epgl_test    127.0.0.1/32    trust`
3. Create pglogical extension: `CREATE EXTENSION pglogical;`
4. Create the provider pglogical node (`pglogical.create_node`)
    
    Example:
    ```sql
    SELECT pglogical.create_node(
        node_name := 'epgl_provider',
        dsn := 'host=localhost port=10432 dbname=epgl_test_db'
    );
    ```
5. Create replication_set and add tables (`pglogical.create_replication_set`, `pglogical.replication_set_add_table`)
    
    Example:
    ```sql
    SELECT pglogical.create_replication_set(
        set_name := 'epgl_test_repl_set_1',
        replicate_insert := true,
        replicate_update := true,
        replicate_delete := true);
    SELECT pglogical.replication_set_add_table(
        set_name := 'epgl_test_repl_set_1', 
        relation := 'test_table1');
    ```

6. PostgreSQL should have 
    - the REPLICATION privilege
    - SELECT on tables you are going to replicate
    - SELECT on pglogical.tables
    
7. You can use these system table to check replication status in PostgreSQL DB 
```sql
select * from pg_catalog.pg_replication_slots;
select * from pg_catalog.pg_stat_replication; 
```

### PostgreSQL configuration for pgoutput plugin (for PostgreSQL 10)
1. No need to install any extension because pgoutput plugin is a part of PostgreSQL 10 (details https://www.postgresql.org/docs/10/static/logical-replication.html).
2. Set server parameters (postgresql.conf) to support logical replication (see https://www.postgresql.org/docs/10/static/logical-replication-config.html)
    
    Example:
    - wal_level = 'logical'
    - max_replication_slots = 5
    - max_wal_senders = 5
3. pg_hba.conf has to allow replication connections from localhost, by a user with replication privilege.
Example: `host    replication     epgl_test    127.0.0.1/32    trust`

4. Create publication with tables (`CREATE PUBLICATION`, `ALTER PUBLICATION`)
    
    Example:
    ```sql
    CREATE PUBLICATION mypub FOR TABLE public.test_table1, public.test_table3;
    ALTER PUBLICATION mypub ADD TABLE public.test_table2;
    ```
 
5. You can use these system table to check replication status in PostgreSQL DB 
```sql
select * from pg_catalog.pg_replication_slots;
select * from pg_catalog.pg_stat_replication;
select * from pg_catalog.pg_publication_tables; 
```

### Erlang Parameters
Options|Type|Default|Description
---|---|---|---
epgl.repl_slot_output_plugin|atom (pglogical or pgoutput)|pgoutput|Decoding output plugin for replication
epgl.reconnect_interval|integer|10|The amount of time to wait before attempting to reconnect to a given server in seconds
epgl.max_reconnect_attempts|integer or infinite|infinite|Maximum number of reconnection attempts. Infinite to keep trying forever
epgl.check_lsn_mode|atom|skip|How to handle messages with already processed LSN. LSN is checked during commit message processing.<br>  - off: do nothing<br>  - log: log an error<br>  - skip: skip such messages
epgl.debug|boolean|false|Log every replication message
epgl.two_msgs_for_pk_update|boolean|false|In case of primary key update send additional delete message with old primary key. And insert message instead of update message for new primary key
epgl.pglogical_config.expected_encoding|string||Field values for textual data will be in this encoding in native protocol text, binary or internal representation
epgl.pglogical_config.binary.bigendian|boolean|true|True if the upstream is big-endian
epgl.pglogical_config.binary.sizeof_datum|integer||Same as sizeof_int, but for the PostgreSQL Datum typedef
epgl.pglogical_config.binary.sizeof_int|integer||sizeof(int) on the upstream
epgl.pglogical_config.binary.sizeof_long|integer||sizeof(long) on the upstream
epgl.pglogical_config.binary.float4_byval|boolean||Upstream PostgreSQL’s float4_byval compile option
epgl.pglogical_config.binary.float8_byval|boolean||Upstream PostgreSQL’s float8_byval compile option
epgl.pglogical_config.binary.integer_datetimes|boolean||Whether TIME, TIMESTAMP and TIMESTAMP WITH TIME ZONE will be sent using integer or floating point representation.
epgl.pglogical_config.binary.basetypes_major_version|string||PostgreSQL mahor version of server, e.g. 905, (select PG_VERSION_NUM()/100);
epgl.pglogical_config.pg_version|string||PostgreSQL server_version of server, e.g. 90506  (select PG_VERSION_NUM();)

### epgl:start_subscriber options
It is possible to define some options when start_subscriber, i.e. call `epgl:start_subscriber(SubscriberId, DBArgs, Callbacks, Options)` 
Forth argument Options is a map where you can set one of the following options: 
Options|Type|Default|Description
---|---|---|---
auto_cast|boolean|true|If you use textual output format you can set this option to true to cast some of the values to erlang representation
binary_mode|boolean|false|Only for pglogical plugin. Use binary output format. You should also check/set epgl.pglogical_config.binary parameters
reload_columns_on_metadata_msg|boolean|false|re-load columns datatypes when receive metadata_msg. set it true if auto_cast = true

## Missing features 
- Add a new table to the replication_set and initial data replication to subscribers on the fly.

## Test Setup

In order to run the epgsql tests, you will need to set up a local
Postgres database that runs within its own, self-contained directory,
in order to avoid modifying the system installation of Postgres.

NOTE: you will need the pglogical extension to run these tests!  
See https://2ndquadrant.com/en/resources/pglogical/pglogical-installation-instructions/
On Ubuntu, you can install them with the script `install_pglogical.sh`. 
It executes steps described in the page above:
    `sudo ./install_pglogical.sh`

1. `./setup_test_db.sh` # This sets up an installation of Postgres in datadir/
2. `./start_test_db.sh` # Starts a Postgres instance on its own port (10432).
3. `make create_testdbs` # Creates the test database environment.
4. `make test` # Runs the tests


## Examples

Examples available in `examples/`. Steps 1-3 of 'Test Setup' should be done in oder to run examples.
