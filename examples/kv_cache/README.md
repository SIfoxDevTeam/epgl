kv_cache
=====

Selects data from the table public.test_table1 
and listens for all changes for this table.
Insert data from this table to internal ETS.

You can use the following method for testing:
```
    kv_cache_server:get_value_by_key(Key).
    kv_cache_server:get_full_table().
```

Build
-----
In order to run this example, you will need to set up a local
Postgres database and start a Postgres instance on its own port (10432).
See 'Test Setup' in the main README.
When you start Postgres instance use the following command to build and run the example:

    $ rebar3 run
