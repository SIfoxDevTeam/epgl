kv_cache_two_tables
=====

Similar concept as for kv_cache, but two different subscribers for two tables public.test_table1 and public.test_table4.

You can use the following method for testing:
```
    kv_cache_two_tables_server1:get_value_by_key(Key).
    kv_cache_two_tables_server1:get_full_table().
    
    kv_cache_two_tables_server2:get_value_by_key(Key).
    kv_cache_two_tables_server2:get_full_table().
```

Build
-----
In order to run this example, you will need to set up a local
Postgres database and start a Postgres instance on its own port (10432).
See 'Test Setup' in the main README.
When you start Postgres instance use the following command to build and run the example:

    $ rebar3 run
