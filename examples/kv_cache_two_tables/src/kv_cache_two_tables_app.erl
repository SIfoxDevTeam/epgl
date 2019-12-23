-module(kv_cache_two_tables_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case kv_cache_two_tables_sup:start_link() of
        {ok, _Pid} = Sup ->
            %% after we started subscribers of replication messages, start replication

            %% re-create replication_slot
            epgl:drop_replication_slot(epgl_subscriber_1, "epgl_test_repl_slot"),
            {ok, SnapshotName, _} = epgl:create_replication_slot(epgl_subscriber_1, "epgl_test_repl_slot"),

            %% get initial table state, callback method handle_replication_msg will be called
            ok = epgl:init_replication_set(epgl_subscriber_1, "epgl_test_repl_set_1", SnapshotName),

            ok = epgl:start_replication(epgl_subscriber_1, "epgl_test_repl_slot", "epgl_test_repl_set_1"),
            Sup;
        Error -> Error
    end.

stop(_State) ->
    ok.
