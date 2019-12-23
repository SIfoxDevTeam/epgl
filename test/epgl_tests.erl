-module(epgl_tests).

-include_lib("eunit/include/eunit.hrl").
-export([handle_replication_msg/2, is_pglogical_exists/0]).
-define(DB_ARGS, [{hostname, "localhost"}, {port, 10432},
    {database, "epgl_test_db"}, {username, "epgl_test"}, {password, "epgl_test"}]).

all_test_() ->
    PglogicalTests =
        case is_pglogical_exists() of
            true ->
                [
                    {setup,
                        fun start_pglogical1/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    create_replication_slot_test(Pid),
                                    drop_replication_slot_test(Pid),
                                    get_table_initial_state_test(Pid),
                                    init_replication_set_test(Pid)]}
                        end},
                    {setup,
                        fun start_pglogical1/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    start_replication_test(Pid)
                                ]}
                        end
                    },
                    {setup,
                        fun start_pglogical2/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    pk_update_test(Pid)
                                ]}
                        end
                    },
                    {setup,
                        fun start_pglogical3/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    create_replication_slot_test(Pid),
                                    drop_replication_slot_test(Pid),
                                    get_table_initial_state_cast_test(Pid),
                                    init_replication_set_cast_test(Pid)]}
                        end},
                    {setup,
                        fun start_pglogical3/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    start_replication_cast_test(Pid)
                                ]}
                        end
                    }
                ];
            false -> []
        end,
    PgoutputTests =
        case pg_version() >= 100000 of
            true ->
                [
                    {setup,
                        fun start_pgoutput1/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    create_replication_slot_test(Pid),
                                    drop_replication_slot_test(Pid),
                                    get_table_initial_state_test(Pid),
                                    init_replication_set_test(Pid)]}
                        end},
                    {setup,
                        fun start_pgoutput1/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    start_replication_test(Pid)
                                ]}
                        end
                    },
                    {setup,
                        fun start_pgoutput2/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    pk_update_test(Pid)
                                ]}
                        end
                    },
                    {setup,
                        fun start_pgoutput3/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    create_replication_slot_test(Pid),
                                    drop_replication_slot_test(Pid),
                                    get_table_initial_state_cast_test(Pid),
                                    init_replication_set_cast_test(Pid)]}
                        end},
                    {setup,
                        fun start_pgoutput3/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    start_replication_cast_test(Pid)
                                ]}
                        end
                    },
                    {setup,
                        fun start_pgoutput1/0,
                        fun stop/1,
                        fun(Pid) ->
                            {inorder,
                                [
                                    connection_test(Pid),
                                    create_replication_slot_temporary_test(Pid)
                                ]}
                        end}
                ];
            false -> []
        end,
    PglogicalTests ++ PgoutputTests.

start(TwoMsgsForPKUpdate, AutoCast, Plugin) ->
    application:set_env(epgl, two_msgs_for_pk_update, TwoMsgsForPKUpdate),
    application:set_env(epgl, max_reconnect_attempts, 10),
    application:set_env(epgl, repl_slot_output_plugin, Plugin),

    Opts = #{"public.test_table1" => [?MODULE], "public.test_table3" => [?MODULE]},
    Name = list_to_atom("epgl_subscriber_" ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, Pid} = epgl:start_subscriber(Name, ?DB_ARGS, Opts, #{auto_cast => AutoCast}),
    epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    Pid.

start_pglogical1() ->
    TwoMsgsForPKUpdate = false,
    AutoCast = false,
    Plugin = pglogical,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

start_pglogical2() ->
    TwoMsgsForPKUpdate = true,
    AutoCast = false,
    Plugin = pglogical,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

start_pglogical3() ->
    TwoMsgsForPKUpdate = false,
    AutoCast = true,
    Plugin = pglogical,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

start_pgoutput1() ->
    TwoMsgsForPKUpdate = false,
    AutoCast = false,
    Plugin = pgoutput,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

start_pgoutput2() ->
    TwoMsgsForPKUpdate = true,
    AutoCast = false,
    Plugin = pgoutput,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

start_pgoutput3() ->
    TwoMsgsForPKUpdate = false,
    AutoCast = true,
    Plugin = pgoutput,
    start(TwoMsgsForPKUpdate, AutoCast, Plugin).

stop(Pid) ->
    catch epgl:stop(Pid).

connection_test(Pid) ->
    ?_assert(is_pid(Pid)).

create_replication_slot_test(Pid) ->
    ?_assertMatch({ok, _SnapshotName, _ConsistentPoint}, epgl:create_replication_slot(Pid, "epgl_test_repl_slot")).

create_replication_slot_temporary_test(Pid) ->
    {ok, C} = connect(),
    {ok, _, [{0}]} = epgsql:equery(C, "select count(1) from pg_replication_slots where slot_name = $1",
        ["epgl_test_repl_slot"]),

    CreateResult = epgl:create_replication_slot(Pid, "epgl_test_repl_slot", #{temporary => true}),

    {ok, _, [{Cnt1}]} = epgsql:equery(C, "select count(1) from pg_replication_slots where slot_name = $1",
        ["epgl_test_repl_slot"]),

    stop(Pid),

    timer:sleep(1000),
    {ok, _, [{Cnt2}]} = epgsql:equery(C, "select count(1) from pg_replication_slots where slot_name = $1",
        ["epgl_test_repl_slot"]),
    epgsql:close(C),

    [?_assertMatch({ok, _SnapshotName, _ConsistentPoint}, CreateResult),
        ?_assertEqual(1, Cnt1), ?_assertEqual(0, Cnt2)].

drop_replication_slot_test(Pid) ->
    ?_assertEqual(ok, epgl:drop_replication_slot(Pid, "epgl_test_repl_slot")).

get_table_initial_state_test(Pid) ->
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    {ok, _Columns, Values} = epgl:get_table_initial_state(Pid, "test_table1", SnapshotName),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual([{<<"1">>,<<"one">>}, {<<"2">>,<<"two">>}, {<<"3">>,null}], Values).

get_table_initial_state_cast_test(Pid) ->
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    {ok, _Columns, Values} = epgl:get_table_initial_state(Pid, "test_table1", SnapshotName),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual([{1,<<"one">>}, {2,<<"two">>}, {3,null}], Values).

init_replication_set_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[<<"1">>,<<"one">>]},
                {row,"public.test_table1",insert,[<<"2">>,<<"two">>]},
                {row,"public.test_table1",insert,[<<"3">>,null]}
            ]}
    ],
    ok = epgl:init_replication_set(Pid, "epgl_test_repl_set_1", SnapshotName),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual(ok, Res).

init_replication_set_cast_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[1,<<"one">>]},
                {row,"public.test_table1",insert,[2,<<"two">>]},
                {row,"public.test_table1",insert,[3,null]}
            ]}
    ],
    ok = epgl:init_replication_set(Pid, "epgl_test_repl_set_1", SnapshotName),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual(ok, Res).

start_replication_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, _, _} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    make_changes(),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[<<"4">>,<<"four">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",update,[<<"4">>,<<"four_v2">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[<<"5">>,<<"five">>]},
                {row,"public.test_table1",insert,[<<"6">>,<<"six">>]}
            ]},
        {#{"public.test_table3" => [<<"id">>,<<"c_bool">>,<<"c_char">>,<<"c_int2">>,<<"c_int4">>,<<"c_int8">>,
            <<"c_float4">>,<<"c_float8">>,<<"c_bytea">>,<<"c_text">>,<<"c_varchar">>,
            <<"c_date">>,<<"c_time">>,<<"c_timetz">>,<<"c_timestamp">>,
            <<"c_timestamptz">>,<<"c_interval">>,<<"c_cidr">>,<<"c_inet">>,<<"c_jsonb">>]},
            [
                {row,"public.test_table3",insert,
                    [<<"1">>,<<"t">>,<<"c">>,<<"1">>,<<"2">>,<<"3">>,<<"4">>,
                    <<"5.1">>,<<"\\x27">>,<<"c_text">>,<<"c_varchar">>,
                    <<"2016-10-25">>,<<"16:28:56.669049">>,
                    <<"16:48:56.669049+00">>,<<"2016-10-25 16:28:56.669049">>,
                    <<"2016-11-25 16:28:56.669049+00">>,<<"5 days 05:00:00">>,
                    <<"127.0.0.1/32">>,<<"127.0.0.2">>, <<"{\"a\": 2, \"b\": [\"c\", \"d\"]}">>]}
           ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",update,[<<"7">>,<<"six">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>],
            "public.test_table3" => [<<"id">>,<<"c_bool">>,<<"c_char">>,<<"c_int2">>,<<"c_int4">>,<<"c_int8">>,
                <<"c_float4">>,<<"c_float8">>,<<"c_bytea">>,<<"c_text">>,<<"c_varchar">>,
                <<"c_date">>,<<"c_time">>,<<"c_timetz">>,<<"c_timestamp">>,
                <<"c_timestamptz">>,<<"c_interval">>,<<"c_cidr">>,<<"c_inet">>,<<"c_jsonb">>]},
            [
                {row,"public.test_table1",delete,[<<"4">>,null]},
                {row,"public.test_table1",delete,[<<"5">>,null]},
                {row,"public.test_table1",delete,[<<"7">>,null]},
                {row,"public.test_table3",delete,
                    [<<"1">>,null,null,null,null,null,null,null,null,null,null,null,
                    null,null,null,null,null,null,null,null]}
            ]}
    ],
    ok = epgl:start_replication(Pid, "epgl_test_repl_slot", "epgl_test_repl_set_1"),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ?_assertEqual(ok, Res).

start_replication_cast_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, _, _} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    make_changes(),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[4,<<"four">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",update,[4,<<"four_v2">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[5,<<"five">>]},
                {row,"public.test_table1",insert,[6,<<"six">>]}
            ]},
        {#{"public.test_table3" => [<<"id">>,<<"c_bool">>,<<"c_char">>,<<"c_int2">>,<<"c_int4">>,<<"c_int8">>,
            <<"c_float4">>,<<"c_float8">>,<<"c_bytea">>,<<"c_text">>,<<"c_varchar">>,
            <<"c_date">>,<<"c_time">>,<<"c_timetz">>,<<"c_timestamp">>,
            <<"c_timestamptz">>,<<"c_interval">>,<<"c_cidr">>,<<"c_inet">>,<<"c_jsonb">>]},
            [
                {row,"public.test_table3",insert,
                    [1, true, <<"c">>,1,2,3,4.0,
                        5.1,<<"'">>,<<"c_text">>,<<"c_varchar">>,
                        <<"2016-10-25">>, {16,28,56},
                        <<"16:48:56.669049+00">>,<<"2016-10-25 16:28:56.669049">>,
                        <<"2016-11-25 16:28:56.669049+00">>,<<"5 days 05:00:00">>,
                        <<"127.0.0.1/32">>,<<"127.0.0.2">>, <<"{\"a\": 2, \"b\": [\"c\", \"d\"]}">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",update,[7,<<"six">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>],
            "public.test_table3" => [<<"id">>,<<"c_bool">>,<<"c_char">>,<<"c_int2">>,<<"c_int4">>,<<"c_int8">>,
                <<"c_float4">>,<<"c_float8">>,<<"c_bytea">>,<<"c_text">>,<<"c_varchar">>,
                <<"c_date">>,<<"c_time">>,<<"c_timetz">>,<<"c_timestamp">>,
                <<"c_timestamptz">>,<<"c_interval">>,<<"c_cidr">>,<<"c_inet">>,<<"c_jsonb">>]},
            [
                {row,"public.test_table1",delete,[4,null]},
                {row,"public.test_table1",delete,[5,null]},
                {row,"public.test_table1",delete,[7,null]},
                {row,"public.test_table3",delete,
                    [1,null,null,null,null,null,null,null,null,null,null,null,
                        null,null,null,null,null,null,null,null]}
            ]}
    ],
    ok = epgl:start_replication(Pid, "epgl_test_repl_slot", "epgl_test_repl_set_1"),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ?_assertEqual(ok, Res).

pk_update_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, _, _} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    pk_update(),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[<<"6">>,<<"six">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",delete,[<<"6">>,null]},
                {row,"public.test_table1",insert,[<<"7">>,<<"six">>]}
            ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",delete,[<<"7">>,null]}
            ]}
    ],
    ok = epgl:start_replication(Pid, "epgl_test_repl_slot", "epgl_test_repl_set_1"),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ?_assertEqual(ok, Res).

receive_replication_msgs([]) ->
    ok;
receive_replication_msgs([Msg | T]) ->
    receive
        {replication_msg, ColumnsDescription, Fields} ->
%%            ct:print("replication_msg ~p  ~p~n", [ColumnsDescription, Fields]),
            {ColumnsDescription, Fields} = Msg,
            receive_replication_msgs(T)
    after
        60000 ->
            error_timeout
    end.

handle_replication_msg(ColumnsDescription, Fields) ->
    ?MODULE ! {replication_msg, ColumnsDescription, Fields},
    ok.

make_changes() ->
    {ok, C} = connect(),
    {ok, 1} = epgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four');"),
    {ok, 1} = epgsql:squery(C, "insert into test_table2 (id, value) values (3, 'three');"),
    {ok, 1} = epgsql:squery(C, "delete from test_table2 where id = 3;"),
    {ok, 1} = epgsql:squery(C, "update test_table1 set value = 'four_v2' where id = 4;"),
    [{ok, 1}, {ok, 1}] = epgsql:squery(C,
        "insert into test_table1 (id, value) values (5, 'five');
         insert into test_table1 (id, value) values (6, 'six');"),
    {ok, 1} = epgsql:squery(C, "insert into test_table3 (id, c_bool, c_char, c_int2, c_int4, c_int8, c_float4, c_float8, c_bytea, c_text, c_varchar,
        c_date, c_time, c_timetz, c_timestamp, c_timestamptz, c_interval, c_cidr, c_inet, c_jsonb)
        values (1, true, 'c', 1, 2, 3, 4.0, 5.1, E'\\047', 'c_text', 'c_varchar',
            '2016-10-25', '16:28:56.669049', '16:48:56.669049', '2016-10-25 16:28:56.669049', '2016-11-25 16:28:56.669049',
            age('2016-10-25 16:28:56.669049', '2016-10-20 11:28:56.669049'), '127.0.0.1', '127.0.0.2',
            '{\"a\": 2, \"b\": [\"c\", \"d\"]}');"),
    {ok, 1} = epgsql:squery(C, "update test_table1 set id = 7 where id = 6;"),
    [{ok, 3}, {ok, 1}] = epgsql:squery(C, "delete from test_table1 where id >= 4;delete from test_table3 where id = 1;"),
    epgsql:close(C).

pk_update() ->
    {ok, C} = connect(),
    {ok, 1} = epgsql:squery(C, "insert into test_table1 (id, value) values (6, 'six');"),
    {ok, 1} = epgsql:squery(C, "update test_table1 set id = 7 where id = 6;"),
    {ok, 1} = epgsql:squery(C, "delete from test_table1 where id >= 4;"),
    epgsql:close(C).

pg_version() ->
    {ok, C} = connect(),
    {ok, _, [{Version}]} = epgsql:equery(C, ["select current_setting('server_version_num')::int"]),
    epgsql:close(C),
    Version.

is_pglogical_exists() ->
    {ok, C} = connect(),
    {ok, _, [{Exists}]} = epgsql:equery(C, ["select count(1) > 0 from pg_catalog.pg_extension where extname = 'pglogical'"]),
    epgsql:close(C),
    Exists.

connect() ->
    Host = proplists:get_value(hostname, ?DB_ARGS),
    DBName = proplists:get_value(database, ?DB_ARGS),
    User = proplists:get_value(username, ?DB_ARGS),
    Password = proplists:get_value(password, ?DB_ARGS),
    Port     = proplists:get_value(port, ?DB_ARGS, 5432),
    epgsql:connect(Host, User, Password, [{database, DBName}, {port, Port}]).