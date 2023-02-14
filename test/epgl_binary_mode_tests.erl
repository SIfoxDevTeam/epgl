-module(epgl_binary_mode_tests).

-include_lib("eunit/include/eunit.hrl").
-export([handle_replication_msg/2]).

all_test_() ->
    case epgl_tests:is_pglogical_exists() of
        true ->

            [
                {setup,
                    fun start/0,
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
                    fun start/0,
                    fun stop/1,
                    fun(Pid) ->
                        {inorder,
                            [
                                connection_test(Pid),
                                start_replication_test(Pid)
                            ]}
                    end
                }
            ];
        false -> []
    end.

start() ->
    application:set_env(epgl, pglogical_config, [{binary, [{bigendian,1}]}]),
    application:set_env(epgl, two_msgs_for_pk_update, false),
    application:set_env(epgl, max_reconnect_attempts, 10),
    application:set_env(epgl, repl_slot_output_plugin, pglogical),

    DBArgs = [{hostname, "localhost"}, {port, 10432},
        {database, "epgl_test_db"}, {username, "epgl_test"}, {password, "epgl_test"}],
    Opts = #{"public.test_table1" => [?MODULE], "public.test_table3" => [?MODULE]},
    Name = list_to_atom("epgl_subscriber_" ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, Pid} = epgl:start_subscriber(Name, DBArgs, Opts, #{binary_mode => true}),
    epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    Pid.

stop(Pid) ->
    epgl:stop(Pid).

connection_test(Pid) ->
    ?_assert(is_pid(Pid)).

create_replication_slot_test(Pid) ->
    ?_assertMatch({ok, _SnapshotName, _ConsistentPoint}, epgl:create_replication_slot(Pid, "epgl_test_repl_slot")).

drop_replication_slot_test(Pid) ->
    ?_assertEqual(ok, epgl:drop_replication_slot(Pid, "epgl_test_repl_slot")).

get_table_initial_state_test(Pid) ->
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    {ok, _Columns, Values} = epgl:get_table_initial_state(Pid, "test_table1", SnapshotName),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual([{1,<<"one">>}, {2,<<"two">>}, {3,null}, {6,<<"six">>}], Values).

init_replication_set_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    {ok, SnapshotName, _ConsistentPoint} = epgl:create_replication_slot(Pid, "epgl_test_repl_slot"),
    ExpectedMsgs = [
        {#{"public.test_table1" => [<<"id">>,<<"value">>]},
            [
                {row,"public.test_table1",insert,[1,<<"one">>]},
                {row,"public.test_table1",insert,[2,<<"two">>]},
                {row,"public.test_table1",insert,[3,null]},
                {row,"public.test_table1",insert,[6,<<"six">>]}
            ]}
    ],
    ok = epgl:init_replication_set(Pid, "epgl_test_repl_set_1", SnapshotName),
    Res = receive_replication_msgs(ExpectedMsgs),
    true = erlang:unregister(?MODULE),
    ok = epgl:drop_replication_slot(Pid, "epgl_test_repl_slot"),
    ?_assertEqual(ok, Res).

start_replication_test(Pid) ->
    true = erlang:register(?MODULE, self()),
    truncate_tables(), %% Clean any old data from previous tests
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
                    [1,true,99,1,2,3,4.0,
                        5.1,<<"'">>,<<"c_text">>,<<"c_varchar">>,
                        {2016,10,25},{16,28,56.669049},
                        {{16,48,56.669049},0},{{2016,10,25},{16,28,56.669049}},
                        {{2016,11,25},{16,28,56.669049}},{{5,0,0.0},5,0},
                        {{127,0,0,1},32},{127,0,0,2},
                        <<"{\"a\": 2, \"b\": [\"c\", \"d\"]}">>]}
           ]},
        {#{"public.test_table1" => [<<"id">>,<<"value">>],
            "public.test_table3" => [<<"id">>,<<"c_bool">>,<<"c_char">>,<<"c_int2">>,<<"c_int4">>,<<"c_int8">>,
                <<"c_float4">>,<<"c_float8">>,<<"c_bytea">>,<<"c_text">>,<<"c_varchar">>,
                <<"c_date">>,<<"c_time">>,<<"c_timetz">>,<<"c_timestamp">>,
                <<"c_timestamptz">>,<<"c_interval">>,<<"c_cidr">>,<<"c_inet">>,<<"c_jsonb">>]
        },
            [
                {row,"public.test_table1",delete,[4,null]},
                {row,"public.test_table1",delete,[5,null]},
                {row,"public.test_table1",delete,[6,null]},
                {row,"public.test_table3",delete,
                    [1,null,null,null,null,null,null,null,null,null,null,null,
                    null,null,null,null,null,null,null,null]}
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
            %%ct:print("replication_msg ~p  ~p~n", [ColumnsDescription, Fields]),
%%            {_ColumnsDescription2, Fields2} = Msg,
%%            lists:foreach(fun(X) -> ct:print("~p~n", [X]) end, lists:zip(Fields, Fields2)),
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
    {ok, C} = epgsql:connect("localhost", "epgl_test", "epgl_test",
        [{database, "epgl_test_db"}, {port, 10432}]),
    {ok, 1} = epgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four');"),
    {ok, 1} = epgsql:squery(C, "insert into test_table2 (id, value) values (3, 'three');"),
    {ok, 1} = epgsql:squery(C, "delete from test_table2 where id = 3;"),
    {ok, 1} = epgsql:squery(C, "update test_table1 set value = 'four_v2' where id = 4;"),
    [{ok, 1}, {ok, 1}] = epgsql:squery(C,
        "insert into test_table1 (id, value) values (5, 'five');insert into test_table1 (id, value) values (6, 'six');"),
    {ok, 1} = epgsql:squery(C, "insert into test_table3 (id, c_bool, c_char, c_int2, c_int4, c_int8, c_float4, c_float8, c_bytea, c_text, c_varchar,
        c_date, c_time, c_timetz, c_timestamp, c_timestamptz, c_interval, c_cidr, c_inet, c_jsonb)
        values (1, true, 'c', 1, 2, 3, 4.0, 5.1, E'\\047', 'c_text', 'c_varchar',
            '2016-10-25', '16:28:56.669049', '16:48:56.669049', '2016-10-25 16:28:56.669049', '2016-11-25 16:28:56.669049',
            age('2016-10-25 16:28:56.669049', '2016-10-20 11:28:56.669049'), '127.0.0.1', '127.0.0.2',
            '{\"a\": 2, \"b\": [\"c\", \"d\"]}');"),
    [{ok, 3}, {ok, 1}] = epgsql:squery(C, "delete from test_table1 where id >= 4;delete from test_table3 where id = 1;"),
    epgsql:close(C).

truncate_tables() ->
    {ok, C} = epgsql:connect("localhost", "epgl_test", "epgl_test",
        [{database, "epgl_test_db"}, {port, 10432}]),
    _ = epgsql:squery(C, "truncate test_table1"),
    _ = epgsql:squery(C, "truncate test_table2"),
    _ = epgsql:squery(C, "truncate test_table3"),
    epgsql:close(C).
