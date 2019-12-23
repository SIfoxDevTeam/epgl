-module(kv_cache_server).

-behaviour(gen_server).
-behaviour(epgl_subscriber).

%% API
-export([start_link/0]).
-export([get_value_by_key/1, get_full_table/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

%% epgl_subscriber callbacks
-export([handle_replication_msg/2]).

-include_lib("epgl/include/epgl.hrl").

-record(state, {
    epgl_pid :: pid(),
    db_args :: list(),
    max_reconnect_attempts :: integer() | infinite,
    reconnect_interval :: integer()}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_value_by_key(Key) ->
    ets:lookup(?MODULE, Key).

get_full_table() ->
    ets:tab2list(?MODULE).

%% gen_server callbacks

init([]) ->
    process_flag(trap_exit, true),
    ets:new(?MODULE, [named_table, public]),

    {ok, Database} = application:get_env(kv_cache, epgl_database),
    {ok, Username} = application:get_env(kv_cache, epgl_username),
    {ok, Password} = application:get_env(kv_cache, epgl_password),
    {ok, Host} = application:get_env(kv_cache, epgl_host),
    {ok, Port} = application:get_env(kv_cache, epgl_port),

    DBArgs = [
        {hostname, Host},
        {port, Port},
        {database, Database},
        {username, Username},
        {password, Password}
    ],

    {ok, EPGLPid} = start_replication(DBArgs),
    {ok, #state{
        epgl_pid = EPGLPid,
        db_args = DBArgs,
        reconnect_interval = application:get_env(epgl, reconnect_interval, 10),
        max_reconnect_attempts = application:get_env(epgl, max_reconnect_attempts, infinite)}}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_info({restart_epgl, ReconnectCount},
    State = #state{epgl_pid = undefined, max_reconnect_attempts = MaxReconnectAttempts})
    when is_integer(MaxReconnectAttempts) andalso ReconnectCount >= MaxReconnectAttempts ->
    io:format("Maximum restart_epgl attempts threshold (~p) is reached ~n", [ReconnectCount]),
    {stop, max_reconnect_attempts, State};

handle_info({restart_epgl, ReconnectCount},
    State = #state{epgl_pid = undefined, reconnect_interval = ReconnectInterval, db_args = DBArgs}) ->
    io:format("Trying to restart EPGL ~n"),
    case start_replication(DBArgs) of
        {ok, EPGLPid} ->
            io:format("EPGL is restarted ~n"),
            {noreply, State#state{epgl_pid = EPGLPid}};
        {error, Reason} ->
            io:format("Cannot restart EPGL ~p~n ", [Reason]),
            timer:send_after((ReconnectInterval * 1000), {restart_epgl, ReconnectCount + 1}),
            {noreply, State}
    end;

handle_info({'EXIT', Pid, Reason}, State = #state{epgl_pid = EPGLPid}) when Pid == EPGLPid ->
    io:format("EPGL has been stopped ~p~n", [Reason]),
    self() ! {restart_epgl, 0},
    {noreply, State#state{epgl_pid = undefined}};

handle_info(_Msg, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, #state{epgl_pid = EPGLPid}) ->
    case EPGLPid of
        undefined -> ok;
        _ -> catch epgl:stop(EPGLPid)
    end,
    ok.

handle_replication_msg(Metadata, Rows) ->
    io:format("Got handle_replication_msg: Metadata ~p. Rows ~p ~n", [Metadata, Rows]),
    lists:foreach(fun process_row/1, Rows).


process_row(#row{table_name = "public.test_table1", change_type = delete, fields = [Id, _Value]}) ->
    io:format("delete key from ETS ~p ~n", [Id]),
    ets:delete(?MODULE, Id),
    ok;

process_row(#row{table_name = "public.test_table1", fields = [Id, Value]}) ->
    io:format("insert/update row to ETS ~p ~n", [{Id, Value}]),
    ets:insert(?MODULE, {Id, Value}),
    ok;

process_row(#row{table_name = TableName}) ->
    io:format("got insert/update request for unknown table ~p ~n", [TableName]),
    ok.

start_replication(DBArgs) ->
    {ok, ReplicationSetName} = application:get_env(kv_cache, epgl_replication_set_name),
    {ok, ReplicationSlot} = application:get_env(kv_cache, epgl_replication_slot),
    Callbacks = #{
        "public.test_table1" => [?MODULE]
    },

    TablesOrder = #{ %%Initialization order, tables name without schema
        "test_table1" => 1
    },

    {ok, Pid} = epgl:start_subscriber(epgl_subscriber_1, DBArgs, Callbacks, #{auto_cast => true}),

    %% re-create replication_slot
    %%epgl:drop_replication_slot(Pid, ReplicationSlot), %% We do not need drop if we use temporary slot
    {ok, SnapshotName, XLogPosition} = epgl:create_replication_slot(Pid, ReplicationSlot, #{temporary => true}),

    io:format("Replication slot is created, snapshot: ~s, xlogposition: ~s ~n", [SnapshotName, XLogPosition]),
    ok = epgl:init_replication_set(Pid, ReplicationSetName, SnapshotName, TablesOrder),
    ok = epgl:start_replication(Pid, ReplicationSlot, ReplicationSetName),
    {ok, Pid}.