-module(kv_cache_two_tables_server2).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_value_by_key/1, get_full_table/0]).
-export([handle_replication_msg/2]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

-include_lib("epgl/include/epgl.hrl").

-define(TABLE_NAME, "public.test_table4").
-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_value_by_key(Key) ->
    ets:lookup(?MODULE, Key).

get_full_table() ->
    ets:tab2list(?MODULE).

init([]) ->
    ets:new(?MODULE, [named_table, public]),
    {ok, #state{}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

handle_replication_msg(_ColumnsDescription, Fields) ->
    lists:foreach(fun process_row/1, Fields).

process_row(#row{table_name = ?TABLE_NAME, change_type = delete, fields = [Id, _Value]}) ->
    io:format("SRV2: delete key from ETS ~p ~n", [Id]),
    ets:delete(?MODULE, Id);
process_row(#row{table_name = ?TABLE_NAME, fields = [Id, Value]}) ->
    io:format("SRV2: insert/update row to ETS ~p ~n", [{Id, Value}]),
    ets:insert(?MODULE, {Id, Value});
process_row(_Row) ->
    ok.