%%%
%%%  Copyright 2017 Sifox
%%%
%%%  Licensed under the Apache License, Version 2.0 (the "License");
%%%  you may not use this file except in compliance with the License.
%%%  You may obtain a copy of the License at
%%%
%%%      http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%  Unless required by applicable law or agreed to in writing, software
%%%  distributed under the License is distributed on an "AS IS" BASIS,
%%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%  See the License for the specific language governing permissions and
%%%  limitations under the License.
%%%

-module(epgl_subscriber).

-behaviour(gen_server).

%% API
-export([start_link/4]).
-export([stop/1]).
-export([handle_x_log_data/4]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

-define(APP, epgl).
-define(PGLOGICAL_QUEUE, "pglogical.queue").

-include("epgl.hrl").
-include("epgl_int.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-callback handle_replication_msg(
    Metadata :: #{TableName :: string() => [ColumnName :: string()]},
    Rows :: [#row{}]) -> ok.

%% Optional
-callback handle_logical_decoding_msg(#logical_decoding_msg{}) -> ok.

-optional_callbacks([ handle_logical_decoding_msg/1 ]).

-record(state, {
    db_connect_opts         :: epgl:db_args(),
    conn                    :: pid() | undefined,
    conn_normal             :: pid() | undefined,
    logical_decoding_msg_callback :: atom(),
    callbacks               :: map(),
    metadata = #{}          :: map(),
    logical_decoding_msgs = [] :: list(),
    rows = []               :: list(),
    replication_slot        :: string() | undefined,
    replication_set         :: string() | undefined,
    last_processed_lsn      :: integer(),
    check_lsn_mode          :: skip | log | off,
    max_reconnect_attempts  :: integer() | infinite,
    reconnect_interval      :: integer(),
    debug                   :: boolean(),
    two_msgs_for_pk_update  :: boolean(),
    options = #{}           :: map(),
    columns = #{}           :: map(),
    pglogical_config = []   :: list(),
    output_plugin           :: atom(),
    align_lsn               :: boolean()
}).

-record(cb_state, {
    pid :: pid(),
    debug :: boolean(),
    decoder_module :: atom()}).

%% [DBArgs, Callbacks]. Callbacks - #{TableName => [CallbackModules]}
start_link(SubscriberId, DBArgs, Callbacks, Options) ->
    gen_server:start_link({local, SubscriberId}, ?MODULE, [DBArgs, Callbacks, Options], []).

stop(C) ->
    gen_server:stop(C),
    ok.

init([DBArgs, Callbacks, Options]) ->
    process_flag(trap_exit, true),
    {ok, Conn} = connect(DBArgs, replication),

    PglogicalConfig = application:get_env(?APP, pglogical_config, []),

    State = #state{
        conn = Conn,
        logical_decoding_msg_callback = application:get_env(?APP, logical_decoding_msg_callback, undefined),
        callbacks = Callbacks,
        db_connect_opts = DBArgs,
        last_processed_lsn = 0,
        check_lsn_mode = application:get_env(?APP, check_lsn_mode, skip),
        reconnect_interval = application:get_env(?APP, reconnect_interval, 10),
        max_reconnect_attempts = application:get_env(?APP, max_reconnect_attempts, infinite),
        debug = application:get_env(?APP, debug, false),
        two_msgs_for_pk_update = application:get_env(?APP, two_msgs_for_pk_update, false),
        options = Options,
        pglogical_config = PglogicalConfig,
        output_plugin = application:get_env(?APP, repl_slot_output_plugin, pgoutput),
        align_lsn = application:get_env(?APP, align_lsn, true)
    },

    BinaryMode = get_option(binary_mode, Options),
    AutoCast = get_option(auto_cast, Options),
    LoadColumnsData = AutoCast orelse BinaryMode,

    NewState = case LoadColumnsData of
       true ->
           {ok, ConnNormal} = connect(DBArgs, normal),
           NewPglogicalConfig =
               case BinaryMode of
                   true -> set_binarymode_pglogical_params(ConnNormal, PglogicalConfig);
                   false -> PglogicalConfig
               end,

           F = fun(Table, Acc) ->
               Acc#{Table => get_column_types(ConnNormal, Table)}
               end,
           ColumnsDataTypes = lists:foldl(F, #{}, maps:keys(Callbacks)),

           case get_option(reload_columns_on_metadata_msg, Options) of
               false ->
                   ok = epgsql:close(ConnNormal),
                   State#state{columns = ColumnsDataTypes, pglogical_config = NewPglogicalConfig};
               true ->
                   State#state{columns = ColumnsDataTypes, conn_normal = ConnNormal, pglogical_config = NewPglogicalConfig}
           end;
       false -> State
   end,

    {ok, NewState}.

handle_call({drop_replication_slot, ReplicationSlot}, _From, State = #state{conn = Conn}) ->
    case epgsql:squery(Conn, ["DROP_REPLICATION_SLOT ", ReplicationSlot]) of
        [{ok, _, _}, {ok, _, _}] -> {reply, ok, State};
        {ok, _, _} -> {reply, ok, State};
        Res -> {reply, Res, State}
    end;

handle_call({create_replication_slot, ReplicationSlot, Options}, _From,
    State = #state{conn = Conn, output_plugin = OutputPlugin}) ->
    %% Result slot_name, consistent_point, snapshot_name, output_plugin
    OutputPluginName =
        case OutputPlugin of
            pglogical -> "pglogical_output";
            pgoutput -> "pgoutput"
        end,
    Temporary =
        case maps:get(temporary, Options, false) of
            true -> " TEMPORARY ";
            _ -> ""
        end,
    case epgsql:squery(Conn,
        ["CREATE_REPLICATION_SLOT ", ReplicationSlot, Temporary, " LOGICAL ", OutputPluginName]) of
        {ok, _Columns, [RowsValues|_]} ->
            SnapshotName = binary_to_list(element(3, RowsValues)),
            ConsistentPoint = binary_to_list(element(2, RowsValues)),

            {reply, {ok, SnapshotName, ConsistentPoint}, State};
        Error -> {reply, Error, State}
    end;

handle_call({get_table_initial_state, TableName, SnapshotName}, _From,
    State = #state{db_connect_opts = Opts}) ->
    %% create new connection because main is in replication mode
    {ok, Conn} = connect(Opts, normal),

    {ok, _, _} = epgsql:squery(Conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
    {ok, _, _} = epgsql:squery(Conn, ["SET TRANSACTION SNAPSHOT '", SnapshotName, "'"]),

    Res = case get_option(binary_mode, State#state.options) of
              true ->
                  epgsql:equery(Conn, ["SELECT * FROM ", TableName]);
              false ->
                  {ok, Columns, Rows} =  epgsql:squery(Conn, ["SELECT * FROM ", TableName]),
                  case get_option(auto_cast, State#state.options) of
                      true ->
                          ColumnsDT = [element(3, Column) || Column <- Columns],
                          {ok, Columns, [
                              list_to_tuple([epgl_cast:cast(Value) || Value <- lists:zip(ColumnsDT, tuple_to_list(Values))]) || Values <- Rows
                          ]};
                      false ->
                          {ok, Columns, Rows}
                  end
          end,

    {ok, _, _} = epgsql:squery(Conn, "END TRANSACTION"),

    close_connection(Conn),

    {reply, Res, State};

handle_call({init_replication_set, ReplicationSets, SnapshotName, TablesOrder}, _From,
    State = #state{callbacks = Callbacks, db_connect_opts = Opts, output_plugin = OutputPlugin}) ->
    %% create new connection because main is in replication mode
    {ok, Conn} = connect(Opts, normal),

    {ok, _, _} = epgsql:squery(Conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
    {ok, _, _} = epgsql:squery(Conn, ["SET TRANSACTION SNAPSHOT '", SnapshotName, "'"]),

    [_|ReplSetsList] = lists:flatten([[",'", X, "'"] || X <- string:tokens(ReplicationSets, ",")]),

    Tables1 =
        case OutputPlugin of
            pglogical ->
                {ok, _, Tables0} = epgsql:squery(Conn,
                    ["SELECT DISTINCT nspname, relname FROM pglogical.tables
                        WHERE set_name = ANY(ARRAY[", ReplSetsList, "])"]),
                Tables0;
            pgoutput ->
                {ok, _, Tables0} = epgsql:squery(Conn,
                    ["SELECT DISTINCT schemaname, tablename FROM pg_catalog.pg_publication_tables
                        WHERE pubname = ANY(ARRAY[", ReplSetsList, "])"]),
                Tables0
        end,

    Tables =
        case TablesOrder of
            undefined -> Tables1;
            TablesOrder ->
                Tables2 = lists:filter(fun({_, T}) -> maps:is_key(T, TablesOrder)
                    orelse maps:is_key(binary_to_list(T), TablesOrder) end, Tables1),
                FSort =
                    fun({_, TA}, {_, TB}) ->
                        OA = maps:get(TA, TablesOrder, 0),
                        OB = maps:get(TB, TablesOrder, 0),
                        OA =< OB
                    end,
                lists:sort(FSort, Tables2)
        end,
    BinaryMode = get_option(binary_mode, State#state.options),
    AutoCast = not BinaryMode andalso get_option(auto_cast, State#state.options),

    F = fun({SchemaName, TableName}) ->
        FullName = binary_to_list(iolist_to_binary([SchemaName, ".", TableName])),
        case maps:get(FullName, Callbacks, []) of
            [] -> ok;
            CbModules ->
                {ok, ColumnsDsc, RowsValues} =
                    case BinaryMode of
                        true -> epgsql:equery(Conn, ["SELECT * FROM ", FullName]);
                        false -> epgsql:squery(Conn, ["SELECT * FROM ", FullName])
                    end,
                case RowsValues of
                    [] -> ok;
                    RowsValues ->
                        Columns = [ColumnName || #column{name = ColumnName} <- ColumnsDsc],
                        Rows = [
                            #row{
                                table_name = FullName,
                                change_type = insert,
                                fields = case AutoCast of
                                             true ->
                                                 [epgl_cast:cast(Field) || Field <- lists:zip([element(3, Column) || Column <- ColumnsDsc], tuple_to_list(X))];
                                             false -> tuple_to_list(X)
                                         end
                            } || X <- RowsValues
                        ],

                        lists:foreach(
                            fun(CbMod) ->
                                ok = CbMod:handle_replication_msg(#{FullName => Columns}, Rows)
                            end, CbModules)
                end
        end
        end,
    lists:foreach(F, Tables),

    {ok, _, _} = epgsql:squery(Conn, "END TRANSACTION"),

    close_connection(Conn),

    {reply, ok, State};

handle_call({start_replication, _, _}, _From, State = #state{replication_slot = ReplicationSlot})
    when ReplicationSlot =/= undefined ->
    {reply, {error, {already_started, ReplicationSlot}}, State};

handle_call({start_replication, ReplicationSlot, ReplicationSets}, _From,
    State = #state{conn = Conn, pglogical_config = PglogicalConfig, output_plugin = OutputPlugin, align_lsn = AlignLSN}) ->
    case start_replication(OutputPlugin, Conn, ReplicationSlot, ReplicationSets, PglogicalConfig, AlignLSN) of
        ok ->
            NewState = State#state{replication_set = ReplicationSets, replication_slot = ReplicationSlot},
            {reply, ok, NewState};
        Error -> {reply, Error, State}
    end;

handle_call({pglogical_msg, _StartLSN, _EndLSN, #begin_msg{}}, _From, State = #state{rows = Rows}) ->
    case Rows of
        [] -> {reply, ok, State};
        _ ->
            error_logger:error_msg("Rows sent not within BEGIN-COMMIT: ~p~n", [Rows]),
            {reply, {error, "Rows sent not within BEGIN-COMMIT"}, State}
    end;

handle_call({pglogical_msg, _StartLSN, EndLSN, #commit_msg{}}, _From, State) ->
    #state{
        rows = Rows,
        logical_decoding_msgs = LogicalDecodingMsgs,
        metadata = Metadata,
        callbacks = Callbacks,
        check_lsn_mode = CheckLSNMode,
        last_processed_lsn = LastEndLSN,
        debug = DebugMode,
        two_msgs_for_pk_update = TwoMsgsForPKUpdate,
        columns = Columns
    } = State,

    NewLastEndLSN =
        case LastEndLSN < EndLSN of
            true -> EndLSN;
            false -> LastEndLSN
        end,
    case check_lsn(CheckLSNMode, LastEndLSN, EndLSN, Rows) of
        ok ->
            case Rows of
                [] ->
                    {reply, ok, State#state{last_processed_lsn = NewLastEndLSN}};
                Rows ->
                    case DebugMode of
                        true -> error_logger:info_msg("Sending rows to Callback ~p~n", [lists:reverse(Rows)]);
                        false -> ok
                    end,
                    AutoCast = get_option(auto_cast, State#state.options) orelse get_option(binary_mode, State#state.options),

                    CallbackData = lists:foldl(
                        fun(X, Acc) ->
                            process_transaction_row(X, Metadata, Callbacks, Acc, Columns, AutoCast, TwoMsgsForPKUpdate)
                        end,
                        #{}, Rows),

                    lists:foreach(
                        fun({CbMod, {MetadataAcc, RowAcc}}) ->
                            ok = CbMod:handle_replication_msg(MetadataAcc, lists:flatten(RowAcc))
                        end, maps:to_list(CallbackData)),
                    case State#state.logical_decoding_msg_callback of
                      undefined ->
                        ok;
                      LogicalDecodingMsgCallback ->
                        lists:foreach(fun(Msg) ->
                                        ok = LogicalDecodingMsgCallback:handle_logical_decoding_msg(Msg)
                                      end, LogicalDecodingMsgs)
                    end,
                    {reply, ok, State#state{logical_decoding_msgs = [], rows = [], last_processed_lsn = NewLastEndLSN}}
            end;
        skip ->
            {reply, ok, State#state{rows = []}}
    end;

handle_call({pglogical_msg, _StartLSN, _EndLSN, #logical_decoding_msg{} = Msg}, _From, State) ->
    #state{logical_decoding_msgs = Msgs} = State,
    {reply, ok, State#state{logical_decoding_msgs = [Msg | Msgs]}};

handle_call({pglogical_msg, _StartLSN, _EndLSN, #relation_msg{name = TableName, id = Relidentifier,
    columns = Columns, namespace = SchemaName}}, _From,
    State = #state{metadata = Metadata, conn_normal = ConnNormal, options = Options}) ->
    FullTableName = binary_to_list(iolist_to_binary([SchemaName, ".", TableName])),
    case get_option(reload_columns_on_metadata_msg, Options) of
        true when FullTableName =/= ?PGLOGICAL_QUEUE ->
            {reply, ok, State#state{metadata = Metadata#{Relidentifier => {FullTableName, Columns,
                get_column_types(ConnNormal, FullTableName)}}}};
        _ -> {reply, ok, State#state{metadata = Metadata#{Relidentifier => {FullTableName, Columns}}}}
    end;

handle_call({pglogical_msg, _StartLSN, _EndLSN, #row_msg{relation_id = Relidentifier} = Row}, _From,
    State = #state{metadata = Metadata, rows = Rows}) ->
    case element(1, maps:get(Relidentifier, Metadata)) of
        ?PGLOGICAL_QUEUE ->
            %% Pglogical queue message: resynchronize_table, add new table to set, sequence replication.
            %% We do not support it yet.
            {reply, ok, State};
        _TableName ->
            {reply, ok, State#state{rows = [Row | Rows]}} %% process later all together in commit msg
    end;

handle_call(get_last_lsn, _From, State = #state{last_processed_lsn = LastLsn}) ->
    {reply, {ok, LastLsn}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{conn = Conn}) when Pid == Conn ->
    error_logger:error_msg("Epgsql has been stopped with reason ~p~n", [Reason]),
    self() ! {reconnect, 0},
    {noreply, State#state{conn = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #state{conn_normal = Conn}) when Pid == Conn ->
    error_logger:error_msg("Epgsql has been stopped with reason ~p~n", [Reason]),
    self() ! {reconnect_normal, 0},
    {noreply, State#state{conn_normal = undefined}};

handle_info({reconnect, ReconnectCount},
    State = #state{conn = undefined, max_reconnect_attempts = MaxReconnectAttempts})
    when is_integer(MaxReconnectAttempts) andalso ReconnectCount >= MaxReconnectAttempts ->
    error_logger:error_msg("Maximum reconnection attempts threshold (~p) is reached~n", [ReconnectCount]),
    {stop, max_reconnect_attempts, State};

handle_info({reconnect, ReconnectCount},
    State = #state{conn = undefined, reconnect_interval = ReconnectInterval,
        db_connect_opts = Opts, replication_slot = ReplSlot, replication_set = ReplSet,
        pglogical_config = PglogicalConfig, output_plugin = OutputPlugin, align_lsn = AlignLSN}) ->
    error_logger:warning_msg("Trying to reconnect to Postgresql~n"),
    case connect(Opts, replication) of
        {ok, Conn} ->
            ok = start_replication(OutputPlugin, Conn, ReplSlot, ReplSet, PglogicalConfig, AlignLSN),
            error_logger:warning_msg("Reconnected to Postgresql~n"),
            {noreply, State#state{conn = Conn, rows = []}};
        {error, Reason} ->
            error_logger:error_msg("Cannot connect to Postgresql, reason - ~p~n", [Reason]),
            timer:send_after((ReconnectInterval * 1000), {reconnect, ReconnectCount + 1}),
            {noreply, State}
    end;

handle_info({reconnect_normal, ReconnectCount},
    State = #state{conn_normal = undefined, max_reconnect_attempts = MaxReconnectAttempts})
    when is_integer(MaxReconnectAttempts) andalso ReconnectCount >= MaxReconnectAttempts ->
    error_logger:error_msg("Maximum reconnection attempts threshold (~p) is reached~n", [ReconnectCount]),
    {stop, max_reconnect_attempts, State};

handle_info({reconnect_normal, ReconnectCount},
    State = #state{conn_normal = undefined, reconnect_interval = ReconnectInterval, db_connect_opts = Opts}) ->
    error_logger:warning_msg("Trying to reconnect to Postgresql~n"),
    case connect(Opts, normal) of
        {ok, Conn} ->
            error_logger:warning_msg("Reconnected to Postgresql~n"),
            {noreply, State#state{conn_normal = Conn}};
        {error, Reason} ->
            error_logger:error_msg("Cannot connect to Postgresql, reason - ~p~n", [Reason]),
            timer:send_after((ReconnectInterval * 1000), {reconnect_normal, ReconnectCount + 1}),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn = Conn, conn_normal = ConnNormal}) ->
    close_connection(Conn),
    close_connection(ConnNormal).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_x_log_data(_StartLSN, EndLSN, <<>>, CbState) ->
    {ok, EndLSN, EndLSN, CbState};

handle_x_log_data(StartLSN, EndLSN, Data,
    CbState = #cb_state{pid = SubscriberPid, debug = DebugMode, decoder_module = DecoderModule}) ->
    {ok, Message} = DecoderModule:decode(Data),
    case DebugMode of
        true -> error_logger:info_msg("handle_x_log_data StartLSN = ~p, EndLSN = ~p, Message = ~p~n",
            [StartLSN, EndLSN, Message]);
        false -> ok
    end,
    ok = gen_server:call(SubscriberPid, {pglogical_msg, StartLSN, EndLSN, Message}),
    {ok, EndLSN, EndLSN, CbState}.

start_replication(pgoutput, Conn, ReplicationSlot, PublicationNames, _ExtraConfig, AlignLSN) ->
    DebugMode = application:get_env(?APP, debug, false),
    epgsql:squery(Conn, "IDENTIFY_SYSTEM"),
    epgsql:start_replication(Conn,
        ReplicationSlot, ?MODULE,
        #cb_state{pid = self(), debug = DebugMode, decoder_module = epgl_pgoutput_decoder},
        "0/0",
        "messages 'true', proto_version '1', publication_names '\"" ++ PublicationNames ++ "\"'",
        [{align_lsn, AlignLSN}]);
start_replication(pglogical, Conn, ReplicationSlot, ReplicationSets, PglogicalConfig, AlignLSN) ->
    DebugMode = application:get_env(?APP, debug, false),
    BinaryConfig = proplists:get_value(binary, PglogicalConfig, []),
    ExpectedEncoding =
        case proplists:get_value(expected_encoding, PglogicalConfig, undefined) of
            undefined ->
                case epgsql:get_parameter(Conn, "server_encoding") of
                    {ok,undefined} -> "UTF8";
                    {ok, V} -> binary_to_list(V)
                end;
            V -> V
        end,

    epgsql:squery(Conn, "IDENTIFY_SYSTEM"),
    epgsql:start_replication(Conn,
        ReplicationSlot, ?MODULE,
        #cb_state{pid = self(), debug = DebugMode, decoder_module = epgl_pglogical_decoder},
        "0/0",
        "startup_params_format '1',
        min_proto_version '1',
        max_proto_version '1',
        \"hooks.setup_function\" 'pglogical.pglogical_hooks_setup',
        expected_encoding '" ++ ExpectedEncoding ++ "'," ++
            prepare_pglogical_param(pg_version, PglogicalConfig, "pg_version") ++
            prepare_pglogical_param(basetypes_major_version, BinaryConfig, "binary.basetypes_major_version") ++
            prepare_pglogical_param(bigendian, BinaryConfig, "binary.bigendian") ++
            prepare_pglogical_param(sizeof_datum, BinaryConfig, "binary.sizeof_datum") ++
            prepare_pglogical_param(sizeof_int, BinaryConfig, "binary.sizeof_int") ++
            prepare_pglogical_param(sizeof_long, BinaryConfig, "binary.sizeof_long") ++
            prepare_pglogical_param(float4_byval, BinaryConfig, "binary.float4_byval") ++
            prepare_pglogical_param(float8_byval, BinaryConfig, "binary.float8_byval") ++
            prepare_pglogical_param(integer_datetimes, BinaryConfig, "binary.integer_datetimes") ++
            prepare_pglogical_param(want_internal_basetypes, BinaryConfig, "binary.want_internal_basetypes") ++
            prepare_pglogical_param(want_binary_basetypes, BinaryConfig, "binary.want_binary_basetypes") ++
            "\"pglogical.replication_set_names\" '" ++ ReplicationSets ++ "'",
        [{align_lsn, AlignLSN}]).

prepare_pglogical_param(Key, Config, ParamName) ->
    case proplists:get_value(Key, Config, undefined) of
        undefined -> "";
        Val when is_binary(Val) -> "\"" ++ ParamName ++ "\" '" ++ binary_to_list(Val) ++ "',";
        Val when is_integer(Val) -> "\"" ++ ParamName ++ "\" '" ++ integer_to_list(Val) ++ "',";
        Val -> "\"" ++ ParamName ++ "\" '" ++ Val ++ "',"
    end.

set_binarymode_pglogical_params(Conn, PglogicalConfig) ->
    PglogicalConfig2 =
        case proplists:get_value(pg_version, PglogicalConfig, undefined) of
            undefined ->
                {ok, _, [{Version}]} = epgsql:equery(Conn, ["select current_setting('server_version_num')::int"]),
                [{pg_version, Version} | PglogicalConfig];
            _ -> PglogicalConfig
        end,

    BinaryConfig = proplists:get_value(binary, PglogicalConfig, []),
    BinaryConfig2 =
        case proplists:get_value(integer_datetimes, BinaryConfig, undefined) of
            undefined ->
                IntegerDatetimes =
                    case epgsql:get_parameter(Conn, "integer_datetimes") of
                        {ok, <<"on">>} ->
                            put(datetime_mod, epgsql_idatetime), %% we need to set this value for epgsql_binary decoding
                            1;
                        {ok, <<"off">>} ->
                            put(datetime_mod, epgsql_fdatetime), %% we need to set this value for epgsql_binary decoding
                            0
                    end,
                [{integer_datetimes, IntegerDatetimes} | BinaryConfig];
            1 ->
                put(datetime_mod, epgsql_idatetime), %% we need to set this value for epgsql_binary decoding
                BinaryConfig;
            0 ->
                put(datetime_mod, epgsql_fdatetime), %% we need to set this value for epgsql_binary decoding
                BinaryConfig
        end,

    BinaryConfig3 =
        case proplists:get_value(basetypes_major_version, BinaryConfig, undefined) of
            undefined ->
                Value = proplists:get_value(pg_version, PglogicalConfig2) div 100,
                [{basetypes_major_version, Value} | BinaryConfig2];
            _ -> BinaryConfig2
        end,

    BinaryConfig4 = [{want_binary_basetypes, 1}, {want_internal_basetypes, 1} |BinaryConfig3],
    lists:keyreplace(binary, 1, PglogicalConfig2, {binary, BinaryConfig4}).

connect(DBArgs, Mode) ->
    Host = proplists:get_value(hostname, DBArgs),
    DBName = proplists:get_value(database, DBArgs),
    User = proplists:get_value(username, DBArgs),
    Password = proplists:get_value(password, DBArgs),
    Port     = proplists:get_value(port, DBArgs, 5432),

    epgsql:connect(
        Host, User, Password,
        [{database, DBName}, {port, Port}
            | case Mode of replication -> [{replication, "database"}]; _ -> [] end]).

process_transaction_row(#row_msg{relation_id = Relidentifier, old_columns = OldColumns, tuple_type = TupleType} = Row,
    Metadata, Callbacks, CallbackData, ColumnsDT, AutoCast, TwoMsgsForPKUpdate) ->
    {TableName, Columns, TableColumnsDT} =
        case maps:get(Relidentifier, Metadata) of
            {TableName1, Columns1} -> {TableName1, Columns1, maps:get(TableName1, ColumnsDT, [])};
            {TableName1, Columns1, TableColumnsDT1} -> {TableName1, Columns1, TableColumnsDT1}
        end,

    case maps:get(TableName, Callbacks, []) of
        [] ->
            CallbackData;
        CbModules ->
            RowData =
                case TwoMsgsForPKUpdate andalso TupleType =/= new andalso OldColumns =/= undefined of
                    true ->
                        [convert_row(Row#row_msg{columns = OldColumns, msg_type = delete}, TableName, AutoCast, TableColumnsDT),
                            convert_row(Row#row_msg{msg_type = insert}, TableName, AutoCast, TableColumnsDT)];
                    false ->
                        convert_row(Row, TableName, AutoCast, TableColumnsDT)
                end,
            ColumnsNames = [ColumnName || #relation_column{name = ColumnName} <- Columns],
            add_callback_data(CbModules, CallbackData, RowData, TableName, ColumnsNames)
    end.

convert_row(#row_msg{msg_type = MsgType, columns = Columns}, TableName, AutoCast, TableColumnsDT) ->
    #row{
        table_name = TableName,
        change_type = MsgType,
        fields =
        case AutoCast of
            false ->
                [Value || #column_value{value = Value} <- Columns];
            true ->
                [epgl_cast:cast(Value) || Value <- lists:zip(TableColumnsDT, Columns)]
        end
    }.

check_lsn(CheckLSNMode, LastEndLSN, EndLSN, Rows) when LastEndLSN >= EndLSN andalso CheckLSNMode == skip ->
    error_logger:error_msg("Message with LSN ~p already processed. Last Processed LSN ~p. Messages ~p~n",
        [EndLSN, LastEndLSN, Rows]),
    skip;
check_lsn(CheckLSNMode, LastEndLSN, EndLSN, Rows) when LastEndLSN >= EndLSN andalso CheckLSNMode == log ->
    error_logger:error_msg("Message with LSN ~p already processed. Last Processed LSN ~p. Messages ~p~n",
        [EndLSN, LastEndLSN, Rows]),
    ok;
check_lsn(_CheckLSNMode, _LastEndLSN, _EndLSN, _Rows) ->
    ok.

close_connection(undefined) -> ok;
close_connection(Conn) -> epgsql:close(Conn).

get_column_types(Conn, Table) ->
    {ok, Columns, _Rows} = epgsql:squery(Conn, ["SELECT * FROM ", Table, " LIMIT 1"]),
    [element(3, Column) || Column <- Columns].

add_callback_data([], CallbackData, _RowData, _TableName, _ColumnsNames) -> CallbackData;
add_callback_data([Callback | Rest], CallbackData, RowData, TableName, ColumnsNames) ->
    {MetadataAcc, RowAcc} = maps:get(Callback, CallbackData, {#{}, []}),
    add_callback_data(Rest,
        CallbackData#{Callback => {MetadataAcc#{TableName => ColumnsNames}, [RowData | RowAcc]}},
        RowData, TableName, ColumnsNames).


get_option(auto_cast, Options) -> maps:get(auto_cast, Options, true);
get_option(binary_mode, Options) -> maps:get(binary_mode, Options, false);
get_option(reload_columns_on_metadata_msg, Options) -> maps:get(reload_columns_on_metadata_msg, Options, false).
