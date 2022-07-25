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

-module(epgl).

%% API
-export([start_subscriber/3]).
-export([start_subscriber/4]).
-export([stop/1]).
-export([drop_replication_slot/2]).
-export([create_replication_slot/2, create_replication_slot/3]).
-export([init_replication_set/3, init_replication_set/4]).
-export([start_replication/3]).
-export([get_table_initial_state/3]).
-export([get_last_lsn/1]).
-export([format_lsn/1]).

-type connect_option() ::
    {database, DBName     :: string()}             |
    {port,     PortNum    :: inet:port_number()}   |
    {hostname, Hostname   :: inet:ip_address() | inet:hostname()} |
    {username, Username   :: string()} |
    {password, Password   :: string()}.

-type db_args() ::  [connect_option()].

-export_type([connect_option/0, db_args/0]).

-spec start_subscriber(
    SubscriberId :: atom(),
    DBArgs :: connect_option(),
    Callbacks :: #{TableName :: string() => [module()]})
        -> {ok, Connection :: pid()} | {error, Reason :: term()}.
%% @doc starts epgl_subscriber server and connects to Postgres in replication mode
%% where
%% 'SubscriberId' - Atom to identify the subscriber process
%% `DBArgs'     - proplist of db connection parameters
%% `Callbacks'  - callbacks for tables. TableName should have format "schemaName.tableName"
%%              For example: #{"public.table1" => [CallbackModule1, CallbackModule2], "public.table2" => [CallbackModule2, CallbackModule3]}
%% 'Options'
%%     - auto_cast - true or false.
%%       If you use textual output format you can set this option to true to cast some of the values (integers, boolean) to erlang representation;
%%     - binary_mode - true or false. Only for pglogical plugin.
%%        Use binary output format. You should also check/set epgl.pglogical_config.binary parameters.
%%     - reload_columns_on_metadata_msg - true or false.
%%          If auto_cast = true or in case of binary output format EPGL loads columns datatypes to convert/cast values correctly.
%%          If this options is false EPGL will load columns datatypes only once during subscriber start.
%%          In this case you should restart EPGL if table definition changes in order to reload columns datatypes.
%%          If this options is true EPGL will re-load columns datatypes when receive metadata_msg from pglogical,
%%          so no need to restart EPGL even if table definition changes (e.g. new column is added)
%%
%% returns `{ok, Pid}' otherwise `{error, Reason}'
start_subscriber(SubscriberId, DBArgs, Callbacks) ->
    start_subscriber(SubscriberId, DBArgs, Callbacks, #{}).

start_subscriber(SubscriberId, DBArgs, Callbacks, Options) ->
    epgl_subscriber:start_link(SubscriberId, DBArgs, Callbacks, Options).

-spec stop(Connection :: pid()) -> ok.
stop(C) ->
    epgl_subscriber:stop(C).

-spec drop_replication_slot(Connection :: pid(), ReplicationSlot :: string()) ->
        ok | {error, Reason :: term()}.
drop_replication_slot(Connection, ReplicationSlot) ->
    gen_server:call(Connection, {drop_replication_slot, ReplicationSlot}, infinity).

-spec create_replication_slot(Connection :: pid(), ReplicationSlot :: string()) ->
        {ok, SnapshotName :: string(), ConsistentPoint :: string()} | {error, Reason :: term()}.
create_replication_slot(Connection, ReplicationSlot) ->
    create_replication_slot(Connection, ReplicationSlot, #{}).

%% 'Options'
%%     - temporary - true or false.
-spec create_replication_slot(Connection :: pid(), ReplicationSlot :: string(), Options :: map()) ->
    {ok, SnapshotName :: string(), ConsistentPoint :: string()} | {error, Reason :: term()}.
create_replication_slot(Connection, ReplicationSlot, Options) ->
    gen_server:call(Connection, {create_replication_slot, ReplicationSlot, Options}, infinity).

-spec get_table_initial_state(Connection :: pid(), TableName :: string(), SnapshotName :: string()) ->
    {ok, Columns :: list(), Values :: list()} | {error, Reason :: term()}.
get_table_initial_state(Connection, TableName, SnapshotName) ->
    gen_server:call(Connection, {get_table_initial_state, TableName, SnapshotName}, infinity).

%% if pgoutput plugin is used (epgl.repl_slot_output_plugin = pgoutput) then ReplicationSets should contain publication name
-spec init_replication_set(
    Connection :: pid(),
    ReplicationSets :: string(),
    SnapshotName :: string(),
    TablesOrder :: #{TableName :: binary() => Ord :: integer()} | undefined) ->
    ok | {error, Reason :: term()}.
init_replication_set(Connection, ReplicationSets, SnapshotName, TablesOrder) ->
    gen_server:call(Connection, {init_replication_set, ReplicationSets, SnapshotName, TablesOrder}, infinity).

init_replication_set(Connection, ReplicationSets, SnapshotName) ->
    init_replication_set(Connection, ReplicationSets, SnapshotName, undefined).


%% if pgoutput plugin is used (epgl.repl_slot_output_plugin = pgoutput) then ReplicationSets should contain publication name
-spec start_replication(Connection :: pid(),
    ReplicationSlot :: string(),
    ReplicationSets :: string()) ->
    ok | {error, Reason :: term()}.
start_replication(Connection, ReplicationSlot, ReplicationSets) ->
    gen_server:call(Connection, {start_replication, ReplicationSlot, ReplicationSets}, infinity).

-spec get_last_lsn(Connection :: pid()) -> Lsn :: integer().
get_last_lsn(SubscriberId) ->
    {ok, Lsn} = gen_server:call(SubscriberId, get_last_lsn),
    Lsn.

%% @doc Formats 64bit LSN into the XXX/XXX format
-spec format_lsn(Lsn :: integer()) -> string().
format_lsn(Lsn) ->
    UHalf = Lsn div 4294967295,
    LHalf = Lsn rem 4294967295,
    integer_to_list(UHalf, 16) ++ "/" ++ integer_to_list(LHalf, 16).
