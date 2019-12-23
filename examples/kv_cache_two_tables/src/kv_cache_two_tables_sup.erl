-module(kv_cache_two_tables_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    DBArgs = [{hostname, "localhost"}, {port, 10432},
        {database, "epgl_test_db"}, {username, "epgl_test"}, {password, "epgl_test"}],
    Callbacks = #{"public.test_table1" => [kv_cache_two_tables_server1],
        "public.test_table4" => [kv_cache_two_tables_server2]},
    Child1 = #{id => epgl, start => {epgl, start_subscriber, [epgl_subscriber_1, DBArgs, Callbacks]}},
    Child2 = #{id => kv_cache_two_tables_server1, start => {kv_cache_two_tables_server1, start_link, []}},
    Child3 = #{id => kv_cache_two_tables_server2, start => {kv_cache_two_tables_server2, start_link, []}},

    {ok, { {one_for_all, 0, 1}, [Child1, Child2, Child3]} }.
