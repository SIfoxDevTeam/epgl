-module(kv_cache_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Child1 = #{id => kv_cache_server, start => {kv_cache_server, start_link, []}},

    {ok, { {one_for_all, 0, 1}, [Child1]} }.
