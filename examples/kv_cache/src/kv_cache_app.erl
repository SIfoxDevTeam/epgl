-module(kv_cache_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    kv_cache_sup:start_link().

stop(_State) ->
    ok.
