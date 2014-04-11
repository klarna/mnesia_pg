-module(mnesia_pg_app).

-behaviour(application).

%% Application callbacks
-export([start/2,
	 stop/1,
	 start_phase/3]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    mnesia_pg_sup:start_link().

stop(_State) ->
    ok.

start_phase(check_schema_cookie, _, _) ->
    case mnesia_pg_conns:check_schema_cookie() of
	ok ->
	    ok;
	Error ->
	    Error
    end.
