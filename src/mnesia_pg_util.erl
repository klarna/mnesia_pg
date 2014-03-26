-module(mnesia_pg_util).

-export([create/0, stop/0]).
-export([test/0]).

test() ->
    mnesia:stop(),
    stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()], [{backend_types, [{pg, mnesia_pg}]}]),
    create().

create() ->
    User = get_env(pg_user, "kred"),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"), 
    Dir = mnesia_monitor:get_env(dir),
    PgDir = filename:join(Dir, "pgdata"),
    PgLog = filename:join(Dir, "pglog"),
    cmd([filename:join(PgBin,"initdb"), " -D ", PgDir]),
    cmd([filename:join(PgBin,"pg_ctl"), " -D ", PgDir, " -l ", PgLog, " -w -t 10 start"]),
    cmd([filename:join(PgBin,"createuser"), " ", User]),
    cmd([filename:join(PgBin,"createdb"), " --owner=", User, " erldb"]).

stop() ->
    Dir = mnesia_monitor:get_env(dir),
    PgDir = filename:join(Dir, "pgdata"),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"), 
    cmd([filename:join(PgBin,"pg_ctl"), " -D ", PgDir, " stop"]).

cmd(Cmd0) ->
    Cmd = lists:flatten(Cmd0),
    L = erlang:min(length(Cmd), 50),
    Res = os:cmd(Cmd),
    io:fwrite("~s~n" ++ lists:duplicate(L, $-)
              ++ "~n~s~n", [Cmd, Res]).
    

get_env(K, Default) ->
    case application:get_env(K) of
        {ok, Val} ->
            Val;
        _ ->
            Default
    end.

