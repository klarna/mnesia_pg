-module(mnesia_pg_util).

-export([create/0,
	 pg_dir/1,
	 pg_bin/1,
	 stop/0, stop/2,
	 get_saved_port/0]).
-export([test/0]).
-export([ttest/0]).

-include("mnesia_pg_int.hrl").

pg_dir(#conf{dir = Dir}) ->
    Dir.

pg_bin(#conf{bin = Bin}) ->
    Bin.


ttest() ->
    dbg:tracer(),
    dbg:tpl(?MODULE, x),
    dbg:p(all,[c]),
    test().

test() ->
    mnesia:stop(),
    %% stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()], [{backend_types, [{pg, mnesia_pg}]}]),
    create().

create() ->
    C = init_env(),
    ensure_running(C).

ensure_running(#conf{status = not_installed} = C) ->
    start_db(init_db(C));
ensure_running(#conf{status = not_running} = C) ->
    start_db(C);
ensure_running(#conf{status = running} = C) ->
    C.

start_monitor(#conf{bin = PgBin, dir = PgDir} = C) ->
    mnesia_pgsql_mon:start(PgBin, PgDir),
    C.

init_db(#conf{status = not_installed,
	      bin = PgBin, dir = PgDir} = C) ->
    cmd([filename:join(PgBin,"initdb"), " -D ", PgDir]),
    C#conf{status = not_running}.

start_db(#conf{status = not_running,
	       bin = PgBin, port = Port, dir = PgDir, user = User} = C) ->
    PortStr = integer_to_list(Port),
    PgLog = filename:join(mnesia_monitor:get_env(dir), "pglog"),
    Opts = "-i -h localhost -p " ++ PortStr,
    cmd([filename:join(PgBin,"pg_ctl"), " -D ", PgDir, " -l ", PgLog,
	 " -w -t 10 -o \"", Opts, "\" start"]),
    cmd([filename:join(PgBin,"createuser"), " -h localhost -p ", PortStr,
	 " ", User]),
    cmd([filename:join(PgBin,"createdb"), " -h localhost -p ", PortStr,
	 " --owner=", User, " erldb"]),
    save_port(Port),
    C#conf{status = running}.

stop() ->
    Dir = mnesia_monitor:get_env(dir),
    PgDir = filename:join(Dir, "pgdata"),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"),
    stop(PgBin, PgDir).

stop(PgBin, PgDir) ->
    cmd([filename:join(PgBin,"pg_ctl"), " -D ", PgDir, " stop"]).

cmd(Cmd0) ->
    Cmd = lists:flatten(Cmd0),
    L = erlang:min(length(Cmd), 50),
    Res = os:cmd(Cmd),
    io:fwrite("~s~n" ++ lists:duplicate(L, $-)
	      ++ "~n~s~n", [Cmd, Res]),
    case Res of
	"/bin/sh:" ++ _ ->
	    error(script_error);
	_ ->
	    ok
    end.

init_env() ->
    C0 = #conf{},
    application:load(mnesia_pg),
    User = get_env(pg_user, C0#conf.user),
    Db = get_env(pg_db, C0#conf.db),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"),
    Dir = mnesia_monitor:get_env(dir),
    PgDir = filename:join(Dir, "pgdata"),
    C = #conf{bin  = PgBin,
	      dir  = PgDir,
	      db   = Db,
	      user = User},
    case filelib:is_dir(PgDir) of
	false -> C#conf{status = not_installed,
			port = get_port()};
	true  -> pg_status(C)
    end.

pg_status(#conf{bin = Bin, dir = Dir} = C) ->
    Ctl = filename:join(Bin, "pg_ctl"),
    Cmd = [Ctl, " -D ", Dir, " status"],
    if_not_running(parse_status(os:cmd(Cmd), C)).

parse_status("pg_ctl: no server" ++ _, C) ->
    C#conf{status = not_running};
parse_status("pg_ctl: server is running" ++ Rest, C) ->
    Pid = re:run(Rest, "\\(PID: ([0-9]+)\\)", [{capture,[1], list}]),
    io:fwrite("postgres running at (PID: ~p)~n", [Pid]),
    [_,Cmd|_] = re:split(Rest, "\\n", [{return,list}]),
    CmdS = re:replace(Cmd, "\\\"", "", [global]),
    case re:run(CmdS, "-i", []) of
	{match,_} ->
	    %% TCP enabled
	    H = match1(re:run(CmdS, "-h[\\h]+([^\\h]+)", [{capture,[1],list}])),
	    P = match1(re:run(CmdS, "-p[\\h]+([^\\h]+)", [{capture,[1],list}])),
	    C#conf{status = running,
		   host = H,
		   port = list_to_integer(P)};
	nomatch ->
	    error(inet_not_enabled)
    end.

if_not_running(#conf{status = not_running} = C) ->
    P = get_port(),
    H = get_env(pg_host, "localhost"),
    C#conf{host = H,
	   port = P};
if_not_running(C) ->
    C.

match1({match, [Res]}) -> Res;
match1(nomatch) -> undefined.

get_env(K, Default) ->
    case application:get_env(mnesia_pg, K) of
	{ok, Val} ->
	    Val;
	_ ->
	    if is_function(Default, 0) -> Default();
	       true -> Default
	    end
    end.

get_port() ->
    get_env(pg_port, fun any_port/0).

%% This doesn't guarantee that the port will be available, but the OS seems
%% reluctant to reuse port numbers right away (for good reason).
any_port() ->
    {ok, S} = gen_tcp:listen(0, []),
    {ok, P} = inet:port(S),
    gen_tcp:close(S),
    application:set_env(mnesia_pg, pg_port, P),
    P.

save_port(Port) ->
    Dir = mnesia_monitor:get_env(dir),
    Bin = term_to_binary([{port, Port}]),
    file:write_file(filename:join(Dir, "port_info.pg"), Bin).

get_saved_port() ->
    Dir = mnesia_monitor:get_env(dir),
    case file:read_file(filename:join(Dir, "port_info.pg")) of
	{ok, Bin} ->
	    proplists:get_value(port, binary_to_term(Bin));
	_ ->
	    undefined
    end.
