-module(mnesia_pg_util).

-export([init_env/0,
	 create/0,
	 pg_dir/1,
	 pg_bin/1,
	 get_env/2,
	 stop/0, stop/2,
	 get_saved_port/0]).
-export([clear_psql_db/0]).
-export([test/0]).
-export([ttest/0]).

-include("mnesia_pg_int.hrl").

init_env() ->
    C0 = #conf{},
    application:load(mnesia_pg),
    User = get_env(pg_user, C0#conf.user),
    Pwd = get_env(pg_pwd, C0#conf.password),
    Db = get_env(pg_db, C0#conf.db),
    Pool = get_env(pool_size, C0#conf.pool_size),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"),
    Dir = mnesia_monitor:get_env(dir),
    PgDir = get_env(pg_dir, filename:join(Dir, "pgdata")),
    C = #conf{bin  = PgBin,
	      dir  = PgDir,
	      db   = Db,
	      user = User,
	      password = Pwd,
	      pool_size = Pool},
    case filelib:is_dir(PgDir) of
	false -> C#conf{status = not_installed,
			port = get_port()};
	true  -> pg_status(C)
    end.

clear_psql_db() ->
    case mnesia_lib:is_running() of
	no ->
	    application:load(mnesia),
	    case init_env() of
		#conf{status = running, was_running = true} = C ->
		    psql_drop_all_tables(C),
		    ok;
		_ ->
		    {error, postgres_not_running}
	    end;
	_ ->
	    {error, mnesia_is_running}
    end.

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
    io:fwrite("init_env() -> ~p~n", [C]),
    ensure_running(C).

ensure_running(#conf{status = not_installed} = C) ->
    start_db(init_db(C));
ensure_running(#conf{status = not_running} = C) ->
    start_db(C);
ensure_running(#conf{status = running} = C) ->
    C.

init_db(#conf{status = not_installed,
	      bin = PgBin, dir = PgDir} = C) ->
    cmd([filename:join(PgBin,"initdb"), " -D ", PgDir]),
    C#conf{status = not_running}.

start_db(#conf{status = not_running, dir = PgDir,
	       port = Port, user = User, db = Db} = C) ->
    PortStr = integer_to_list(Port),
    PgLog = filename:join(mnesia_monitor:get_env(dir), "pglog"),
    Opts = "-i -h localhost -p " ++ PortStr,
    cmd([c("pg_ctl", C), "start -D ", PgDir, " -l ", PgLog,
	 " -w -t 10 -o \"", Opts, "\""]),
    CreateUser =
	cmd([c("createuser",C), " -h localhost -p ", PortStr,
	     " ", User]),
    cmd([c("createdb",C), " -h localhost -p ", PortStr,
	 " --owner=", User, " ", Db]),
    modify_user(CreateUser, C),
    save_port(Port),
    C#conf{status = running, was_running = false}.

modify_user(_, #conf{password = ""}) ->
    ok;
modify_user(Res, #conf{user = User, password = Pwd} = C) ->
    case Res of
	"createuser: " ++ _ = Err ->
	    case re:run(Err, "already exists", []) of
		{match, _} -> ok;
		_ -> error({unknown_error, Err})
	    end;
	_ -> ok
    end,
    psql(["alter role ", User, " with password '", Pwd, "';"], C).


%% psql(SQL) ->
%%     Conf = mnesia_pgsql_mon:get_conf(),
%%     psql(SQL, Conf).

psql(SQL, #conf{bin = PgBin, db = Db, host = Host, port = Port}) ->
    PortStr = integer_to_list(Port),
    cmd([filename:join(PgBin, "psql"), " -h ", Host, " -p ", PortStr,
	 " -d ", Db, " -c \"", SQL, "\""]).

psql_drop_all_tables(C) ->
    Tabs = psql_list_tables(C),
    io:fwrite("list_tables -> ~p~n", [Tabs]),
    [psql(["drop table if exists \"", T, "\" cascade"], C)
     || T <- Tabs].

psql_list_tables(#conf{bin = PgBin, db = Db, host = Host, port = Port}) ->
    PortStr = integer_to_list(Port),
    SQL = ("select table_name from information_schema.tables"
	   " where table_schema='public'"),
    R = cmd([filename:join(PgBin, "psql"), " -h ", Host, " -p ", PortStr,
	     " -d ", Db, " -t -c \"", SQL, "\""]),
    string:tokens(R, "\n\r\t\s").
    

c(Cmd, #conf{bin = Bin}) ->
    filename:join(Bin, Cmd).

stop() ->
    Dir = mnesia_monitor:get_env(dir),
    PgDir = filename:join(Dir, "pgdata"),
    PgBin = get_env(pg_bin, "/usr/local/pgsql/bin"),
    stop(PgBin, PgDir).

stop(PgBin, PgDir) ->
    cmd([filename:join(PgBin,"pg_ctl"), "stop -D ", PgDir]).

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
	    Res
    end.

pg_status(#conf{bin = Bin, dir = Dir} = C) ->
    Ctl = filename:join(Bin, "pg_ctl status"),
    Cmd = [Ctl, " -D ", Dir],
    if_not_running(parse_status(cmd(Cmd), C)).

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
		   port = list_to_integer(P),
		   was_running = true};
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
