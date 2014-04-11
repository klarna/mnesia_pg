%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-
-module(mnesia_pgsql_mon).
-behaviour(gen_server).

-export([get_conf/0]).

-export([connect/1]).

-record(st, {master,
             mref,
             pg_bin,
             pg_dir,
             port,
             conf,
             sock}).

-include("mnesia_pg_int.hrl").

-record(srv, {lsock,
              sock,
              port,
              conf}).  % server state

%% gen_server API
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

get_conf() ->
    gen_server:call(?MODULE, get_conf).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, LSock} = gen_tcp:listen(0, mon_port_opts()),
    {ok, MyPort} = inet:port(LSock),
    Me = self(),
    Conf = mnesia_pg_util:create(),
    io:fwrite("init: Conf = ~p (was_running=~p)~n", [Conf,Conf#conf.was_running]),
    case Conf#conf.was_running of
        true ->
            ok;
        false ->
            %% We started a postgres instance; monitor it!
            spawn_link(fun() -> do_accept(LSock, Me) end),
            spawn_link(fun() -> start(MyPort) end)
    end,
    spawn_link(fun monitor_mnesia/0),
    {ok, #srv{lsock = LSock,
              port = MyPort,
              conf = Conf}}.


handle_cast(_, S) ->
    {noreply, S}.

handle_info({accepted, Sock}, #srv{conf = Conf} = S) ->
    io:fwrite("got {accepted, ~p}~n", [Sock]),
    SendRes = gen_tcp:send(Sock, term_to_binary(Conf)),
    io:fwrite("SendRes = ~p~n", [SendRes]),
    {noreply, S#srv{sock = Sock}};
handle_info({tcp_closed, Sock}, #srv{sock = Sock} = S) ->
    io:fwrite("Got tcp_closed (~p)~n", [Sock]),
    {stop, remote_closed, S};
handle_info(Msg, S) ->
    io:fwrite("Got unknown Msg = ~p~n", [Msg]),
    {noreply, S}.

handle_call(get_conf, _, #srv{conf = Conf} = S) ->
    {reply, Conf, S};
handle_call(_, _, S) -> {reply, error, S}.
terminate(_, _) -> ok.
code_change(_, S, _) -> {ok, S}.

do_accept(LSock, Parent) ->
    io:fwrite("do_accept(~p - ~p)~n", [LSock, inet:port(LSock)]),
    case gen_tcp:accept(LSock, 30000) of
        {ok, Sock} ->
            io:fwrite("pgsql monitor connected!~n", []),
            ok = gen_tcp:controlling_process(Sock, Parent),
            io:fwrite("Parent (~p) now controlling~n", [Parent]),
            Parent ! {accepted, Sock};
        Error ->
            exit({accept_error, Error})
    end.

monitor_mnesia() ->
    MRef = monitor(process, mnesia_sup),
    receive
        {'DOWN', MRef, _, _, _} ->
            application:stop(mnesia_pg)
    end.

start(MyPort) ->
    Ebin = filename:dirname(code:which(mnesia_pg)),
    io:fwrite("Ebin = ~p~n", [Ebin]),
    Log = filename:join(mnesia_monitor:get_env(dir), "mon_log.pg"),
    EnsureRes = filelib:ensure_dir(filename:join(Log, "dummy")),
    io:fwrite("Ensure = ~p~n", [EnsureRes]),
    Cmd = ["run_erl /tmp/ulf/ ", Log, " \"erl -pa ", Ebin,
           " -run mnesia_pgsql_mon connect ", integer_to_list(MyPort),
           "\""],
    io:fwrite("Cmd = ~s~n", [Cmd]),
    CmdRes = os:cmd(Cmd),
    io:fwrite("CmdRes = ~p~n", [CmdRes]),
    ok.

connect([PortS]) ->
    spawn(fun() -> do_connect(list_to_integer(PortS)) end).

do_connect(Port) ->
    case gen_tcp:connect("localhost", Port, mon_port_opts()) of
        {ok, Sock} ->
            io:fwrite("Connected to master~n", []),
            receive
                {tcp, Sock, Cmd} ->
                    run(binary_to_term(Cmd), Sock);
                {tcp_closed, Sock} ->
                    error(tcp_closed);
                Other ->
                    io:fwrite("Received Other: ~p~n", [Other]),
                    error(protocol_error)
            after 10000 ->
                    error(timeout)
            end;
        Other ->
            error({connect_error, Other})
    end.


run(Conf, Sock) ->
    io:fwrite("run(~p)~n", [Conf]),
    loop(#st{conf = Conf, sock = Sock}).

loop(#st{sock = Sock, conf = Conf} = S) ->
    receive
        {tcp_closed, Sock} ->
            io:fwrite("sock (~p) closed~n", [Sock]),
            stop_pgsql(Conf),
            init:stop();
        Other ->
            io:fwrite("Received ~p - ignoring~n", [Other]),
            loop(S)
    end.

stop_pgsql(#conf{bin = PgBin, dir = PgDir}) ->
    mnesia_pg_util:stop(PgBin, PgDir).

mon_port_opts() ->
    [binary,
     {active, true},
     {delay_send, false},
     {packet, 4}].
