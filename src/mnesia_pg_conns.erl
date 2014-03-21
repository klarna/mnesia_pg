-module(mnesia_pg_conns).

-behaviour(gen_server).
-export([start_link/0]).
-export([alloc/1, free/1, ref/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

start_link() ->
    {ok, N} = application:get_env(pool_size),
    {ok, User} = application:get_env(pg_user),
    {ok, Pwd} = application:get_env(pg_pwd),
    gen_server:start_link({local, conn_pool}, ?MODULE, [N, User, Pwd], []).

open_connections(N, User, Pwd) ->
    L = lists:seq(1, N),			% N connections
    ConnL = lists:map(fun(_) ->
			      {ok, C} = pgsql:connect("localhost", User, Pwd, [{database,"erldb"}]),
			      C
		      end, L),
    ConnL.

alloc(Tab) ->
    C = gen_server:call(conn_pool, {alloc, self()}),
    case (C) of
	wait ->
	    receive
		{new_conn, C2} ->
		    {C2, Tab}
	    after 1000 ->
		    io:fwrite("ERR: failed to allocate PG connection for table ~p~n", [Tab]),
		    void			% mnesia abort?
	    end;
	_ ->
	    {C, Tab}
    end.

free(Conn) ->
    gen_server:cast(conn_pool, {free, Conn}).

ref() ->
    gen_server:call(conn_pool, get_ref).

init([N, User, Pwd]) ->
    ConnL = open_connections(N, User, Pwd),
    process_flag(trap_exit, true),
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    {ok, {ConnL, []}}.

terminate(_, ConnL) ->
    lists:map(fun(C) ->
		      pgsql:close(C)
	      end, ConnL).

handle_call(get_ref, _From, State) ->
    {reply, random:uniform(100000000), State};
handle_call({alloc, _Pid}, _From, {[Conn|ConnL], WaitL}) ->
    {reply, Conn, {ConnL, WaitL}};
handle_call({alloc, Pid}, _From, {[], WaitL}) ->
    {reply, wait, {[], lists:append(WaitL,[Pid])}}.

handle_cast({free, Conn}, {ConnL, []}) ->
    {noreply, {[Conn|ConnL], []}};
handle_cast({free, Conn}, {ConnL, [Pid|WaitL]}) ->
    Pid ! {new_conn, Conn},
    {noreply, {ConnL, WaitL}}.

handle_info(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

