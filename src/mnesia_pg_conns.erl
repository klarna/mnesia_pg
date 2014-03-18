-module(mnesia_pg_conns).

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

alloc() ->
    gen_server:call(conn_pool, alloc).		% returns PG connection

free(Conn) ->
    gen_server:cast(conn_pool, {free, Conn}).

init([N, User, Pwd]) ->
    ConnL = open_connections(N, User, Pwd),
    process_flag(trap_exit, true),
    {ok, ConnL}.

terminate(_, ConnL) ->
    lists:map(fun(C) ->
		      pgsql:close(C)
	      end, ConnL).

handle_call(alloc, _From, [Conn|ConnL]) ->
    {reply, Conn, ConnL}.

handle_cast({free, Conn}, ConnL) ->
    {noreply, [Conn|ConnL]}.

handle_info(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

