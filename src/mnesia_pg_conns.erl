%%%
%%% Copyright (c) 2014-2015 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.

-module(mnesia_pg_conns).

-behaviour(gen_server).
-export([start_link/0]).
-export([alloc/1, free/1, ref/0, state/0]).
-export([check_schema_cookie/0]).
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, code_change/3, terminate/2]).

-include("mnesia_pg_int.hrl").

start_link() ->
    Conf = mnesia_pgsql_mon:get_conf(),
    gen_server:start_link({local, conn_pool}, ?MODULE, Conf, []).

open_connections(#conf{pool_size = N,
		       host = Host,
		       port = Port,
		       user = User,
		       password = Pwd,
		       db = Db}) ->
    L = lists:seq(1, N),			% N connections
    ConnL = lists:map(
	      fun(_) ->
		      {ok, C} = pgsql:connect(Host, User, Pwd,
					      [{database, Db},
					       {port, Port}]),
		      C
	      end, L),
    ConnL.

alloc(Tab) ->
    C = gen_server:call(conn_pool, {alloc, self()}),
    case (C) of
	wait ->
	    receive
		{new_conn, C2} ->
		    C2
	    after 1000 ->
		    io:fwrite("ERR: failed to allocate PG connection for table ~p~n", [Tab]),
		    void			% mnesia abort?
	    end;
	_ ->
	    C
    end.

free(Conn) ->
    gen_server:cast(conn_pool, {free, Conn}).

ref() ->
    gen_server:call(conn_pool, get_ref).

state() ->
    gen_server:call(conn_pool, get_state).

check_schema_cookie() ->
    gen_server:call(conn_pool, check_schema_cookie).

init(Conf) ->
    ConnL = open_connections(Conf),
    io:fwrite("Connection opened~n", []),
    %% check_schema_cookie(ConnL),
    process_flag(trap_exit, true),
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    {ok, {ConnL, []}}.

terminate(_, {ConnL, _}) ->
    lists:foreach(fun(C) ->
			  try pgsql:close(C)
			  catch
			      _:_ -> ok
			  end
		  end, ConnL).

handle_call(get_ref, _From, State) ->
    {reply, random:uniform(100000000), State};
handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call({alloc, _Pid}, _From, {[Conn|ConnL], WaitL}) ->
    {reply, Conn, {ConnL, WaitL}};
handle_call({alloc, Pid}, _From, {[], WaitL}) ->
    {reply, wait, {[], lists:append(WaitL,[Pid])}};
handle_call(check_schema_cookie, _From, {ConnL, _} = State) ->
    Result = check_schema_cookie(ConnL),
    {reply, Result, State}.

handle_cast({free, Conn}, {ConnL, []}) ->
    {noreply, {[Conn|ConnL], []}};
handle_cast({free, Conn}, {ConnL, [Pid|WaitL]}) ->
    Pid ! {new_conn, Conn},
    {noreply, {ConnL, WaitL}}.

handle_info(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal

check_schema_cookie([H|_]) ->
    sql_transaction(H, fun() -> do_check_schema_cookie(H) end).

do_check_schema_cookie(H) ->
    MyCookie = mnesia_lib:val({schema, cookie}),
    SQL = "select erlval from schema where erlkey='cookie'",
    Res = pgsql:equery(H, SQL, []),
    io:fwrite("schema query: ~p~n", [Res]),
    case Res of
	{error,{error,error,<<"42P01">>,_,_}} ->
	    %% schema doesn't exist
	    io:fwrite("schema doesn't exist~n", []),
	    CreateRes = sql_create_schema(H),
	    io:fwrite("CreateRes = ~p~n", [CreateRes]),
	    insert_cookie(H, MyCookie);
	{ok, _, []} ->
	    %% No cookie
	    io:fwrite("No cookie~n", []),
	    insert_cookie(H, MyCookie),
	    ok;
	{ok, [{column,<<"erlval">>,bytea,_,_,_}],[{Bin}]} = Res ->
	    io:fwrite("Res = ~p~n", [Res]),
	    try binary_to_term(Bin) of
		MyCookie ->
		    io:fwrite("Cookies match!~n", []),
		    ok;
		WrongCookie ->
		    {error,
		     {schema_cookie_mismatch,{WrongCookie,MyCookie}}}
	    catch
		error:_ ->
		    error(cannot_decode_cookie)
	    end
    end.

insert_cookie(C, Cookie) ->
    Bin = term_to_binary(Cookie),
    InsertRes =
	pgsql:equery(C, ("insert into schema (erlkey, erlval) values"
			 " ('cookie', $1)"), [Bin]),
    io:fwrite("InsertRes = ~p~n", [InsertRes]),
    File = filename:join(mnesia_monitor:get_env(dir), "cookie.pg"),
    file:write_file(File, Bin),
    ok.
    


sql_transaction(_C, F) ->
    %% pgsql:squery(C, "begin"),
    try  F()
	 %% pgsql:squery(C, "commit")
    catch
	error:E ->
	    %% pgsql:squery(C, "rollback"),
	    error(E);
	exit:R ->
	    %% pgsql:squery(C, "rollback"),
	    exit(R);
	throw:T ->
	    %% pgsql:squery(C, "rollback"),
	    throw(T)
    end.

	    

sql_create_schema(H) ->
    pgsql:squery(H, ("create table schema"
		     " (erlkey character varying(64), erlval bytea)")).
