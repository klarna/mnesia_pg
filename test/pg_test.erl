-module(pg_test).

-include_lib("stdlib/include/ms_transform.hrl").

-export([t0/0, t1/0, t2/0, t3/0, t3b/0, t4/0, t5/0, dbg/1]).

-record(x, {name, value}).

dbg(on) ->
    dbg:start(),
    dbg:tracer(),
    dbg:tpl(mnesia, '_', []),
    %dbg:tpl(mnesia_locker, '_', []),
    dbg:tpl(mnesia_lib, '_', []),
    dbg:tpl(mnesia_pg, '_', []),
    dbg:tpl(pgsql, '_', []),
    dbg:p(all,c);
dbg(off) ->
    dbg:stop_clear().

t0() ->
    setup_mnesia(),
    {atomic,ok} = mnesia:create_table(pg0, [{pg_copies, [node()]},
					    {attributes, record_info(fields, x)},
					    {record_name, x}]),
    mnesia:wait_for_tables([pg0], 30000).

t1() ->
    Fun = fun() ->
		  mnesia:write(pg0, {x, allan, 123}, write),
		  mnesia:read(pg0, allan)
	  end,
    mnesia:transaction(Fun).

t2() ->
    mnesia:dirty_read(pg0, allan).

t3() ->
    MS = ets:fun2ms(fun(X) -> X end),		%select *
    F = fun() -> mnesia:select(pg0,MS) end,
    mnesia:transaction(F).

t3b() ->
    MS = ets:fun2ms(fun(#x{name=X}) -> X end),
    F = fun() -> mnesia:select(pg0,MS) end,
    mnesia:transaction(F).

t4() ->
    F = fun() -> mnesia:match_object(pg0, {x,'_','_'}, read) end,
    mnesia:transaction(F).

t5() ->
    Fun = fun() ->
		  mnesia:delete(pg0, allan, write)
	  end,
    mnesia:transaction(Fun).

setup_mnesia() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()], [{backend_types,
					  [{pg_copies,
					    mnesia_pg}]}]),
    ok = mnesia:start().
