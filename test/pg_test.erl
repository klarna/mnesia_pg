-module(pg_test).

-export([t0/0]).

t0() ->
    setup_mnesia(),
    {atomic,ok} = mnesia:create_table(pg0, [{pg_copies, [node()]},
					   {record_name, x}]),
    ok = mnesia:wait_for_tables([pg0], 30000),
    mnesia:write(pg0, {x, allan, 123}, write),
    mnesia:read(pg0, allan, read).

setup_mnesia() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()], [{backend_types,
					  [{pg_copies,
					    mnesia_pg}]}]),
    ok = mnesia:start().
