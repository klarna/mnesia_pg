-module(pg_perf).

-include_lib("stdlib/include/ms_transform.hrl").

-export([fill_table/2, setup/0, iterate_table/2, search_table/2, cleanup/0, test_avg/4]).

-record(x, {name, value, fat}).

write(Tab, Rec) ->
    Fun = fun() ->
		  mnesia:write(Tab, Rec, write)
	  end,
    mnesia:transaction(Fun).

read(Tab, Key) ->
    Fun = fun() ->
		  mnesia:read(Tab, Key, read)
	  end,
    mnesia:transaction(Fun).

search(Tab, MS) ->
    F = fun() -> mnesia:select(Tab, MS) end,
    mnesia:transaction(F).

setup_mnesia() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()], [{backend_types,
					  [{pg_copies,
					    mnesia_pg},
					   {leveldb_copies,
					    klarna_leveldb_backend}]}]),
    ok = mnesia:start().

setup() ->
    setup_mnesia(),
    {atomic,ok} = mnesia:create_table(d, [{disc_copies, [node()]},
					  {attributes, record_info(fields, x)},
					  {record_name, x}]),
    {atomic,ok} = mnesia:create_table(do, [{disc_only_copies, [node()]},
					   {attributes, record_info(fields, x)},
					   {record_name, x}]),
    {atomic,ok} = mnesia:create_table(pgb, [{pg_copies, [node()]},
					    {attributes, record_info(fields, x)},
					    {record_name, x}]),
    {atomic,ok} = mnesia:create_table(ldb, [{leveldb_copies, [node()]},
					    {attributes, record_info(fields, x)},
					    {record_name, x}]),
    ok = mnesia:wait_for_tables([d,do,pgb,ldb], 30000),
    ok.

cleanup() ->
    mnesia:delete_table(d),
    mnesia:delete_table(do),
    mnesia:delete_table(pgb),
    mnesia:delete_table(ldb),
    ok.

get_key(X) ->
    {X, key}.

get_value(X) ->
    {value, X, "valuable stuff " ++ integer_to_list(X), list_to_binary("valuable stuff " ++ integer_to_list(X)), math:pow(X,3)}.

get_fat(0, Acc) ->
    Acc;
get_fat(N, Acc) ->
    get_fat(N-1, <<N:32,Acc/binary>>).

fill_table(Tab, N) ->
    NumList = lists:seq(1,N),
    Fat = get_fat(10*1014, <<>>),
    lists:foreach(fun(X) ->
			  Name = get_key(X),
			  Val = get_value(X),
			  Bin = <<X:32, Fat/binary>>,
			  Rec = #x{name=Name, value=Val, fat=Bin},
			  write(Tab, Rec)
		  end, NumList).

iterate_table(Tab, N) ->
    NumList = lists:seq(1,N),
    L = lists:map(fun(X) ->
			  Name = get_key(X),
			  read(Tab, Name)
		  end, NumList),
    length(L).

search_table(Tab, N) ->
    NumList = lists:seq(1,N),
    L = lists:map(fun(X) ->
			  Name = get_key(X),
			  search(Tab, [{{x,Name,'$1', '_'},[],['$1']}])
		  end, NumList),
    length(L).

test_avg(M, F, A, N) when N > 0 ->
    L = test_loop(M, F, A, N, []),
    Length = length(L),
    Min = lists:min(L),
    Max = lists:max(L),
    Med = lists:nth(round((Length / 2)), lists:sort(L)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, L) / Length),
    io:format("Range: ~b - ~b mics~n"
          "Median: ~b mics~n"
          "Average: ~b mics~n",
          [Min, Max, Med, Avg]),
    Med.
 
test_loop(_M, _F, _A, 0, List) ->
    List;
test_loop(M, F, A, N, List) ->
    {T, _Result} = timer:tc(M, F, A),
    test_loop(M, F, A, N - 1, [T|List]).
