-module(pg_perf).

-include_lib("stdlib/include/ms_transform.hrl").

-export([setup/0,
	 fill_table/2, fill_table/4,
	 iterate_table/2, iterate_table/4,
	 search_table/2, search_table/4,
	 cleanup/0,
	 test_avg/4]).

-export([seed/0, seed/3]).

-export([write/2, write/3,
	 read/2, read/3,
	 search/2, search/3]).

-record(x, {name, value, fat}).

write(Tab, Rec) ->
    write(transaction, Tab, Rec).

write(Type, Tab, Rec) ->
    Fun = fun() ->
		  mnesia:write(Tab, Rec, write)
	  end,
    mnesia:activity(Type, Fun).

read(Tab, Key) ->
    read(transaction, Tab, Key).

read(Type, Tab, Key) ->
    Fun = fun() ->
		  mnesia:read(Tab, Key, read)
	  end,
    mnesia:activity(Type, Fun).

search(Tab, MS) ->
    search(transaction, Tab, MS).

search(Type, Tab, MS) ->
    F = fun() -> mnesia:select(Tab, MS) end,
    mnesia:activity(Type, F).

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
    {value, X,
     "valuable stuff " ++ integer_to_list(X),
     list_to_binary("valuable stuff " ++ integer_to_list(X)),
     math:pow(X,3)}.

get_fat(0, Acc) ->
    Acc;
get_fat(N, Acc) ->
    get_fat(N-1, <<N:32,Acc/binary>>).

fill_table(Tab, N) ->
    fill_table(transaction, seq, Tab, N).

fill_table(Type, Keyspace, Tab, N) ->
    %% Fat = get_fat(10*1014, <<>>),
    Fat = <<>>,
    foreach(fun(X) ->
		    Name = get_key(X),
		    Val = get_value(X),
		    Bin = <<X:32, Fat/binary>>,
		    Rec = #x{name=Name, value=Val, fat=Bin},
		    write(Type, Tab, Rec)
	    end, Keyspace, N).

iterate_table(Tab, N) ->
    iterate_table(transaction, seq, Tab, N).

iterate_table(Type, Keyspace, Tab, N) ->
    fold(fun(X,Acc) ->
		 Name = get_key(X),
		 [#x{name = Name}] = read(Type, Tab, Name),
		 Acc+1
	 end, 0, Keyspace, N).

search_table(Tab, N) ->
    search_table(transaction, seq, Tab, N).

search_table(Type, Keyspace, Tab, N) ->
    fold(fun(X, Acc) ->
		 Name = get_key(X),
		 Val = get_value(X),
		 [Val] =
		     search(Type, Tab, [{{x,Name,'$1', '_'},[],['$1']}]),
		 Acc + 1
	 end, 0, Keyspace, N).

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

foreach(F, seq, N) ->
    lists:foreach(F, lists:seq(1, N));
foreach(F, random, N) ->
    foreach(F, {random, seed(), N}, N);
foreach(F, {random, Seed}, N) ->
    foreach(F, {random, Seed, N}, N);
foreach(F, {random, Seed, Range}, N) ->
    foreach_f(F, fun(_) -> random_s(Range, Seed) end, N);
foreach(F, KS, N) when is_function(KS, 1) ->
    foreach_f(F, KS, N).

foreach_f(_, _, 0) -> ok;
foreach_f(F, KS, N) ->
    case KS(N) of
	done -> ok;
	{Key, KS1} ->
	    F(Key),
	    foreach_f(F, KS1, N-1)
    end.

fold(F, Acc, seq, N) ->
    lists:foldl(F, Acc, lists:seq(1, N));
fold(F, Acc, random, N) ->
    fold(F, Acc, {random, seed(), N}, N);
fold(F, Acc, {random, Seed}, N) ->
    fold(F, Acc, {random, Seed, N}, N);
fold(F, Acc, {random, Seed, Range}, N) ->
    fold_f(F, Acc, fun(_) -> random_s(Range, Seed) end, N);
fold(F, Acc, KS, N) when is_function(KS, 1) ->
    fold_f(F, Acc, KS, N).

fold_f(_, Acc, _, 0) -> Acc;
fold_f(F, Acc, KS, N) ->
    case KS(N) of
	done -> Acc;
	{Key, KS1} ->
	    fold_f(F, F(Key, Acc), KS1, N-1)
    end.

random_s(Range, Seed) ->
    {Key, NewSeed} = random:uniform_s(Range, Seed),
    {Key, fun(_) -> random_s(Range, NewSeed) end}.

seed() ->
    {M, S, U} = os:timestamp(),
    seed(M, S, U).

%% Create seed without side-effects (or at least repairing the unavoidable one)
seed(A,B,C) ->
    OldSeed = random:seed(A, B, C),
    NewSeed = get(random_seed),
    put(random_seed, OldSeed),
    NewSeed.
		   
