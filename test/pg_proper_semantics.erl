%% This module uses the proper_statem pattern to generate random
%% sequences of commands, mixing dirty and transaction operations
%% (including dirty ops from within transactions). Each sequence is run
%% against a disc_only_copies table and a pg_copies table, after
%% which the result of each operation in the sequence is compared between
%% the two runs. The postcondition is that every command in every sequence
%% should yield the same value against both backends.
%% 
%% @end
-module(pg_proper_semantics).

-export([test/0,
	 test/1,
	 setup/0,
	 setup_mnesia/0,
	 prop_seq/0]).

%% statem callbacks
-export([initial_state/0,
	 command/1,
	 precondition/2,
	 postcondition/3,
	 next_state/3]).

%% command callbacks
-export([activity/2]).

-include_lib("proper/include/proper.hrl").

-record(st, {}).
-define(KEYS, [a,b,c]).

test() ->
    test(100).

test(N) ->
    setup_mnesia(),
    true = proper:quickcheck(?MODULE:prop_seq(), N),
    ok.

prop_seq() ->
    ?FORALL(Cmds, proper_statem:commands(?MODULE),
	    begin
		setup(),
		{H, S, Res} =
		    proper_statem:run_commands(?MODULE, Cmds),
		cleanup(),
		?WHENFAIL(
		   io:fwrite("History: ~w~n"
			     "State  : ~w~n"
			     "Result : ~w~n", [H, S, Res]),
		   proper:aggregate(
		     proper_statem:command_names(Cmds), Res =:= ok))
	    end).

setup_mnesia() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()], [{backend_types,
					  [{pg_copies,
					    mnesia_pg}]}]),
    ok = mnesia:start().

setup() ->
    {atomic,ok} = mnesia:create_table(d, [{disc_copies, [node()]},
					  {record_name, x}]),
    {atomic,ok} = mnesia:create_table(pg, [{pg_copies, [node()]},
					  {record_name, x}]),
    ok = mnesia:wait_for_tables([d,pg], 30000),
    ok.

cleanup() ->
    {atomic, ok} = mnesia:delete_table(d),
    {atomic, ok} = mnesia:delete_table(pg),
    ok.

initial_state() ->
    #st{}.

command(#st{}) ->
    ?LET(Type, type(),
	 {call, ?MODULE, activity, [Type, sequence()]}).

type() ->
    proper_types:oneof([async_dirty, transaction]).
    
precondition(_, _) ->
    true.

postcondition(_, {call,?MODULE,activity,_}, {A, B}) ->
    A == B;
postcondition(_, _, _) ->
    false.

next_state(St, _, _) ->
    St.

sequence() ->	
    proper_types:list(db_cmd()).

db_cmd() ->
    ?LET(Type, type(),
	 proper_types:oneof([{Type, read, key()},
			     {Type, write, key(), value()},
			     {Type, delete, key()}])).

key() ->
    proper_types:oneof([a,b,c]).

value() ->
    proper_types:oneof([1,2,3]).

activity(Type, Seq) ->
    {mnesia:activity(Type, fun() ->
				   apply_seq(Type, d, Seq)
			   end),
     mnesia:activity(Type, fun() ->
				   apply_seq(Type, pg, Seq)
			   end)}.

apply_seq(Type, Tab, Seq) ->
    apply_seq(Type, Tab, Seq, []).

apply_seq(transaction=X, Tab, [H|T], Acc) ->
    Res = case H of
	      {X,read, K}   -> mnesia:read(Tab, K, read);
	      {_,read, K}   -> mnesia:dirty_read(Tab,K);
	      {X,write,K,V} -> mnesia:write(Tab, {x, K, V}, write);
	      {_,write,K,V} -> mnesia:dirty_write(Tab, {x,K,V});
	      {X,delete,K}  -> mnesia:delete(Tab, K, write);
	      {_,delete,K}  -> mnesia:dirty_delete(Tab,K)
	  end,
    apply_seq(X, Tab, T, [Res|Acc]);
apply_seq(X, Tab, [H|T], Acc) ->
    Res = case H of
	      {_,read, K}   -> mnesia:read(Tab, K, read);
	      {_,write,K,V} -> mnesia:write(Tab, {x, K, V}, write);
	      {_,delete,K}  -> mnesia:delete(Tab, K, write)
	  end,
    apply_seq(X, Tab, T, [Res|Acc]);
apply_seq(_, _, [], Acc) ->
    lists:reverse(Acc).
