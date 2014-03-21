%%
%% %CopyrightBegin%
%%
%% TODO
%% Klarna Copyright Text
%%
%% %CopyrightEnd%
%%

%%%header_doc_include

-module(mnesia_pg).


%% ----------------------------------------------------------------------------
%% BEHAVIOURS
%% ----------------------------------------------------------------------------

-behaviour(mnesia_backend_type).

%% ----------------------------------------------------------------------------
%% EXPORTS
%% ----------------------------------------------------------------------------

%%
%% CONVENIENCE API
%%

-export([register/0,
         default_alias/0]).

%%
%% DEBUG API
%%

-export([show_table/1,
         show_table/2,
         show_table/3,
	 fold/6]).

%%
%% BACKEND CALLBACKS
%%

%% backend management
-export([init_backend/0,
	 add_aliases/1,
	 remove_aliases/1]).

%% schema level callbacks
-export([semantics/2,
	 check_definition/4,
	 create_table/3,
	 load_table/4,
	 close_table/2,
	 sync_close_table/2,
	 delete_table/2,
	 info/3]).

%% table synch calls
-export([sender_init/4,
	 sender_handle_info/5,
	 receiver_first_message/4,
	 receive_data/5,
	 receive_done/4]).

%% low-level accessor callbacks.
-export([delete/3,
	 first/2,
	 fixtable/3,
	 insert/3,
	 last/2,
	 lookup/3,
	 match_delete/3,
	 next/3,
	 prev/3,
	 repair_continuation/2,
	 select/1,
	 select/3,
	 select/4,
	 slot/3,
	 update_counter/4]).

%% Index consistency
-export([index_is_consistent/3,
	 is_index_consistent/2]).

%% record and key validation
-export([validate_key/6,
	 validate_record/6]).

%% file extension callbacks
-export([real_suffixes/0,
         tmp_suffixes/0]).

-export([ix_prefixes/3]).

%%%header_doc_include

%%%impl_doc_include

%% ----------------------------------------------------------------------------
%% INCLUDES
%% ----------------------------------------------------------------------------

-include_lib("mnesia/src/mnesia.hrl").
-include_lib("kernel/include/file.hrl").

%% ----------------------------------------------------------------------------
%% DEFINES
%% ----------------------------------------------------------------------------

%% Data and meta data (a.k.a. info) are stored in the same table.
%% This is a table of the first byte in data
%% 0    = before meta data
%% 1    = meta data
%% 2    = before data
%% >= 8 = data

-define(INFO_START, 0).
-define(INFO_TAG, 1).
-define(DATA_START, 2).
-define(BAG_CNT, 32).   % Number of bits used for bag object counter
-define(MAX_BAG, 16#FFFFFFFF).

-ifdef(DEBUG).
-define(dbg(E), E).
-else.
-define(dbg(E), ok).
-endif.
%% ----------------------------------------------------------------------------
%% RECORDS
%% ----------------------------------------------------------------------------

-record(sel, {alias,                            % TODO: not used
	      tab,
	      ref,
	      keypat,
	      ms,                               % TODO: not used
	      compiled_ms,
	      limit,
	      key_only = false,                 % TODO: not used
	      direction = forward}).            % TODO: not used

%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

register() ->
    Alias = default_alias(),
    Module = ?MODULE,
    mnesia:add_backend_type(Alias, Module),
    {ok, {Alias, Module}}.

default_alias() ->
    pg_copies.


%% ----------------------------------------------------------------------------
%% DEBUG API
%% ----------------------------------------------------------------------------

%% A debug function that shows the table content
show_table(Tab) ->
    show_table(default_alias(), Tab).

show_table(Alias, Tab) ->
    show_table(Alias, Tab, 100).

show_table(Alias, Tab, Limit) ->
    Ref = get_ref(Alias, Tab),
    with_iterator(Ref, fun(Curs) -> i_show_table(Curs, Limit) end).

%% PRIVATE

i_show_table(_, 0) ->
    {error, skipped_some};
i_show_table(Curs, Limit) ->
    case fetch_next(Curs) of
        {ok, EncKey, EncVal} ->
            {Type,Val} =
                case EncKey of
                    << ?INFO_TAG, K/binary >> ->
                        {info,{decode_key(K),decode_val(EncVal)}};
                    _ ->
                        K = decode_key(EncKey),
                        V = decode_val(EncVal),
                        {data,{K,V}}
                end,
            io:fwrite("~p: ~p~n", [Type, Val]),
            i_show_table(Curs, Limit-1);
        _ ->
            ok
    end.

%% ----------------------------------------------------------------------------
%% BACKEND CALLBACKS
%% ----------------------------------------------------------------------------

%% backend management

init_backend() ->
    application:start(mnesia_pg),
    ok.

add_aliases(_Aliases) ->
    ok.

remove_aliases(_Aliases) ->
    ok.

%% schema level callbacks

%% This function is used to determine what the plugin supports
%% semantics(Alias, storage)   ->
%%    ram_copies | disc_copies | disc_only_copies  (mandatory)
%% semantics(Alias, types)     ->
%%    [bag | set | ordered_set]                    (mandatory)
%% semantics(Alias, index_fun) ->
%%    fun(Alias, Tab, Pos, Obj) -> [IxValue]       (optional)
%% semantics(Alias, _) ->
%%    undefined.
%%
semantics(_Alias, storage) -> disc_only_copies;
semantics(_Alias, types  ) -> [set, ordered_set];
semantics(_Alias, index_types) -> [ordered];
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(_Alias, _) ->
    true.

index_is_consistent(_Alias, _, __Bool) ->
    ok.

%% PRIVATE FUN
index_f(_Alias, _Tab, Pos, Obj) ->
    [element(Pos, Obj)].

ix_prefixes(_Tab, _Pos, Obj) ->
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
	      try Pfxs = prefixes(list_to_binary(V)),
		   Pfxs ++ Acc
	      catch
		  error:_ ->
		      Acc
	      end;
	 (V, Acc) when is_binary(V) ->
	      Pfxs = prefixes(V),
	      Pfxs ++ Acc;
	 (_, Acc) ->
	      Acc
      end, [], tl(tuple_to_list(Obj))).

prefixes(<<P:3/binary, _/binary>>) ->
    [P];
prefixes(_) ->
    [].

%% For now, only verify that the type is set or ordered_set.
%% set is OK as ordered_set is a kind of set.
check_definition(Alias, Tab, Nodes, Props) ->
    Id = {Alias, Nodes},
    Props1 = lists:map(
	       fun({type, T} = P) ->
		       if T==set; T==ordered_set ->
                               P;
			  true ->
			       mnesia:abort({combine_error,
                                             Tab,
                                             [Id, {type,T}]})
		       end;
		  ({user_properties, _} = P) ->
		       P;
		  (P) -> P
	       end, Props),
    {ok, Props1}.

-ifdef(DEBUG).
pp_calls(I, [{M,F,A,Pos} | T]) ->
    Spc = lists:duplicate(I, $\s),
    Pp = fun(Mx,Fx,Ax,Px) ->
		[atom_to_list(Mx),":",atom_to_list(Fx),"/",integer_to_list(Ax),
		 pp_pos(Px)]
	end,
    [Pp(M,F,A,Pos)|[["\n",Spc,Pp(M1,F1,A1,P1)] || {M1,F1,A1,P1} <- T]].
pp_pos([]) -> "";
pp_pos(L) when is_integer(L) ->
    [" (", integer_to_list(L), ")"];
pp_pos([{file,_},{line,L}]) ->
    [" (", integer_to_list(L), ")"].

dbg_caller() ->
    try 1=2
    catch
	error:_ ->
	    tl(erlang:get_stacktrace())
    end.
-endif.

sync_close_table(_Alias, _Tab) ->
    ok.

info(Alias, Tab, size) ->
    count_table(Alias, Tab);			% not fast...
info(_Alias, _Tab, _Item) ->
    undefined.

%% table synch calls, not used in KRED

%% ===========================================================
%% Table synch protocol
%% Callbacks are
%% Sender side:
%%  1. sender_init(Alias, Tab, RemoteStorage, ReceiverPid) ->
%%        {standard, InitFun, ChunkFun} | {InitFun, ChunkFun} when
%%        InitFun :: fun() -> {Recs, Cont} | '$end_of_table'
%%        ChunkFun :: fun(Cont) -> {Recs, Cont1} | '$end_of_table'
%%
%%       If {standard, I, C} is returned, the standard init message will be
%%       sent to the receiver. Matching on RemoteStorage can reveal if a
%%       different protocol can be used.
%%
%%  2. InitFun() is called
%%  3a. ChunkFun(Cont) is called repeatedly until done
%%  3b. sender_handle_info(Msg, Alias, Tab, ReceiverPid, Cont) ->
%%        {ChunkFun, NewCont}
%%
%% Receiver side:
%% 1. receiver_first_message(SenderPid, Msg, Alias, Tab) ->
%%        {Size::integer(), State}
%% 2. receive_data(Data, Alias, Tab, _Sender, State) ->
%%        {more, NewState} | {{more, Msg}, NewState}
%% 3. receive_done(_Alias, _Tab, _Sender, _State) ->
%%        ok
%%
%% The receiver can communicate with the Sender by returning
%% {{more, Msg}, St} from receive_data/4. The sender will be called through
%% sender_handle_info(Msg, ...), where it can adjust its ChunkFun and
%% Continuation. Note that the message from the receiver is sent once the
%% receive_data/4 function returns. This is slightly different from the
%% normal mnesia table synch, where the receiver acks immediately upon
%% reception of a new chunk, then processes the data.
%%

sender_init(Alias, Tab, _RemoteStorage, _Pid) ->
    %% Need to send a message to the receiver. It will be handled in
    %% receiver_first_message/4 below. There could be a volley of messages...
    {standard,
     fun() ->
	     select(Alias, Tab, [{'_',[],['$_']}], 100)
     end,
     chunk_fun()}.

sender_handle_info(_Msg, _Alias, _Tab, _ReceiverPid, Cont) ->
    %% ignore - we don't expect any message from the receiver
    {chunk_fun(), Cont}.

receiver_first_message(_Pid, {first, Size} = _Msg, _Alias, _Tab) ->
    {Size, _State = []}.

receive_data(Data, Alias, Tab, _Sender, State) ->
    [insert(Alias, Tab, Obj) || Obj <- Data],
    {more, State}.

receive_done(_Alias, _Tab, _Sender, _State) ->
    ok.

%% End of table synch protocol
%% ===========================================================

%% PRIVATE

chunk_fun() ->
    fun(Cont) ->
	    select(Cont)
    end.

%% low-level accessor callbacks.

delete(_Alias, Tab, Key) ->
    delete_from(Tab, encode_primary_key(Key)).

%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    true.

insert(_Alias, Tab, Obj) ->
    Pos = keypos(Tab),
    Key = element(Pos, Obj),
    PKey = encode_primary_key(Key),	% lookup via index
    TKey = encode_key(Key),		% sorted via index
    Val = encode_val(Obj),
    upsert_into(Tab, PKey, TKey, Val).

lookup(Alias, Tab, Key) ->
    {C, Tab} = get_ref(Alias, Tab),
    PKey = encode_primary_key(Key),
    {ok, N, _, Res} = pgsql:equery(C, "select from " ++ Tab ++ " where erlsha=$1", [PKey]),
    mnesia_pg_conns:free(C),
    case (N) of
	0 ->
	    [];
	1 ->
	    [Row] = Res,
	    [decode_val(lists:nth(3, Row))]
    end.

match_delete(_Alias, Tab, Pat) when is_atom(Pat) ->
    case is_wild(Pat) of
	true ->
	    delete_all(Tab),
	    ok;
	false ->
	    %% can this happen??
	    error(badarg)
    end;
match_delete(Alias, Tab, Pat) when is_tuple(Pat) ->
    KP = keypos(Tab),
    Key = element(KP, Pat),
    case is_wild(Key) of
	true ->
	    delete_all(Tab),
	    ok;
	false ->
	    delete_from_pattern(Alias, Tab, Pat)
    end,
    ok.

%% These are NOT fast. We should use a cursor, however, the mnesia API only passes us a table name and a key,
%% and we have no trivial way of keeping a cursor in cases these functions are used for iterating through a table.
%% Hence, we end up searching for the row each time, using select. Blah!
%% Tracking this 'magically' will hurt a bit.
%% The simplest optimization is to add a separate index on erlkey that is ordered and that we can rely on when scanning forward/backward
next(Alias, Tab, Key) ->
    {C, Tab} = get_ref(Alias, Tab),
    {ok, _, Res} = pgsql:equery(C, "select erlkey from " ++ Tab ++ "where erlkey > $1 order by erlkey limit 1", [encode_key(Key)]),
    mnesia_pg_conns:free(C),
    case (Res) of
	[] ->
	    '$end_of_table';
	1 ->
	    [{TKey}] = Res,
	    decode_key(TKey)
    end.

prev(Alias, Tab, Key) ->
    {C, Tab} = get_ref(Alias, Tab),
    {ok, _, Res} = pgsql:equery(C, "select erlkey from " ++ Tab ++ "where erlkey < $1 order by erlkey limit 1", [encode_key(Key)]),
    mnesia_pg_conns:free(C),
    case (Res) of
	[] ->
	    '$end_of_table';
	1 ->
	    [{TKey}] = Res,
	    decode_key(TKey)
    end.

first(Alias, Tab) ->
    {C, Tab} = get_ref(Alias, Tab),
    {ok, _, Res} = pgsql:squery(C, "select erlkey from " ++ Tab ++ " order by erlkey limit 1"),
    mnesia_pg_conns:free(C),
    case (Res) of
	[] ->
	    '$end_of_table';
	1 ->
	    [{TKey}] = Res,
	    decode_key(TKey)
    end.

last(Alias, Tab) ->
    {C, Tab} = get_ref(Alias, Tab),
    {ok, _, Res} = pgsql:squery(C, "select erlkey from " ++ Tab ++ " order by erlkey desc limit 1"),
    mnesia_pg_conns:free(C),
    case (Res) of
	[] ->
	    '$end_of_table';
	1 ->
	    [{TKey}] = Res,
	    decode_key(TKey)
    end.

repair_continuation(Cont, _Ms) ->
    Cont.

select(C) ->
    Cont = get_sel_cont(C),
    Cont().

select(Alias, Tab, Ms) ->
    case select(Alias, Tab, Ms, infinity) of
	{Res, '$end_of_table'} ->
	    Res;
	'$end_of_table' ->
	    '$end_of_table'
    end.

select(Alias, Tab, Ms, Limit) when Limit==infinity; is_integer(Limit) ->
    Ref = get_ref(Alias, Tab),
    do_select(Ref, Tab, Ms, Limit).

slot(Alias, Tab, Pos) when is_integer(Pos), Pos >= 0 ->
    Ref = get_ref(Alias, Tab),
    F = fun(Curs) -> slot_iter_set(fetch_next(Curs), Curs, 0, Pos) end,
    with_iterator(Ref, F);
slot(_, _, _) ->
    error(badarg).


%% Exactly which objects Mod:slot/2 is supposed to return is not defined,
%% so let's just use the same version for both set and bag. No one should
%% use this function anyway, as it is ridiculously inefficient.
slot_iter_set({ok, _K, V}, _Curs, P, P) ->
    [decode_val(V)];
slot_iter_set({ok, _, _}, Curs, P1, P) when P1 < P ->
    slot_iter_set(fetch_next(Curs), Curs, P1+1, P);
slot_iter_set(Res, _, _, _) when element(1, Res) =/= ok ->
    '$end_of_table'.

%% update value of key by incrementing
%% need select for update since the value in erlval is an int encoded as a binary
update_counter(Alias, Tab, Key, Val) when is_integer(Val) ->
    {C, Tab} = get_ref(Alias, Tab),
    PKey = encode_primary_key(Key),
    {ok, [], []} = pgsql:squery(C, "begin"),
    {ok, N, _, Res} = pgsql:equery(C, "select from " ++ Tab ++ " where erlsha=$1 for update", [PKey]),
    case (N) of
	0 ->
	    Return = badarg;
	1 ->
	    [Row] = Res,
	    case decode_val(lists:nth(3, Row)) of
		{_, _, Old} when is_integer(Old) ->
		    New = Old+Val,
		    Return = New,
		    pgsql:equery(C, "update " ++ Tab ++ " set erlval=$2, change_time = current_timestamp where erlsha=$1", [PKey, encode_val(New)]);
		_ ->
		    Return = badarg
	    end
    end,
    mnesia_pg_conns:free(C),
    Return.

%% PRIVATE

with_iterator({C, Tab}, F) ->
    Curs = open_cursor(C, Tab),
    try F(Curs)
    after
	close_cursor(Curs)
    end.

with_positioned_iterator({C, Tab}, Pat, F) ->
    Curs = open_cursor(C, Tab, Pat),
    try F(Curs)
    after
	close_cursor(Curs)
    end.

%% record and key validation

validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    {RecName, Arity, Type}.

validate_record(_Alias, _Tab, RecName, Arity, Type, _Obj) ->
    {RecName, Arity, Type}.

%% file extension callbacks

%% Extensions for files that are permanent. Needs to be cleaned up
%% e.g. at deleting the schema.
real_suffixes() ->
    [].

%% Extensions for temporary files. Can be cleaned up when mnesia
%% cleans up other temporary files.
tmp_suffixes() ->
    [].

%% ----------------------------------------------------------------------------
%% PRIVATE SELECT MACHINERY
%% ----------------------------------------------------------------------------

do_select(Ref, Tab, MS, Limit) ->
    do_select(Ref, Tab, MS, false, Limit).

do_select(Ref, Tab, MS, AccKeys, Limit) when is_boolean(AccKeys) ->
    Keypat = keypat(MS, keypos(Tab)),
    Sel = #sel{tab = Tab,
	       ref = Ref,
	       keypat = Keypat,
	       ms = MS,
	       compiled_ms = ets:match_spec_compile(MS),
	       key_only = needs_key_only(MS),
	       limit = Limit},
    {Pfx, _} = Keypat,
    case (Pfx) of
	<<>> ->
	    with_iterator(Ref, fun(Curs) -> i_do_select(Curs, Sel, AccKeys, []) end);
	_ ->
	    with_positioned_iterator(Ref, Pfx, fun(Curs) -> i_do_select(Curs, Sel, AccKeys, []) end)
    end.

i_do_select(Curs, #sel{keypat = {Pfx, _KP},
                    compiled_ms = MS,
                    limit = Limit} = Sel, AccKeys, Acc) ->
    select_traverse(fetch_next(Curs), Curs, Limit, Pfx, MS, Sel, AccKeys, Acc).

needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    not(wild_in_body(BodyVars) orelse
	case bound_in_headpat(HP) of
	    {all,V} -> lists:member(V, BodyVars);
	    none    -> false;
	    Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
	end);
needs_key_only(_) ->
    %% don't know
    false.

extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
	true ->
	    [T];
	false ->
	    []
    end;
extract_vars(_) ->
    [].

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
		      intersection(Vs, BodyVars) =/= []
	      end, Vars).

intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2);
bound_in_headpat(_) ->
    %% this is not the place to throw an exception
    none.

map_vars([H|T], P) ->
    case extract_vars(H) of
	[] ->
	    map_vars(T, P+1);
	Vs ->
	    [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

select_traverse({ok, K, V}, Curs, Limit, Pfx, MS, Sel,
		AccKeys, Acc) ->
    case is_prefix(Pfx, K) of
	true ->
	    Rec = decode_val(V),
	    case ets:match_spec_run([Rec], MS) of
		[] ->
		    select_traverse(fetch_next(Curs), Curs, Limit, Pfx, MS, Sel, AccKeys, Acc);
		[Match] ->
                    Fun = fun(NewLimit, NewAcc) ->
                                  select_traverse(fetch_next(Curs), Curs,
                                                  NewLimit, Pfx, MS, Sel,
                                                  AccKeys, NewAcc)
                          end,
		    Acc1 = if AccKeys ->
				   [{K, Match}|Acc];
			      true ->
				   [Match|Acc]
			   end,
                    traverse_continue(decr(Limit), Fun, Acc1, Sel)
	    end;
	false ->
	    {lists:reverse(Acc), '$end_of_table'}
    end;
select_traverse(void, _, _, _, _, _, _, Acc) ->
    {lists:reverse(Acc), '$end_of_table'}.

is_prefix(A, B) when is_binary(A), is_binary(B) ->
    Sa = byte_size(A),
    case B of
	<<A:Sa/binary, _/binary>> ->
	    true;
	_ ->
	    false
    end.




decr(I) when is_integer(I) ->
    I-1;
decr(infinity) ->
    infinity.

traverse_continue(0, F, Acc, #sel{limit = Limit, ref = Ref}) ->
    {lists:reverse(Acc),
     fun() ->
	     with_iterator(Ref,
			   fun(_) ->
				   F(Limit, [])
			   end)
     end};
traverse_continue(Limit, F, Acc, _) ->
    F(Limit, Acc).

keypat([{HeadPat,Gs,_}|_], KeyPos) when is_tuple(HeadPat) ->
    KP      = element(KeyPos, HeadPat),
    KeyVars = extract_vars(KP),
    Guards  = relevant_guards(Gs, KeyVars),
    Pfx     = mnesia_sext:prefix(KP),
    {Pfx, [{KP, Guards, [true]}]};
keypat(_, _) ->
    {<<>>, [{'_',[],[true]}]}.

relevant_guards(Gs, Vars) ->
    case Vars -- ['_'] of
	[] ->
	    [];
	Vars1 ->
	    Fun =
                fun(G) ->
                        Vg = extract_vars(G),
                        intersection(Vg, Vars1) =/= [] andalso (Vg--Vars1) == []
                end,
	    lists:filter(Fun, Gs)
    end.

get_sel_cont(C) ->
    Cont = case C of
	       {?MODULE, C1} -> C1;
	       _ -> C
	   end,
    case Cont of
	_ when is_function(Cont, 0) ->
	    case erlang:fun_info(Cont, module) of
		{_, ?MODULE} ->
		    Cont;
		_ ->
		    erlang:error(badarg)
	    end;
	'$end_of_table' ->
	    fun() -> '$end_of_table' end;
	_ ->
	    erlang:error(badarg)
    end.


%% ----------------------------------------------------------------------------
%% COMMON PRIVATE
%% ----------------------------------------------------------------------------

%% Note that since a callback can be used as an indexing backend, we
%% cannot assume that keypos will always be 2. For indexes, the tab
%% name will be {Tab, index, Pos}, and The object structure will be
%% {{IxKey,Key}} for an ordered_set index, and {IxKey,Key} for a bag
%% index.
%%
keypos({_, index, _}) ->
    1;
keypos({_, retainer, _}) ->
    2;
keypos(Tab) when is_atom(Tab) ->
    2.

encode_primary_key(Key) ->
    crypto:sha(term_to_binary(Key)).

encode_key(Key) ->
    mnesia_sext:encode(Key).

decode_key(CodedKey) ->
    case mnesia_sext:partial_decode(CodedKey) of
	{full, Result, _} ->
	    Result;
	_ ->
	    error(badarg, CodedKey)
    end.

encode_val(Val) ->
    term_to_binary(Val).

decode_val(CodedVal) ->
    binary_to_term(CodedVal).

get_ref(_Alias, Tab) ->
    mnesia_pg_conns:alloc(Tab).			% returns a connection

fold(Alias, Tab, Fun, Acc, MS, N) ->
    Ref = get_ref(Alias, Tab),
    do_fold(Ref, Tab, Fun, Acc, MS, N).

do_fold(Ref, Tab, Fun, Acc, MS, N) ->
    {AccKeys, F} =
	if is_function(Fun, 3) ->
		{true, fun({K,Obj}, Acc1) ->
			       Fun(Obj, K, Acc1)
		       end};
	   is_function(Fun, 2) ->
		{false, Fun}
	end,
    do_fold1(do_select(Ref, Tab, MS, AccKeys, N), F, Acc).

do_fold1('$end_of_table', _, Acc) ->
    Acc;
do_fold1({L, Cont}, Fun, Acc) ->
    Acc1 = lists:foldl(Fun, Acc, L),
    do_fold1(select(Cont), Fun, Acc1).

is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin 
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end;
is_wild(_) ->
    false.


%% PG interface
%% Each table has 4 columns: sha encoded key encoded value change time
%% an index is created on erlsha
%% encoded key can be ordered by asc, which adheres to erlang ordering

open_cursor(C, Tab) ->
    CursorName = "cursor_" ++ Tab ++ mnesia_pg_conns:ref(),
    {ok, [], []} = pgsql:squery(C, "begin"),
    {ok, [], []} = pgsql:squery(C, "declare " ++ CursorName ++ " cursor for select * from " ++ Tab ++ " order by erlkey asc"),
    {C, CursorName}.

open_cursor(C, Tab, Pat) ->
    CursorName = "cursor_" ++ Tab ++ mnesia_pg_conns:ref(),
    {ok, [], []} = pgsql:squery(C, "begin"),
    {ok, [], []} = pgsql:squery(C, "declare " ++ CursorName ++ " cursor for select * from " ++ Tab ++ " where erlkey like '" ++ Pat ++ "%' order by erlkey asc"),
    {C, CursorName}.

close_cursor({C, CursorName}) ->
    pgsql:squery(C, "close " ++ CursorName),
    mnesia_pg_conns:free(C).

fetch_next({C, CursorName}) ->
    Res = pgsql:squery(C, "fetch next from " ++ CursorName),
    case (Res) of
	{ok, 0} ->
	    void;
	{ok, 1, _, R} ->
	    {ok, lists:nth(2, R), lists:nth(3, R)}
    end.

%% TODO: replace with call to upsert function, as 
%% select mnesia_upsert($1,$2,$3,CURRENT_TIMESTAMP)
upsert_into(Tab, PKey, TKey, Val) ->
    {C, Tab} = mnesia_pg_conns:alloc(Tab),    
    pgsql:equery(C, "insert into " ++ Tab ++ " (erlsha,erlkey,erlval,change_time) values ($1,$2,$3,CURRENT_TIMESTAMP)", [PKey,TKey,Val]),
    mnesia_pg_conns:free(C).

delete_from(Tab, PKey) ->
    {C, Tab} = mnesia_pg_conns:alloc(Tab),    
    {ok, N} = pgsql:equery(C, "delete from " ++ Tab ++ " where erlsha=$1", [PKey]),
    mnesia_pg_conns:free(C),
    N.

delete_list(Tab, PKeyList) ->
    {C, Tab} = mnesia_pg_conns:alloc(Tab),
    N = lists:foldl(fun(PKey, Sum) ->
			    {ok, N} = pgsql:equery(C, "delete from " ++ Tab ++ " where erlsha=$1", [PKey]),
			    Sum + N
		    end,
		    0,
		    PKeyList),		       
    mnesia_pg_conns:free(C),
    N.

delete_all(Tab) ->
    {C, Tab} = mnesia_pg_conns:alloc(Tab),    
    pgsql:squery(C, "delete from " ++ Tab),
    mnesia_pg_conns:free(C).

delete_from_pattern(Alias, Tab, Pat) ->
    Ref = get_ref(Alias, Tab),
    Fun = fun(_, Key, Acc) -> [Key|Acc] end,
    Keys = do_fold(Ref, Tab, Fun, [], [{Pat,[],['$_']}], 30),
    if Keys == [] ->
	    ok;
       true ->
	    delete_list(Tab, Keys),
	    ok
    end.

create_table(Alias, Tab, _Props) ->
    {C, Tab} = get_ref(Alias, Tab),
    pgsql:squery(C, "drop table " ++ Tab),
    {ok, [], []} = pgsql:squery(C, "create table " ++ Tab ++ "( erlsha numeric, erlkey character varying(2048), erlval bytea, change_time timestamp )"),
    {ok, [], []} = pgsql:squery(C, "create or replace function upsert_" ++ Tab ++ "(pkey numeric, ekey character varying, eval bytea) RETURNS void AS\n" ++
			 "$$\n" ++
			 "BEGIN\n" ++
			 " INSERT INTO " ++ Tab ++ " VALUES (pkey, ekey, eval, current_timestamp);\n" ++
			 "EXCEPTION WHEN unique_violation THEN\n" ++
			 " UPDATE " ++ Tab ++ " SET erlval = eval, change_time = current_timestamp WHERE erlsha = pkey;\n" ++
			 "END;\n" ++
			 "$$\n" ++
			 "LANGUAGE plpgsql;"),
    {ok, [], []} = pgsql:squery(C, "create or replace index " ++ Tab ++ "_term_idx ON " ++ Tab ++ " USING btree (erlkey)"),
    {ok, [], []} = pgsql:squery(C, "create or replace unique index " ++ Tab ++ "_sha_idx ON " ++ Tab ++ " USING hash (erlsha)"),
    mnesia_pg_conns:free(C).

delete_table(Alias, Tab) ->
    {C, Tab} = get_ref(Alias, Tab),
    pgsql:squery(C, "drop table " ++ Tab),
    mnesia_pg_conns:free(C).

load_table(_Alias, _Tab, _LoadReason, _Opts) ->
    ok.

close_table(_Alias, _Tab) ->
    ok.

count_table(Alias, Tab) ->
    {C, Tab} = get_ref(Alias, Tab),
    {ok, _, [{X}]} = pgsql:squery(C, "select count(*) from " ++ Tab),
    mnesia_pg_conns:free(C),
    list_to_integer(binary_to_list(X)).

%%%impl_doc_include
