%%%-------------------------------------------------------------------
%%% @author Ken Friis Larsen <kflarsen@diku.dk>
%%% @copyright (C) 2011, Ken Friis Larsen
%%% Created : Oct 2011 by Ken Friis Larsen <kflarsen@diku.dk>
%%%-------------------------------------------------------------------
-module(mr).

-export([start/1, stop/1, job/5]).

%%%% Interface

start(N) ->
    {Reducer, Mappers} = init(N),
    {ok, spawn(fun() -> coordinator_loop(Reducer, Mappers) end)}.


stop(Pid) -> ....

job(CPid, MapFun, RedFun, RedInit, Data) -> ....


%%%% Internal implementation


init(N) ->
    Pid = spawn(mr, reducer_loop, []);
    spawn(fun() -> mapper_loop(Pid,
	%% Feed mapper loop


%% synchronous communication

rpc(Pid, Request) ->
    Pid ! {self(), Request},
    receive
	{Pid, Response} ->
	    Response
    end.

reply(From, Msg) ->
    From ! {self(), Msg}.

reply_ok(From) ->
    reply(From, ok).

reply_ok(From, Msg) ->
    reply(From, {ok, Msg}).


%% asynchronous communication

info(Pid, Msg) ->
    Pid ! Msg.

stop_async(Pid) ->
    info(Pid, stop).

data_async(Pid, D) ->
    info(Pid, {data, D}).

setup_async(Pid, Fun) ->
    info(Pid, {setup, Fun}).


%%% Coordinator

coordinator_loop(Reducer, Mappers) ->
    receive
	{From, stop} ->
	    io:format("~p stopping~n", [self()]),
	    lists:foreach(fun stop_async/1, Mappers),
	    stop_async(Reducer),
	    reply_ok(From);
	....
    end.


send_data(Mappers, Data) ->
    send_loop(Mappers, Mappers, Data).

send_loop(Mappers, [Mid|Queue], [D|Data]) ->
    data_async(Mid, D),
    send_loop(Mappers, Queue, Data);
send_loop(_, _, []) -> ok;
send_loop(Mappers, [], Data) ->
    send_loop(Mappers, Mappers, Data).


%%% Reducer

process_list(L) ->
    case L of
	        [] ->
			    ok;
		    [{Key,Value} | Tail] ->
		        GatherID ! {Key,Value};
			    process_list_from_mapper(Tail);
	end.

reducer_loop() ->
    receive
	stop -> 
	    io:format("Reducer ~p stopping~n", [self()]),
	    ok;
    L ->
	    process_list(L);
		reducer_loop()
    end.

gather_data_from_mappers(Fun, Acc, Missing) ->
    receive
	    {Key, Value} ->
		    Fun(Acc,Key,Value)
    end.


%%% Mapper

mapper_loop(Reducer, Fun) ->
    receive
	stop -> 
	    io:format("Mapper ~p stopping~n", [self()]),
	    ok;
	{ Key, Value } ->
	    Reducer ! Fun(Key, Value),
		mapper_loop(Reducer,Fun);
	Unknown ->
	    io:format("unknown message: ~p~n",[Unknown]), 
	    mapper_loop(Reducer, Fun)
    end.
