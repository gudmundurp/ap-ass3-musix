%%%-------------------------------------------------------------------
%%% @author Ken Friis Larsen <kflarsen@diku.dk>
%%% @copyright (C) 2011, Ken Friis Larsen
%%% Created : Oct 2011 by Ken Friis Larsen <kflarsen@diku.dk>
%%%-------------------------------------------------------------------
-module(mr_framework).

-export([start/1, stop/1, job/5]).

%%%% Interface

start(N) ->
    {Reducer, Mappers} = init(N),
    {ok, spawn(fun() -> coordinator_loop(Reducer, Mappers) end)}.


stop(Pid) -> 
    Pid ! {self(), stop}.

job(CPid, MapFun, RedFun, RedInit, Data) ->
    CPid ! {MapFun, RedFun, RedInit, Data ++ [endOfData], job}.


%%%% Internal implementation

init(N) -> 
    Reducer = spawn(fun() -> reducer_loop() end),
    Mappers = spawn_mappers(N, Reducer,[]),
    {Reducer,Mappers}.

spawn_mappers(0, Reducer,Mappers) ->
    [].
spawn_mappers(N, Reducer) ->
    Pid = spawn(fun() -> mapper_loop(Reducer, fun(X) -> X end) end),
    spawn_mappers(N-1,Reducer,[Pid | Mappers]).

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
	{MapFun, RedFun, RedInit, Data, job} ->
            Reducer ! { RedFun, RedInit, Mappers, start_gathering },
            foreach(fun(M) ->
	        M ! { MapFun, function },
		Mappers
            ),
	    send_data(Mappers, Data),
	    coordinator_loop(Reducer, Mappers);
	{Result, result} ->
	    io:format("~p~n",Result),
	    coordinator_loop(Reducer, Mappers);
	{ From, ok } ->
	    
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
            GatherID ! {Key,Value},
            process_list_from_mapper(Tail)
    end.

reducer_loop(CId) ->
    receive
	stop -> 
	    io:format("Reducer ~p stopping~n", [self()]),
	    ok;
	{RedFun,RedInit,start_gathering} ->
	    Acc = gather_data_from_mappers(RedFun,RedInit,Mappers),
	    Cid ! {Acc, result},
	    ok;
    end.

gather_data_from_mappers(Fun, Acc, [M|Tail]) ->
    receive
        {data, endOfData} ->
	    io:format("Finish gathering data",[]),
            Acc;
	{data, D} ->
            Acc = Fun(B,Acc),
	    gather_data_from_mappers(Fun,Acc,Missing)
    end.


%%% Mapper

mapper_loop(Reducer, Fun) ->
    receive
	stop -> 
	    io:format("Mapper ~p stopping~n", [self()]),
	    ok;
	{data, D } ->
	    Reducer ! {data, Fun(D)},
	    mapper_loop(Reducer,Fun);
	{ F, function } ->
	    mapper_loop(Reducer,F);
	Unknown ->
	    io:format("unknown message: ~p~n",[Unknown]), 
	    mapper_loop(Reducer, Fun)
    end.
