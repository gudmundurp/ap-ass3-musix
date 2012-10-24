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


stop(Pid) -> 
    Pid ! {self(), stop}.

job(CPid, MapFun, RedFun, RedInit, Data) ->
    %io:format("Starting job with data ~p~n", [Data]),
    Ans = rpc(CPid,{MapFun, RedFun, RedInit, Data, job}),
    %io:format("Got back answer: ~p~n",[Ans]),
    Ans.


%%%% Internal implementation

init(N) -> 
    Reducer = spawn(fun() -> reducer_loop() end),
    Mappers = spawn_mappers(N, Reducer,[]),
    %io:format("Ammount of Mappers is ~p and inside is ~p~n",[length(Mappers),Mappers]),
    {Reducer,Mappers}.

spawn_mappers(0, _, Mappers) ->
    Mappers;
spawn_mappers(N, Reducer, Mappers) ->
    Pid = spawn(fun() -> mapper_loop(Reducer, fun(X) -> X end) end),
    spawn_mappers(N-1,Reducer,[Pid | Mappers]).

%% synchronous communication

rpc(Pid, Request) ->
    %io:format("Starting rpc with request ~p~n", [Request]),
    Pid ! {self(), Request},
    receive
    {Pid, Response} ->
        %io:format("rpc with request ~p got response ~p~n", [Request,Response]),
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

foreach(F, [H|T]) ->
    F(H),
    foreach(F, T);
foreach(_, []) ->
    ok.

coordinator_loop(Reducer, Mappers) ->
    %io:format("Coordinator_loop waiting for message~n"),
    receive
    {From, stop} ->
        %io:format("~p stopping~n", [self()]),
        lists:foreach(fun stop_async/1, Mappers),
        stop_async(Reducer),
        reply_ok(From);
    {JPid,{MapFun, RedFun, RedInit, Data, job}} ->
        %io:format("Got job with data ~p and RedInit ~p~n",[Data,RedInit]),
        foreach(fun(M) -> setup_async(M,MapFun) end, Mappers),
	    spawn(fun() -> send_data(Mappers, Data) end),
		Result = rpc(Reducer,{RedFun, RedInit, length(Data)}),
		reply_ok(JPid,Result),
        coordinator_loop(Reducer, Mappers)
    end.

send_data(Mappers, Data) ->
    %io:format("Starting data send loop~n"),
    send_loop(Mappers, Mappers, Data).
send_loop(Mappers, [Mid|Queue], [D|Data]) ->
    %io:format("Inside loop, head of data is ~p and rest is ~p. Amount of mappers left is ~p~n",[D,Data,length(Queue)]),
    data_async(Mid, D),
    send_loop(Mappers, Queue, Data);
send_loop(_, _, []) -> 
    %io:format("End of data reached, hurray!~n"),
    ok;
send_loop(Mappers, [], Data) ->
    %io:format("WTF?"),
    send_loop(Mappers, Mappers, Data).

%io:format("Refilling Mapper queue, data is ~p, there are ~p mappers~n",[Data,length(Mappers)]),
%%% Reducer

reducer_loop() ->
    receive
    stop -> 
        %io:format("Reducer ~p stopping~n", [self()]),
        ok;
    {Cid, {RedFun,RedInit,Length}} ->
        Acc = gather_data_from_mappers(RedFun,RedInit,Length),
		reply_ok(Cid, Acc),
        reducer_loop()
    end.
	
gather_data_from_mappers(_,   Acc, 0) -> Acc;
gather_data_from_mappers(Fun, Acc, Missing) ->
    receive
        {data, D} ->
	        %io:format("D is ~p and Acc is ~p. Will compute result.~n",[D,Acc]),
            Ans = Fun(D,Acc),
	        %io:format("Result of Fun(D,Acc) is ~p~n",[Ans]),
            gather_data_from_mappers(Fun,Ans,Missing-1)
    end.


%%% Mapper

mapper_loop(Reducer, Fun) ->
    receive
    stop -> 
        %io:format("Mapper ~p stopping~n", [self()]),
        ok;
    {data, D } ->
        data_async(Reducer,Fun(D)),
        mapper_loop(Reducer,Fun);
    { setup, F } ->
        mapper_loop(Reducer,F);
    _ ->
        %io:format("unknown message: ~p~n",[Unknown]), 
        mapper_loop(Reducer, Fun)
    end.

