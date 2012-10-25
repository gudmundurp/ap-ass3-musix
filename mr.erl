%%%-------------------------------------------------------------------
%%% @author Ken Friis Larsen <kflarsen@diku.dk>
%%% @copyright (C) 2011, Ken Friis Larsen
%%% Created : Oct 2011 by Ken Friis Larsen <kflarsen@diku.dk>
%%%-------------------------------------------------------------------
-module(mr).

-export([start/1, stop/1, job/5, job/6]).

%%%% Interface

start(N) ->
    {Reducer, Mappers} = init(N),
    {ok, spawn(fun() -> coordinator_loop(Reducer, Mappers) end)}.


stop(Pid) -> 
    Pid ! {self(), stop}.

job(CPid, MapFun, RedFun, RedInit, Data) ->
    Ans = rpc(CPid,{MapFun, RedFun, RedInit, Data, job, false}),
    Ans.
job(CPid, MapFun, RedFun, RedInit, Data, debug) ->
    Ans = rpc(CPid,{MapFun, RedFun, RedInit, Data, job, true}),
    Ans.


%%%% Internal implementation

init(N) -> 
    Reducer = spawn(fun() -> reducer_loop() end),
    Mappers = spawn_mappers(N, Reducer,[]),
    {Reducer,Mappers}.

spawn_mappers(0, _, Mappers) ->
    Mappers;
spawn_mappers(N, Reducer, Mappers) ->
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

foreach(F, [H|T]) ->
    F(H),
    foreach(F, T);
foreach(_, []) ->
    ok.

coordinator_loop(Reducer, Mappers) ->
    receive
    {From, stop} ->
        io:format("~p stopping~n", [self()]),
        lists:foreach(fun stop_async/1, Mappers),
        stop_async(Reducer),
        reply_ok(From);
    {JPid,{MapFun, RedFun, RedInit, Data, job, Debug}} ->
        foreach(fun(M) -> setup_async(M,MapFun) end, Mappers),
	spawn(fun() -> send_data(Mappers, Data) end),
        {ok,Result} = rpc(Reducer,{RedFun, RedInit, length(Data), Debug}),
        reply_ok(JPid,Result),
        coordinator_loop(Reducer, Mappers)
    end.

send_data(Mappers, Data) ->
    send_loop(Mappers, Mappers, Data).
send_loop(Mappers, [Mid|Queue], [D|Data]) ->
    data_async(Mid, D),
    send_loop(Mappers, Queue, Data);
send_loop(_, _, []) ->     
    ok;
send_loop(Mappers, [], Data) ->
    send_loop(Mappers, Mappers, Data).

%%% Reducer

reducer_loop() ->
    receive
    stop -> 
        io:format("Reducer ~p stopping~n", [self()]),
        ok;
    {Cid, {RedFun,RedInit,Length,Debug}} ->
        Acc = gather_data_from_mappers(RedFun,RedInit,Length,Debug),
	reply_ok(Cid, Acc),
        reducer_loop()
    end.
	
gather_data_from_mappers(_,   Acc, 0, _) ->
    Acc;
gather_data_from_mappers(Fun, Acc, Missing,Debug) ->
    receive
        {data, D} ->
            Ans = Fun(D,Acc),
	    if
              (Debug) ->io:format("Missing: ~p~n", [Missing]);
              (true)  -> ok
            end,
            gather_data_from_mappers(Fun,Ans,Missing-1,Debug)
    end.


%%% Mapper

mapper_loop(Reducer, Fun) ->
    receive
    stop -> 
        io:format("Mapper ~p stopping~n", [self()]),
        ok;
    {data, D } ->
        data_async(Reducer,Fun(D)),
        mapper_loop(Reducer,Fun);
    { setup, F } ->
        mapper_loop(Reducer,F);
    _ ->
        mapper_loop(Reducer, Fun)
    end.

