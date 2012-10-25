-module(task1).
-import(mr).
-import(read_mxm).
-export([run_tasks/0, append_to_dict/3]).

append_to_dict(_, [], Dict) ->
    Dict;
append_to_dict(TrackId, [W1 |Words], Dict) ->
    NewDict = dict:append(W1, TrackId, Dict),
    append_to_dict(TrackId, Words, NewDict).

run_tasks() ->
    % TASK 1
    {_,Tracks} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    {T1,T2} = lists:split(10,Tracks),
    {ok, MR} = mr:start(4),

    % Task 4
    {ok, WtoS} = mr:job(MR,
        fun(Track) ->
	    {TrackId, _, WordList} = read_mxm:parse_track(Track),
	    WordIds = lists:flatmap(fun({WordId,_}) -> [WordId] end, WordList),
	    {TrackId, WordIds}
	end,
	fun({TrackId, WordIds}, WordMap) ->
	   %io:format("Song id: ~p~n", [TrackId]),
	   append_to_dict(TrackId, WordIds, WordMap)
	end,
	dict:new(),
	T1
	),
    io:format("DONE", []),
    io:format("dict contains ~p~n", [length(dict:fetch(2,WtoS))]),

    mr:stop(MR).

