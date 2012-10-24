-module(task1).
-import(mr).
-import(read_mxm).
-export([run_tasks/0]).

run_tasks() ->
    {_,Tracks} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    {ok, MR} = mr:start(4),
    {ok, C} = mr:job(MR,
        fun(Track) ->
	    {_, _, WordList} = read_mxm:parse_track(Track),
	    lists:foldl(fun({_,X}, Sum) -> X + Sum end, 0, WordList)
	    end,
	fun(WordCount, Count) -> Count+WordCount end,
	    0,
	    Tracks
	),
    io:format("Total number of words: ~p~n",[C]),	

    mr:stop(MR).

