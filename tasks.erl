-module(tasks).
-import(mr).
-import(read_mxm).
-export([run_tasks/0]).
	
run_tasks() ->
    {Words,Tracks} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    
	{ok, MR} = mr:start(4),
	
    {ok, C} = mr:job(MR,
        fun( Word ) -> Word end,
	    fun( _, Count ) -> Count+1 end,
	    0,
	    Words
	),
    io:format("Total number of words: ~p~n",[C]),

	L = length(Tracks),
	io:format("Total number of tracks: ~p~n",[L]),
    {ok,Av_diff_song} = mr:job(MR,
	    fun( Track ) -> 
		    {_,_,WordList} = read_mxm:parse_track(Track),
			length(WordList)
		end,
		fun( Length, Acc ) -> Acc+Length/L end,
		0,
		Tracks
	),
	io:format("Average different words in a song: ~p~n",[Av_diff_song]),
	
	Av_total_song = C/L,
	io:format("Average total nr. of words in a song: ~p~n",[Av_total_song]),
	
	
	
	mr:stop(MR).

