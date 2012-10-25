-module(tasks).
-import(mr).
-import(read_mxm).
-export([run_tasks/0]).

get_grepper( WordArr,Tracks,MR ) ->
    fun(Word) ->
        {ok,IDs} = mr:job(MR,
            fun( Track ) -> 
                {TrackID,_,WordList} = read_mxm:parse_track(Track),
                { TrackID, lists:foldl(fun({W,_},Acc) -> Acc or (array:get(W-1,WordArr) == Word) end, false, WordList) }
            end,
            fun( {TrackID, Exists}, Acc ) ->
                if 
                    Exists     -> [TrackID | Acc];
                    not Exists -> Acc
                end
            end,
            [],
            Tracks
        ),
        IDs
    end.

append_to_dict(_, [], Dict) ->
    Dict;
append_to_dict(TrackId, [W1 |Words], Dict) ->
    NewDict = dict:append(W1, TrackId, Dict),
    append_to_dict(TrackId, Words, NewDict).

run_tasks() ->
    {Words,Tracks} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    
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
    io:format("Average total number of words in a song: ~p~n", [C/L]),
        
    WordArr = array:from_list(Words),
    
    Grep = get_grepper( WordArr, Tracks, MR ),
    GrepCount = fun(Word) -> TList = Grep(Word), length(TList) end, 

    io:format("Number of tracks with word 'i': ~p~n",[GrepCount("i")]),
    io:format("Number of tracks with word 'love' ~p~n",[GrepCount("love")]),
    io:format("Number of tracks with word 'satan': ~p~n",[GrepCount("satan")]),
    io:format("Number of tracks with word 'and': ~p~n",[GrepCount("and")]),
    io:format("Number of tracks with word 'hate' ~p~n",[GrepCount("hate")]),
    io:format("Number of tracks with word 'god': ~p~n",[GrepCount("god")]),
    
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
	Tracks),

    io:format("Number of tracks with word 'i' according to the reverse index: ~p~n", [length(dict:fetch(1,WtoS))]),

    mr:stop(MR).

