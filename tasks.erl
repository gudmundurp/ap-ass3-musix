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
        
    WordArr = array:from_list(Words),
    
    Grep = get_grepper( WordArr, Tracks, MR ),
    GrepCount = fun(Word) -> TList = Grep(Word), length(TList) end, 

    io:format("Number of tracks with word 'i': ~p~n",[GrepCount("i")]),
    io:format("Number of tracks with word 'love' ~p~n",[GrepCount("love")]),
    io:format("Number of tracks with word 'hate' ~p~n",[GrepCount("hate")]),
    io:format("Number of tracks with word 'god': ~p~n",[GrepCount("god")]),
    io:format("Number of tracks with word 'satan': ~p~n",[GrepCount("satan")]),
    mr:stop(MR).

