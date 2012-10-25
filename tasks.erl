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
    io:format("Number of tracks with word 'god': ~p~n",[GrepCount("god")]),
    io:format("Number of tracks with word 'satan': ~p~n",[GrepCount("satan")]),
    %io:format("Number of tracks with word 'good' ~p~n",[GrepCount("good")]),
    %io:format("Number of tracks with word 'evil' ~p~n",[GrepCount("evil")]),
    %io:format("Number of tracks with word 'love' ~p~n",[GrepCount("love")]),
    %io:format("Number of tracks with word 'hate' ~p~n",[GrepCount("hate")]),

        {ok,Dictionary} = mr:job(MR,
        fun( WordIndex ) ->
            io:format("At word index ~p~n",[WordIndex]),
            TList = 
            lists:foldl(fun(Track,TList) ->
                    {TrackID,_,WordList} = read_mxm:parse_track(Track),
                        
                %io:format("At Track ~p~n",[TrackID]),

            Exists = lists:any(
                fun(W) ->
                              element(1,W) == WordIndex end,
                              WordList
            ),
                if 
              ( Exists ) -> [Track|TList];
                  true       -> TList
                end
              end,
              [],
              Tracks
                    ),
        {WordIndex,Tracks}
        end,
            fun( {WordIndex, Tracks}, Dict ) ->
            dict:store(array:get(WordIndex-1,WordArr), Tracks, Dict)
            end,
        dict:new(),
        lists:seq(1,length(Words))
    ),
    io:format("Dictionary computed.~n"),

    GetTC = fun( Word ) ->
        length(dict:get(Word,Dictionary))
    end,
    io:format("Number of tracks with word 'god': ~p~n",[GetTC("god")]),
    io:format("Number of tracks with word 'satan': ~p~n",[GetTC("satan")]),
    io:format("Number of tracks with word 'good' ~p~n",[GetTC("good")]),
    io:format("Number of tracks with word 'evil' ~p~n",[GetTC("evil")]),
    io:format("Number of tracks with word 'love' ~p~n",[GetTC("love")]),
    io:format("Number of tracks with word 'hate' ~p~n",[GetTC("hate")]),

    %{ok,Av_total_song} = mr:job(MR,
    %    fun( Track ) -> 
    %       {_,_,WordList} = read_mxm:parse_track(Track),
    %       io:format("About to call foldl~n"),
    %       Ans=list:foldl(fun({_,C},Sum)->Sum+C end,0,WordList),
    %       io:format("Done with MapFun, Ans was: ~p~n",[Ans]),
    %       Ans
    %   end,
    %   fun( Sum, Acc ) -> io:format("Sum is ~p, Acc is ~p~n",[Sum,Acc]),Acc+Sum/L end,
    %   0,
    %   Tracks
    %),
    %io:format("Average total nr. of words in a song: ~p~n",[Av_total_song]),
    
    
    
    mr:stop(MR).

