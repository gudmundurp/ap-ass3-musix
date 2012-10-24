-module(tasks).
-import(mr).
-import(read_mxm).
-export([run_tasks/0]).

take(List, N) -> take(List, N, []).

take([H|T], N, Acc) when N > 0 ->
    take(T, N-1, [H|Acc]);
take(_, 0, Acc) -> lists:reverse(Acc).

countWords(Words) ->
    io:format("Length of Words is ~p~n",[length(Words)]),
    {ok, MR} = mr:start(4),
    {ok, Res} = mr:job(MR,
        fun( Word ) -> Word end,
	    fun( _, Count ) -> Count+1 end,
	    0,
	    Words
	),
    mr:stop(MR),
    Res.

run_tasks() ->
    {Words,_} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    countWords(Words).
