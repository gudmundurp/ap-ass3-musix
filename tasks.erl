-module(tasks).
-import(mr).
-import(read_mxm).
-export(run_tasks/0).

run_tasks() ->
    {Words,_} = read_mxm:from_file("data/mxm_dataset_test.txt"),
    countWords(Words).

countWords(Words) ->
    {ok, MR} = mr:start(4),
    mr:job(MR,
           fun( Word ) ->
	       1,
	   fun( _, Count ) ->
	       Count+1,
	   0,
	   Words
    ),
    mr:stop().
