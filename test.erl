-module(test).
-export([test_sum/0]).
-import(mr).
test_sum() ->
    {ok, MR} = mr:start(3),
    {ok, Sum} = mr:job(MR,
        fun(X) -> X end,
        fun(X,Acc) -> X+Acc end,
        0,
        lists:seq(1,10)),
    {ok, Fac} = mr:job(MR,
        fun(X) -> X end,
        fun(X,Acc) -> X*Acc end,
        1,
        lists:seq(1,10)),
    mr:stop(MR),
    {Sum, Fac}.
