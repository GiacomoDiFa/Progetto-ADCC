% modulo di debug
-module(debug).

-export([
    delete_close_tables/1,
    spawn_reader/2,
    spawn_writer/2
]).

delete_close_tables(NameList) -> 
    lists:foreach(fun(T) -> mnesia:delete_table(T) end, NameList),
    mnesia:delete_table(owner),
    mnesia:delete_table(policy),
    mnesia:stop()
.

spawn_reader(Foglio, Pid) ->
    spawn(fun() ->
        receive
            {start} ->
                Result = spreadsheet:get(Foglio, 1,1,1),
                Pid!{read_result, Result};
            Msg -> {error, Msg}
        end
    end)
.

spawn_writer(Foglio, Pid) ->
    spawn(fun() ->
        receive
            {start} ->
                Result = spreadsheet:set(Foglio, 1,1,1, 'WRITE'),
                Pid!{write_result, Result};
            Msg -> {error, Msg}
        end
    end)
.