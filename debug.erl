% modulo di debug
-module(debug).

-export([
    delete_close_tables/0,
    spawn_reader/2,
    spawn_writer/2
]).

delete_close_tables() ->
    TabelleLocali = mnesia:system_info(tables), 
    lists:foreach(fun(T) -> mnesia:delete_table(T) end, TabelleLocali),
    %mnesia:delete_table(owner),
    %mnesia:delete_table(policy),
    %mnesia:delete_table(format),
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