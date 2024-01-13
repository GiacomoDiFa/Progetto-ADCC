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

loop_call_test_reader(Foglio,Pid) ->
    receive
            {start} ->
                Result = spreadsheet:get(Foglio, 1,1,1),
                Pid!{read_result, Result}, loop_call_test_reader(Foglio,Pid);
            {stop} -> Pid!{stop,node()}
        end
.

spawn_reader(Foglio, Pid) ->
    spawn(fun() ->
        loop_call_test_reader(Foglio,Pid)
    end)
.

loop_call_test_writer(Foglio,Pid) ->
    receive
            {start} ->
                Result = spreadsheet:set(Foglio, 1,1,1, 'WRITE'),
                Pid!{write_result, Result}, loop_call_test_writer(Foglio,Pid);
            {stop} -> Pid!{stop,node()}
        end
.

spawn_writer(Foglio, Pid) ->
    spawn(fun() ->
        loop_call_test_writer(Foglio,Pid)
    end)
.