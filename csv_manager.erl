-module(csv_manager).
-export([to_csv/2,from_csv/1]).
-record(spreadsheet, {table, riga, colonne}).
% Funzione per ottenere il contenuto della tabella Mnesia e scriverlo su un file CSV
to_csv(TableName, FilePath) ->
    % Apri il file per la scrittura
    {ok, File} = file:open(FilePath, [write]),
    
    % Estrai i dati dalla tabella Mnesia
    Records = ets:tab2list(TableName),
    %io:format("questo è il mio record: ~p",[Records]),
    
    % Converti i dati in formato CSV
    CsvContent = records_to_csv(Records),
    
    % Scrivi il CSV nel file
    file:write(File, CsvContent),
    
    % Chiudi il file
    file:close(File).

% Funzione per convertire i record in formato CSV
records_to_csv(Records) ->
    lists:map(fun({Foglio, Table, Riga, Colonna}) ->
            Row = lists:join(",", [atom_to_list(Foglio), integer_to_list(Table), integer_to_list(Riga), io_lib:format("~p", [Colonna])]),
            Row ++"\n"
        end, Records).






remove_extension(FileName) ->
    Reverse = string:reverse(FileName),
    %E fatto brutto qui perchè c'è max 100, se mi viene in mente un altro modo ci rimetto mano
    SubString = string:sub_string(Reverse,5,100),
    Reverse2 = string:reverse(SubString),
    Reverse2.


from_csv(FilePath) ->
    SpreadsheetFields = record_info(fields, spreadsheet),
    TableName = remove_extension(FilePath),
    NodeList = [node()],
    mnesia:create_table(TableName, [
        {attributes, SpreadsheetFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    mnesia:wait_for_tables([TableName],5000),
    {ok, File} = file:open(FilePath, [read]),
    read_lines(File),
    file:close(File).

read_lines(File) ->
    case io:get_line(File, "") of
        eof ->
            ok;
        Line ->
            process_line(Line),
            read_lines(File)
    end.

process_line(Line) ->
    % Rimuove il carattere di newline dalla fine della riga
    TrimmedLine = string:trim(Line),
    NewLine = "[" ++ TrimmedLine ++ "]",
    Parse = fun(S) -> 
        {ok, Ts, _} = erl_scan:string(S),
        {ok, Result} = erl_parse:parse_term(Ts ++ [{dot,1} || element(1, lists:last(Ts)) =/= dot]),
        Result 
            end,
    Prova = Parse(NewLine),
    io:format("~p",[Prova]),
    [Col1|Tail1] = Prova,
    [Col2|Tail2] = Tail1,
    [Col3|Tail3] = Tail2,
    [Col4|_] = Tail3,
    F = fun() ->
        Data = #spreadsheet{table=Col2,riga=Col3,colonne=Col4},
                mnesia:write(Data)     
    end,
    mnesia:transaction(F),
    io:format("Col1:~p Col2:~p Col3:~p Col4:~p~n",[Col1,Col2,Col3,Col4])
.
