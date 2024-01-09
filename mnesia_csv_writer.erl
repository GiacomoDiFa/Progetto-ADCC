-module(mnesia_csv_writer).
-export([write_mnesia_table_to_csv/2]).
% Funzione per ottenere il contenuto della tabella Mnesia e scriverlo su un file CSV
write_mnesia_table_to_csv(TableName, FilePath) ->
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
    lists:foldl(fun({Foglio, Table, Riga, Colonna}, Acc) ->
                    FoglioStr = atom_to_list(Foglio),
                    TableStr = tuple_to_list(Table),
                    [TableInt|_] = TableStr,
                    RigaStr = tuple_to_list(Riga),
                    [RigaInt|_] = RigaStr,
                    ColonnaStr = lists:flatten(io_lib:format("~p", [Colonna])),
                    RowStr = FoglioStr ++ "," ++ integer_to_list(TableInt) ++ "," ++ integer_to_list(RigaInt) ++ "," ++ ColonnaStr ++ "\n",
                    Acc ++ RowStr
                end, "Foglio,Tabella,Riga,Colonna\n", Records).
