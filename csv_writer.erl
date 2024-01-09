-module(csv_writer).
-export([to_csv/2]).
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
    lists:foldl(fun({Foglio, Table, Riga, Colonna}, Acc) ->
                    FoglioStr = atom_to_list(Foglio),
                    TableStr = integer_to_list(Table),
                    
                    RigaStr = integer_to_list(Riga),
                    
                    ColonnaStr = lists:flatten(io_lib:format("~p", [Colonna])),
                    RowStr = FoglioStr ++ "," ++ TableStr ++ "," ++ RigaStr ++ "," ++ ColonnaStr ++ "\n",
                    Acc ++ RowStr
                end, "Foglio,Tabella,Riga,Colonna\n", Records).
