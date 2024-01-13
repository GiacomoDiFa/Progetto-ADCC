-module(csv_manager).
-export([to_csv/2,from_csv/1]).
-record(spreadsheet, {table, riga, colonne}).
% Funzione per ottenere il contenuto della tabella Mnesia e scriverlo su un file CSV
% FileName := "nome_tabella.csv"
to_csv(TableName, FileName) ->
    %Controllo che table name sia nel mio mnesia
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(TableName, TabelleLocali) of
        false -> {error, invalid_name_of_table};
        true ->
            % Apri il file per la scrittura
            {ok, File} = file:open(FileName, [write]),
            % Estrai i dati dalla tabella Mnesia
            Records = ets:tab2list(TableName),
            %io:format("questo è il mio record: ~p",[Records]),
            % Converti i dati in formato CSV
            CsvContent = records_to_csv(Records),
            % Scrivi il CSV nel file
            file:write(File, CsvContent),
            % Chiudi il file
            file:close(File)
    end
.

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
    Final = string:reverse(SubString),
    Final.

%QUA SI POTREBBE AGGIUNGERE IL FATTO DI CONTROLLARE LA PRIMA PAROLA DEL FILE CSV CON UN NOME DELLE TABELLE PER L'UTENTE
%STUPIDO, PERCHè ALTRIMENTI QUESTO CODICE SE HO UN FILE DEL FOGLIO1 CHIAMATO CIAO.CSV MI CREA UNA TABELLA CHIAMATA CIAO VUOTA
%AVEVO PROVATO A FARLO MA NON ERO RIUSCITO, è DA RIPROVARCI
from_csv(FilePath) ->
    SpreadsheetFields = record_info(fields, spreadsheet),
    TableName = remove_extension(FilePath),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(list_to_atom(TableName),TabelleLocali) of
        true -> {error, table_is_already_in_mnesia};
        false ->
            NodeList = [node()],
            mnesia:create_schema(NodeList),
            mnesia:create_table(list_to_atom(TableName), [
                {attributes, SpreadsheetFields},
                {disc_copies, NodeList},
                {type, bag}]),
                {ok, File} = file:open(FilePath, [read]),
                read_lines(File),
                file:close(File)
    end
.

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
    %io:format("~p",[Prova]),
    [Col1|Tail1] = Prova,
    [Col2|Tail2] = Tail1,
    [Col3|Tail3] = Tail2,
    [Col4|_] = Tail3,
    F = fun() ->
        Data = {Col1,Col2,Col3,Col4},
                mnesia:write(Data)     
    end,
    Result = mnesia:transaction(F),
    case Result of
        {aborted,Reason} -> {error,Reason};
        {atomic,Res} -> Res
    end
.
