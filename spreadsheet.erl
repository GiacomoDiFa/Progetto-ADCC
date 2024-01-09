%RICORDARSI DI FARE IL CONTROLLO DI TUTTI GLI ERRORI
%NON MI RICORDO COME SI FA ZIOBONO

% creo uno shop con un DB distrbuito (Mnesia DBMS)
-module(spreadsheet).

% per fare query complesse
-include_lib("stdlib/include/qlc.hrl").

-export([
    create_table/0,
    new/4,
    set_access_mode/2,
    get_access_mode/1,
    share/2,
    delete_close_tables/1,
    create_reader/2
]).

% definisco i record che utilizzero'
%record che rappresenta foglio
-record(spreadsheet, {table, riga, colonne}).
%record che rappresenta owner del foglio
-record(owner, {foglio, pid}).
%record che rappresenta le policy
-record(policy, {pid, foglio, politica}).

%PRIMA FARLO IN LOCALE POI NEL DISTRIBUITO (FILE SHOP_DB MATTE)
%FARE CONTROLLO ERRORI SE UNO DIGITA MALE O SE QUALCOSA NON VA A BUON FINE ETC
create_table() ->
    % creo il DB solo in locale -> node()
    NodeList = [node()],
    mnesia:create_schema(NodeList),
    % faccio partire Mnesia
    mnesia:start(),
    % creo lo schema delle due tablelle del DB coi campi che prendo dai records
    %SpreadsheetFields = record_info(fields, spreadsheet),
    OwnerFields = record_info(fields, owner),
    PolicyFields = record_info(fields, policy),
    % NB il nome della tabella e' == al nome dei records che essa ospita
    % specifico i parametri opzionali per avere una copia del DB
    % su disco e in RAM anche nei nodi distribuiti
    %mnesia:create_table(Name, [
    %    {attributes, SpreadsheetFields},
    %    {disc_copies, NodeList},
    %    {type, bag}
    %]),
    mnesia:create_table(owner, [
        {attributes, OwnerFields},
        {disc_copies, NodeList}
    ]),
    mnesia:create_table(policy, [
        {attributes, PolicyFields},
        {disc_copies, NodeList},
        {type, bag}
    ])
    %popolo il foglio con k tabelle di n righe e m colonne
    %popola_foglio(Name, K, N, M),
    %creo una nuova tabella in cui dico che il nodo è proprietario del foglio (tabella)
    %popola_owner_table(Name)
.

% devo aver gia' creato lo schema
new(TabName, N, M, K) ->
    % creo lo schema delle due tablelle del DB coi campi che prendo dai records
    SpreadsheetFields = record_info(fields, spreadsheet),
    %OwnerFields = record_info(fields, owner),
    %PolicyFields = record_info(fields, policy),
    
    % NB il nome della tabella e' == al nome dei records che essa ospita
    % specifico i parametri opzionali per avere una copia del DB
    % su disco e in RAM anche nei nodi distribuiti
    NodeList = mnesia:table_info(owner, disc_copies),
    mnesia:create_table(TabName, [
        {attributes, SpreadsheetFields},
        {disc_copies, NodeList},
        {type, bag},
        {access_mode, read_only}
    ]),
    %popolo il foglio con k tabelle di n righe e m colonne
    popola_foglio(TabName, K, N, M),
    %creo una nuova tabella in cui dico che il nodo è proprietario del foglio (tabella)
    popola_owner_table(TabName)
.

get_access_mode(TabName) -> mnesia:table_info(TabName, access_mode).

set_access_mode(TabName, AccessMode) -> mnesia:change_table_access_mode(TabName, AccessMode).

%DA TENERE PER IL DEBUG (EVENTUALMENTE TOGLIERE ALLA FINE)
delete_close_tables(NameList) -> 
    lists:foreach(fun(T) -> mnesia:delete_table(T) end, NameList),
    mnesia:delete_table(owner),
    mnesia:delete_table(policy),
    mnesia:stop()
.

%MAGARI FARE CONTROLLO ERRORI SU TRANSAZIONI MNESIA
popola_owner_table(Foglio)->
    F = fun()->
        Data = #owner{foglio=Foglio, pid=self()},
        mnesia:write(Data)
    end,
    mnesia:transaction(F),
    % aggiorno le policy
    F1 = fun() ->
            Data = #policy{pid=self(), foglio=Foglio, politica=write},
            mnesia:write(Data)
        end,
    mnesia:transaction(F1)
.

crea_riga(Name, I, J, M) -> {Name, I, J, crea_colonne(M)}.

crea_colonne(M)-> lists:duplicate(M, undef).

popola_foglio(Name, K, N, M) when K > 0, N > 0 ->
    Fila = fun(I) -> 
        lists:map(
            fun(J) -> 
                crea_riga(Name, I, J, M) 
            end, 
            lists:seq(0, N-1)
        )
    end,
    Matrice = lists:flatmap(Fila, lists:seq(0, K-1)),
    %Matrice,
    salva_in_mnesia(Name, Matrice)
.

salva_in_mnesia(Foglio, Matrice) ->
    F = fun() ->
        lists:foreach(
            fun(Elem)-> 
                mnesia:write(Foglio, Elem, write)
            end,
            Matrice
        ) 
    end,
    mnesia:transaction(F)
.



share(Foglio, AccessPolicies)->
    %AccessPolicies = {Proc,AP}
    %Proc = Pid
    %AP = read | write
    %controllo che share la chiami solo il proprietario della tabella
    {Proc, Ap} = AccessPolicies,
    F = fun() ->
        mnesia:read({owner, Foglio}) 
        end,
    {atomic, Result} = mnesia:transaction(F),
    case Result of
        %il foglio non esiste
        [] ->  
            io:format("Nessun risultato trovato per la chiave ~p.~n",[Foglio]),
            {error, sheet_not_found};
        %il foglio esiste
        [{owner, Foglio, Value}] -> 
            io:format("Risultato trovato: ~p -> ~p~n",[Foglio, Value]),
            %controllo che chi voglia condividere sia il proprietario del foglio
            case Value == self() of
                %non sono il proprietario
                false -> {error, not_the_owner};
                %sono il proprietario
                % HO GIA I PERMESSI DI SCRITTURA
                true -> 
                    Query = qlc:q([X || 
                        X <- mnesia:table(policy),
                        X#policy.pid =:= Proc,
                        X#policy.foglio =:= Foglio 
                    ]),
                    F2 = fun() ->
                        qlc:e(Query)
                            % mnesia:read({policy,Proc})
                        end,
                    %leggo se il pid e il foglio sono già presenti nella tabella
                    {atomic ,Result1} = mnesia:transaction(F2),
                    case Result1 of
                        %tabella "vuota" quindi posso scrivere
                        [] -> 
                            io:format("Nessun risultato trovato quindi posso scrivere"),
                            F3 = fun()->
                                    Data = #policy{pid=Proc, foglio=Foglio, politica=Ap},
                                    mnesia:write(Data)
                                end,
                            mnesia:transaction(F3);
                        %elemento già scritto quindi devo prima eliminarlo e poi risalvarlo
                        [{policy, PidTrovato, FoglioTrovato, PolicyTrovato}] ->
                            io:format("Risultato trovato: ~p -> ~p ~p ~n", [PidTrovato, FoglioTrovato, PolicyTrovato]),
                            F4 = fun() ->
                                    mnesia:delete_object({policy, PidTrovato, FoglioTrovato, PolicyTrovato})
                                end,
                            mnesia:transaction(F4),
                            %scrivere nella tabella le policy
                            F3 = fun()->
                                    Data = #policy{pid=Proc, foglio=Foglio, politica=Ap},
                                    mnesia:write(Data)
                                end,
                            mnesia:transaction(F3)
                    end
            end             
    end
.

%DEBUG
create_reader(Foglio, Pid) ->
    spawn(fun() ->
        receive
            {start} ->
                %QUI MANCA IL CONTROLLO DEGLI ACCESSI
                F = fun() ->
                    mnesia:read({Foglio, 0}) 
                end,
                {atomic, Result} = mnesia:transaction(F),
                Pid!{read_result, Result}
        end
    end)
.