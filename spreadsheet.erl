%RICORDARSI DI FARE IL CONTROLLO DI TUTTI GLI ERRORI
%NON MI RICORDO COME SI FA ZIOBONO

% creo uno shop con un DB distrbuito (Mnesia DBMS)
-module(spreadsheet).

% per fare query complesse
-include_lib("stdlib/include/qlc.hrl").

-export([
    create_table/0,
    new/1,
    new/4,
    share/2,
    get/4,
    set/5,
    info/1
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
    % controllo che TabName non sia gia' presente
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(TabName, TabelleLocali) of
        true -> {error, invalid_name};
        false ->
            % creo lo schema delle due tablelle del DB coi campi che prendo dai records
            SpreadsheetFields = record_info(fields, spreadsheet),
            %OwnerFields = record_info(fields, owner),
            %PolicyFields = record_info(fields, policy),
            
            % NB il nome della tabella e' == al nome dei records che essa ospita
            % specifico i parametri opzionali per avere una copia del DB
            % su disco e in RAM anche nei nodi distribuiti
            NodeList = [node()], %mnesia:table_info(owner, disc_copies),
            mnesia:create_table(TabName, [
                {attributes, SpreadsheetFields},
                {disc_copies, NodeList},
                {type, bag}
                %{index, [#spreadsheet.riga]}
            ]),
            %popolo il foglio con k tabelle di n righe e m colonne
            popola_foglio(TabName, K, N, M),
            %creo una nuova tabella in cui dico che il nodo è proprietario del foglio (tabella)
            popola_owner_table(TabName)
    end
.

new(TabName) -> spreadsheet:new(TabName, 10, 10, 10).

%MAGARI FARE CONTROLLO ERRORI SU TRANSAZIONI MNESIA
popola_owner_table(Foglio)->
    F = fun()->
        Data = #owner{foglio=Foglio, pid=self()},
        mnesia:write(Data)
    end,
    {atomic, ok} = mnesia:transaction(F),
    % aggiorno le policy
    F1 = fun() ->
            Data = #policy{pid=self(), foglio=Foglio, politica=write},
            mnesia:write(Data)
        end,
    {atomic, ok} = mnesia:transaction(F1)
.

crea_riga(TabName, I, J, M) -> {TabName, I, J, crea_colonne(M)}.

crea_colonne(M)-> lists:duplicate(M, undef).

popola_foglio(Name, K, N, M) when K > 1, N > 1, M > 1 ->
    Fila = fun(I) -> 
        lists:map(
            fun(J) -> 
                crea_riga(Name, I, J, M) 
            end, 
            lists:seq(1, N)
        )
    end,
    Matrice = lists:flatmap(Fila, lists:seq(1, K)),
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

% SERVE il controllo della policy ANCHE in lettura
% perche' il foglio puo' essere letto solo se condiviso
get(SpreadSheet, TableIndex, I, J) ->
    MioPid = self(),
    PolicyQuery = qlc:q(
        % list comprehension
        [ X#policy.politica ||
            % seleziona tutte righe tabella shop
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.foglio == SpreadSheet
        ]
    ),
    % invoco la query dentro una transazione e ritorno il risultato
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    %io:format(">>>~p<<<", [Result]),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] ->
                    % solo se il file e' condiviso Policy e' popolata
                    Condition = (Policy == read) or (Policy == write), 
                    case Condition of
                        false -> {error, not_allowed};
                        true ->
                            TakeRowQuery = qlc:q(
                                % list comprehension
                                [ X#spreadsheet.colonne ||
                                    % seleziona tutte righe tabella shop
                                    X <- mnesia:table(SpreadSheet),
                                    X#spreadsheet.table == TableIndex,
                                    X#spreadsheet.riga == I
                                ]
                            ),
                            % invoco la query dentro una transazione e ritorno il risultato
                            Fun1 = fun() -> qlc:e(TakeRowQuery) end,
                            Result1 = mnesia:transaction(Fun1),
                            case Result1 of
                                {aborted, Reason1} -> {error, Reason1};
                                {atomic, []} -> {error, not_found};
                                {atomic, [RigaI]} -> lists:nth(J, RigaI);
                                Msg -> {error, {unknown, Msg}}
                            end
                    end
            end
    end
.

% AGGIUNGERE I CONTROLLI
set(SpreadSheet, TableIndex, I, J, Value) ->
    MioPid = self(),
    PolicyQuery = qlc:q(
        % list comprehension
        [ X#policy.politica ||
            % seleziona tutte righe tabella shop
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.foglio == SpreadSheet
        ]
    ),
    % invoco la query dentro una transazione e ritorno il risultato
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    %io:format(">>>~p<<<", [Result]),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] -> 
                    case Policy == write of
                        false -> {error, not_allowed};
                        true ->
                            TakeColumnQuery = qlc:q(
                                % list comprehension
                                [ X#spreadsheet.colonne ||
                                    % seleziona tutte righe tabella shop
                                    X <- mnesia:table(SpreadSheet),
                                    X#spreadsheet.table == TableIndex,
                                    X#spreadsheet.riga == I
                                ]
                            ),
                            % invoco la query dentro una transazione e ritorno il risultato
                            Fun1 = fun() -> qlc:e(TakeColumnQuery) end,
                            Result1 = mnesia:transaction(Fun1),
                            case Result1 of
                                {aborted, Reason1} -> {error, Reason1};
                                {atomic, Res1} ->
                                    case Res1 of
                                        [] -> {error, not_found};
                                        [RigaI] -> 
                                            {L1, L2} = lists:split(J, RigaI),
                                            L1WithoutLast = lists:droplast(L1),
                                            FinalRow = L1WithoutLast ++ [Value] ++ L2,
                                            F2 = fun() ->
                                                Record = {SpreadSheet,
                                                    TableIndex, 
                                                    I,
                                                    % prendo il record di prima e lo elimino
                                                    RigaI},  
                                                mnesia:delete_object(Record),
                                                NewRecord = {SpreadSheet, TableIndex, I, FinalRow},
                                                mnesia:write(NewRecord)
                                            end,
                                            {atomic, ok} = mnesia:transaction(F2), ok;
                                        Msg -> {error, {unknown, Msg}}
                                    end
                            end
                    end;
                Msg1 -> {error, {unknown, Msg1}} 
            end
    end
.

share(Foglio, AccessPolicies)->
    %AccessPolicies = {Proc,AP}
    %Proc = Pid
    %AP = read | write
    %controllo che share la chiami solo il proprietario della tabella
    {Proc, Ap} = AccessPolicies,
    Condition = (Ap == read) or (Ap == write),
    case Condition of
        false -> {error, wrong_policy_format};
        true -> 
            F = fun() ->
                mnesia:read({owner, Foglio}) 
                end,
            Result = mnesia:transaction(F),
            case Result of
                {aborted, Reason} -> {error, Reason};
                {atomic, Res} ->
                    case Res of
                        %il foglio non esiste
                        [] ->  
                            %io:format("Nessun risultato trovato per la chiave ~p.~n",[Foglio]),
                            {error, sheet_not_found};
                        %il foglio esiste
                        [{owner, Foglio, Value}] -> 
                            %io:format("Risultato trovato: ~p -> ~p~n",[Foglio, Value]),
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
                                    Result1 = mnesia:transaction(F2),
                                    case Result1 of
                                        {aborted, Reason1} -> {error, Reason1};
                                        {atomic, Res1} ->
                                            case Res1 of
                                                %tabella "vuota" quindi posso scrivere
                                                [] -> 
                                                    %io:format("Nessun risultato trovato quindi posso scrivere"),
                                                    F3 = fun()->
                                                            Data = #policy{pid=Proc, foglio=Foglio, politica=Ap},
                                                            mnesia:write(Data)
                                                        end,
                                                    Result2 = mnesia:transaction(F3),
                                                    case Result2 of
                                                        {aborted, Reason2} -> {error, Reason2};
                                                        {atomic, _} -> ok
                                                    end;
                                                [{policy, PidTrovato, FoglioTrovato, PolicyTrovato}] ->
                                                    %elemento già scritto quindi devo prima eliminarlo e poi risalvarlo
                                                    %io:format("Risultato trovato: ~p -> ~p ~p ~n", [PidTrovato, FoglioTrovato, PolicyTrovato]),
                                                    F4 = fun() ->
                                                            mnesia:delete_object({policy, PidTrovato, FoglioTrovato, PolicyTrovato})
                                                        end,
                                                    Result4 = mnesia:transaction(F4),
                                                    case Result4 of
                                                        {aborted, Reason4} -> {error, Reason4};
                                                        {atomic, _} -> 
                                                            %scrivere nella tabella le policy
                                                            F3 = fun()->
                                                                    Data = #policy{pid=Proc, foglio=Foglio, politica=Ap},
                                                                    mnesia:write(Data)
                                                                end,
                                                            Result3 = mnesia:transaction(F3),
                                                            case Result3 of
                                                                {aborted, Reason3} -> {error, Reason3};
                                                                {atomic, _} -> ok
                                                            end
                                                    end;
                                                Msg -> {error, {unknown, Msg}}
                                            end 
                                    end
                            end;
                        Msg2 -> {error, {unknown, Msg2}} 
                    end             
            end
    end
.

info(Foglio) ->
    % controllo che Foglio sia gia' presente
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(Foglio, TabelleLocali) of
        false -> {error, not_exist};
        true ->
            QueryScrittura = qlc:q([X#policy.pid || 
                X <- mnesia:table(policy),
                X#policy.foglio == Foglio,
                X#policy.politica == write]),
            FScrittura = fun() -> qlc:e(QueryScrittura) end,
            
            ResultScrittura = mnesia:transaction(FScrittura),
            case ResultLettura of
                {aborted, Reason} -> {error, Reason};
                {atomic, ResLettura} ->
                    case ResLettura of
                        %tabella "vuota" quindi posso scrivere
                        [] -> {error, policy_table_empty};
                        [{policy, PidTrovato, FoglioTrovato, PolicyTrovata}] ->
                            case PolicyTrovata == read of
                                true -> ListaPermessiLettura ++ [{PidTrovato, FoglioTrovato}];
                                false ->
                                    case PolicyTrovata == write of
                                        true -> ListaPermessiScrittura ++ [{PidTrovato, FoglioTrovato}];
                                        false -> {error, wrong_policy_format}
                                    end
                            end;
                        Msg -> {error, {unknown, Msg}}
                    end
            end,
            ListaPermessiLettura = mnesia:foldl(
                fun(Elem, Acc) ->
                    {policy, PidTrovato, FoglioTrovato, PolicyTrovata} = Elem,
                    {PidTrovato, PolicyTrovata}    
                end,
                [],
                policy
            ),
            ListaPermessi = {policy_list, 
                {read, ListaPermessiLettura}, 
                {write, ListaPermessiScrittura}
            },
            
            
    end
.

celle_per_foglio(Foglio) ->
    % numero di celle per il foglio corrente
    Fun = mnesia:foldl(
        fun(_Elem, Acc) ->
            Acc + 1    
        end,
        0,
        Foglio
    ),
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} -> Res
    end
.

lista_lettura(Foglio) ->
    % ottengo i permessi di lettura e scrittura
    QueryLettura = qlc:q([X#policy.pid || 
        X <- mnesia:table(policy),
        X#policy.foglio == Foglio,
        X#policy.politica == read]),
    FLettura = fun() -> qlc:e(QueryLettura) end,
    ResultLettura = mnesia:transaction(FLettura),
    case ResultLettura of
        {aborted, Reason} -> {error, Reason};
        {atomic, ResLettura} ->
            case ResLettura of
                %tabella "vuota" quindi posso scrivere
                [] -> {error, policy_table_empty};
                [{policy, PidTrovato, FoglioTrovato, PolicyTrovata}]
    