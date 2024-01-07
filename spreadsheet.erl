%RICORDARSI DI FARE IL CONTROLLO DI TUTTI GLI ERRORI
%NON MI RICORDO COME SI FA ZIOBONO

% creo uno shop con un DB distrbuito (Mnesia DBMS)
-module(spreadsheet).

% per fare query complesse
-include_lib("stdlib/include/qlc.hrl").

-export([
    new/4
]).

% definisco i record che utilizzero'
%record che rappresenta foglio
-record(spreadsheet, {table,riga, colonna}).
%record che rappresenta owner del foglio
-record(owner,{foglio,pid}).

%FARE CONTROLLO ERRORI SE UNO DIGITA MALE O SE QUALCOSA NON VA A BUON FINE ETC
new(Name,N,M,K) ->
    % creo il DB solo in locale -> node()
    Nodelist = [node()],
    mnesia:create_schema(Nodelist),
    % faccio partire Mnesia
    mnesia:start(),
    % creo lo schema delle due tablelle del DB coi campi che prendo dai records
    SpreadsheetFields = record_info(fields, spreadsheet),
    OwnerFields = record_info(fields,owner),
    % NB il nome della tabella e' == al nome dei records che essa ospita
    % specifico i parametri opzionali per avere una copia del DB
    % su disco e in RAM anche nei nodi distribuiti
    mnesia:create_table(Name,[
        {attributes, SpreadsheetFields},
        {disc_copies, Nodelist},
        {type,bag}
    ]),
    mnesia:create_table(owner,[
        {attributes,OwnerFields},
        {disc_copies,Nodelist}]),
    %popolo il foglio con k tabelle di n righe e m colonne
    popola_foglio(Name,K,N,M),
    %creo una nuova tabella in cui dico che il nodo è proprietario del foglio (tabella)
    popola_owner_table(Name)
.

popola_owner_table(Name)->
    F = fun()->
        Data = #owner{foglio=Name,pid=self()},
        mnesia:write(Data)
        end,
        mnesia:transaction(F)
    .

crea_riga(Name,I,J,M) ->
    {Name,{I},{J},{crea_colonne(M)}}.


popola_foglio(Name,K, N,M) when K > 0, N > 0 ->
    Fila = fun(I) ->
        lists:map(fun(J) -> crea_riga(Name,I,J,M) end, lists:seq(0, N-1))
    end,
    Matrice = lists:flatmap(Fila, lists:seq(0, K-1)),
    Matrice,
    salva_in_mnesia(Name,Matrice)
    .

crea_colonne(M)->
    lists:duplicate(M, 0).

salva_in_mnesia(Name,Matrice) ->
    F = fun() ->
        lists:foreach(fun(Elem)-> mnesia:write(Name,Elem,write)end,Matrice)
    end,
mnesia:transaction(F)
.

