%RICORDARSI DI FARE IL CONTROLLO DI TUTTI GLI ERRORI
%NON MI RICORDO COME SI FA ZIOBONO

% creo uno shop con un DB distrbuito (Mnesia DBMS)
-module(spreadsheet).

% per fare query complesse
-include_lib("stdlib/include/qlc.hrl").

-export([
    new/4,
    share/2
]).

% definisco i record che utilizzero'
%record che rappresenta foglio
-record(spreadsheet, {table,riga, colonna}).
%record che rappresenta owner del foglio
-record(owner,{foglio,pid}).
%record che rappresenta le policy
-record(policy,{pid,foglio,politica}).

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
    PolicyFields = record_info(fields,policy),
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
    mnesia:create_table(policy,[
        {attributes,PolicyFields},
        {disc_copies,Nodelist},
        {type,bag}]),
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





share(Foglio,AccessPolicies)->
%AccessPolicies = {Proc,AP}
%Proc = Pid
%AP = read | write
%controllo che share la chiami solo il proprietario della tabella
{Proc,Ap} = AccessPolicies,
F = fun() ->
    mnesia:read({owner,Foglio}) 
    end,
{atomic,Result} = mnesia:transaction(F),
case Result of
    %il foglio non esiste
    [] ->  io:format("Nessun risultato trovato per la chiave ~p.~n",[Foglio]);
    %il foglio esiste
    [{owner,Foglio,Value}] -> io:format("Risultato trovato: ~p -> ~p~n",[Foglio,Value]),
                            %controllo che chi voglia condividere sia il proprietario del foglio
                            case Value == self() of
                                %non sono il proprietario
                                false -> error;
                                %sono il proprietario
                                true -> io:format("i pid sono uguali"),
                                        F2 = fun()->
                                                        mnesia:read({policy,Proc})
                                            end,
                                        %leggo se il foglio è dentro la tabella 
                                        %NB: DA QUI IN POI FUNZIONA BENE SOLO SE I PID SONO IN CONDIVISIONE SU UN UNICO FOGLIO, ALTRIMENTI NON FUNZIONA BENE
                                        %BISOGNA ANCORA LAVORARCI PERCHè NON C'è UN METODO CHE LEGGE UNA SINGOLA RIGA IN UNA TABELLA PURTROPPO
                                        %MA LEGGE OGNI CHIAVE PRESENTE IN QUELLA TABELLA
                                        %QUINDI CI VUOLE UN ALTRO METODO CHE "FILTRA" LA LISTA RISULTATO PER VEDERE SE IL PID C'è GIA (O UNA ROBA DEL GENERE)
                                        {atomic,Result1} = mnesia:transaction(F2),
                                        case Result1 of
                                            %tabella "vuota" quindi posso scrivere
                                            [] -> io:format("Nessun risultato trovato quindi posso scrivere"),
                                                  F3 = fun()->
                                                              Data = #policy{pid=Proc,foglio=Foglio,politica=Ap},
                                                              mnesia:write(Data)
                                                        end,
                                                  mnesia:transaction(F3);
                                            %elemento già scritto quindi devo prima eliminarlo e poi risalvarlo
                                            [{policy,PidTrovato,FoglioTrovato,PolicyTrovato}] ->io:format("Risultato trovato: ~p -> ~p ~p ~n",[PidTrovato,FoglioTrovato,PolicyTrovato]),
                                                                                                F4 = fun() ->
                                                                                                             mnesia:delete_object({policy,PidTrovato,FoglioTrovato,PolicyTrovato})
                                                                                                     end,
                                                                                                mnesia:transaction(F4),
                                                                                                %scrivere nella tabella le policy
                                                                                                F3 = fun()->
                                                                                                            Data = #policy{pid=Proc,foglio=Foglio,politica=Ap},
                                                                                                            mnesia:write(Data)
                                                                                                     end,
                                                                                                mnesia:transaction(F3)
                                        end
                            end             
end
.
