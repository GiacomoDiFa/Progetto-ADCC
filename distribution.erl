% modulo per gestire la distribuzione nei nodi
% GESTISCE ANCHE IL CASO IN LOCALE
-module(distribution).

-export([
    create_table/0,
    create_table_distrib/1,
    start/0,
    start_distrib/1,
    stop/0,
    stop_distrib/1,
    save_name/1,
    lookup_name/1
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% meglio se faccio un file records.hrl ???
% definisco i record che utilizzero'
%record che rappresenta foglio
%-record(spreadsheet, {table, riga, colonne}).
%record che rappresenta owner del foglio
-record(owner, {foglio, pid}).
%record che rappresenta le policy
-record(policy, {pid, foglio, politica}).
%record che rappresenta il formato dei fogli (BAG)
-record(format, {foglio, tab_index, nrighe, ncolonne}).

% LOCALE
% da chiamare SOLO UNA VOLTA per creare il DB Mnesia LOCALE
% serve per creare i file del DB sui vari nodi (installare il DB)
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
    FormatFields = record_info(fields, format),
    % NB il nome della tabella e' == al nome dei records che essa ospita
    % specifico i parametri opzionali per avere una copia del DB
    % su disco e in RAM anche nei nodi distribuiti
    mnesia:create_table(owner, [
        {attributes, OwnerFields},
        {disc_copies, NodeList}
    ]),
    mnesia:create_table(policy, [
        {attributes, PolicyFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    mnesia:create_table(format, [
        {attributes, FormatFields},
        {disc_copies, NodeList},
        {type, bag}
    ])
.

% DISTRIBUITA
% da chiamare SOLO UNA VOLTA per creare il DB Mnesia DISTRIBUITO
% serve per creare i file del DB sui vari nodi (installare il DB)
create_table_distrib(RemoteNodeList) ->
    % creo il DB in locale -> node() e in remoto 
    % nei nodi che presenti in RemoteNodeList
    NodeList = [node()] ++ RemoteNodeList,
    mnesia:create_schema(NodeList),
    % faccio partire Mnesia nei nodi remoti e da me
    start_remote(RemoteNodeList),
    % creo lo schema delle due tabelle del DB coi campi che prendo dai records
    %SpreadsheetFields = record_info(fields, spreadsheet),
    OwnerFields = record_info(fields, owner),
    PolicyFields = record_info(fields, policy),
    FormatFields = record_info(fields, format),
    % NB il nome della tabella e' == al nome dei records che essa ospita
    % specifico i parametri opzionali per avere una copia del DB
    % su disco e in RAM anche nei nodi distribuiti
    mnesia:create_table(owner, [
        {attributes, OwnerFields},
        {disc_copies, NodeList}
    ]),
    mnesia:create_table(policy, [
        {attributes, PolicyFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    mnesia:create_table(format, [
        {attributes, FormatFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    % stop per ogni nodo remoto e per me
    distribution:stop_distrib(RemoteNodeList)
.

% LOCALE
% da utilizzare OGNI VOLTA che si vuole usare il DB
% va invocato su ogni nodo del sistema
start() ->
    % faccio partire il DBMS Mnesia
    mnesia:start(),
    % aspetto (5 sec) che vengano caricate le tabelle del DB (distribuito)
    mnesia:wait_for_tables([owner, policy, format], 5000)
.

% DISTRIBUITA
% fa partire Mnesia e carica le tabelle
start_distrib(RemoteNodeList) ->
    % start per ogni nodo remoto e per me
    start_remote(RemoteNodeList),
    % aspetto (5 sec) che vengano caricate le tabelle del DB (distribuito)
    mnesia:wait_for_tables([owner, policy, format], 5000)
.

% NON VIENE ESPORTATA ALL'ESTERNO
% DISTRIBUITA
% fa partire Mnesia ma non carica le tabelle
start_remote(RemoteNodeList) ->
    % NB nel DISTRIBUITO devo far partire Mnesia da dentro
    % ciascun nodo remoto e poi lo faccio partire da me in locale
    MioPid = self(),
    lists:foreach(
        fun(Node) ->
            spawn(
                Node, 
                fun() ->
                    % NB devo farlo "dentro" ogni nodo remoto 
                    mnesia:start(),
                    MioPid!{mnesia_started} 
                end
            ) 
        end,
        RemoteNodeList
    ),
    % mi metto in attesa che Mnesia sia partito in tutti i nodi
    % remoti e poi lo eseguo nel mio nodo locale (master)
    lists:foreach(
        fun(_Node) -> 
            receive 
                {mnesia_started} -> ok 
            end
        end,
        RemoteNodeList
    ),
    % faccio partire Mnesia in locale
    mnesia:start()
.

% LOCALE
% API SEMPLICE PER STOP DI MNESIA
stop() -> mnesia:stop().

% DISTRIBUITA
% stop per ogni nodo remoto e per me
stop_distrib(RemoteNodeList) ->
    % NB nel DISTRIBUITO devo far terminare Mnesia da dentro
    % ciascun nodo remoto e poi lo faccio terminare da me in locale
    MioPid = self(),
    lists:foreach(
        fun(Node) ->
            spawn(
                Node, 
                fun() ->
                    % NB devo farlo "dentro" ogni nodo remoto 
                    mnesia:stop(),
                    MioPid!{mnesia_stopped} 
                end
            ) 
        end,
        RemoteNodeList
    ),
    % mi metto in attesa che Mnesia sia terminato in tutti i nodi
    % remoti e poi lo termino nel mio nodo locale (master)
    lists:foreach(
        fun(_Node) -> 
            receive 
                {mnesia_stopped} -> ok 
            end
        end,
        RemoteNodeList
    ),
    % faccio terminare Mnesia in locale
    mnesia:stop()
.

save_name(Name)->
    global:register_name(Name,self())
.

lookup_name(Name)->
    global:whereis_name(Name)
.