-module(base_de_datos).
-include_lib("eunit/include/eunit.hrl").
-export([prueba/0, prueba1/0, prueba2/0, prueba3/0, prueba4/0, prueba5/0, prueba6/0, prueba7/0, prueba8/0, prueba9/0, prueba10/0, start/2]).

start(Nombre, Cantidad) ->
    Aux = crear_lista(Nombre, Cantidad),
    lists:foreach( fun(Elem)-> 
                            Aux2 = lists:delete(Elem, Aux),
                            replica_server:start(Elem, Aux2)
                    end, Aux).

stop(Nombre, Cantidad) ->
    Aux = crear_lista(Nombre, Cantidad),
    lists:foreach( fun(Elem)-> 
                        replica_server:stop(Elem)
                    end, Aux).

crear_lista(_, 0) -> % Caso base: cuando la cantidad llega a cero, termina la recursión
    [];

crear_lista(Nombre, Cantidad) when Cantidad > 0 -> % Caso recursivo: mientras la cantidad sea mayor que cero, sigue construyendo la lista
    [list_to_atom(atom_to_list(Nombre) ++ integer_to_list(Cantidad)) | crear_lista(Nombre, Cantidad - 1)].

prueba() ->
    start(server, 5).

%% incerciones de los 3 tipos
prueba1() ->
    replica_server:putt(server1, "1", 1234, os:timestamp(), one),
    replica_server:putt(server2, "2", 1264, os:timestamp(), quorum),
    replica_server:putt(server3, "3", 3456, os:timestamp(), all).

%% busqueda de los 3 tipos
prueba2()->    
    Result1 = replica_server:gett(server1, "1", one),
    Result2 = replica_server:gett(server3, "2", quorum),
    Result3 = replica_server:gett(server5, "3", all),
    {Result1, Result2, Result3}.

%% eliminacion de los 3 tipos
prueba3() ->
    replica_server:remm(server1, "1", os:timestamp(), one),
    replica_server:remm(server4, "15", os:timestamp(), quorum),
    replica_server:remm(server5, "3", os:timestamp(), all).

%% buscamos las claves anteriores
prueba4() ->
    Result1 = replica_server:gett(server1, "1", one),
    Result2 = replica_server:gett(server4, "15", quorum),
    Result3 = replica_server:gett(server5, "3", all),
    {Result1, Result2, Result3}.

prueba5() ->
    stop(server, 200).

prueba6() ->
    start(server, 6),
    Result1 = replica_server:gett(server1, "1", one),
    replica_server:putt(server1, "1", 1234, 1, one),
    Result2 = replica_server:gett(server2, "1", one),
    replica_server:putt(server2, "1", 1234, 2, one),
    Result3 = replica_server:gett(server3, "1", one),
    Result4 = replica_server:gett(server3, "1", quorum),
    replica_server:putt(server3, "1", 1234, 3, one),
    Result5 = replica_server:gett(server4, "1", quorum),
    replica_server:putt(server4, "1", 1234, 4, one),
    Result6 = replica_server:gett(server5, "1", all),
    replica_server:putt(server5, "1", 1234, 5, all),
    {Result1, Result2, Result3, Result4, Result5, Result6}.

% prueba con muchos servers
prueba7() ->
    start(server, 100),
    replica_server:remm(server15, "4", 1, all),
    Result1 = replica_server:gett(server50, "4", quorum),
    Result2 = replica_server:gett(server5, "4", one),
    Result3 = replica_server:gett(server70, "4", all),
    {Result1, Result2, Result3}.

% Prueba que no tiene quorum y falla
prueba8() ->
    start(server, 10),
    replica_server:putt(server1, "1", 1234, 22, one),
    replica_server:putt(server2, "1", 1234, 22, one),
    replica_server:putt(server3, "1", 1234, 22, one),
    replica_server:putt(server4, "1", 1234, 22, one),
    replica_server:putt(server5, "1", 1234, 22, one),
    replica_server:putt(server6, "1", 1234, 22, one),
    replica_server:putt(server10, "1", 3333, 2, quorum),
    {replica_server:gett(server3, "1", one), replica_server:gett(server9, "1", one)}.

% Prueba que tiene quorum y funciona
prueba9() ->
    start(server, 10),
    replica_server:putt(server1, "1", 1234, 3, one),
    replica_server:putt(server2, "1", 1234, 3, one),
    replica_server:putt(server3, "1", 1234, 3, one),
    replica_server:putt(server4, "1", 1234, 3, one),
    replica_server:putt(server5, "1", 1234, 3, one),
    replica_server:putt(server10, "1", 3333, 2, quorum),
    {replica_server:gett(server8, "1", one), replica_server:gett(server3, "1", one)}.

% Prueba que falta uno para el all y falla
prueba10() ->
    start(server, 10),
    replica_server:putt(server1, "1", 1234, 1500, one),
    replica_server:putt(server10, "1", 3333, 1300, all),
    {replica_server:gett(server1, "1", one), replica_server:gett(server10, "1", one)}.