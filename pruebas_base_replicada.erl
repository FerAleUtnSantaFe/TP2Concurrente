-module(pruebas_base_replicada).
-include_lib("eunit/include/eunit.hrl").
-export([prueba_basica/0, prueba1/0, prueba2/0, prueba3/0, prueba4/0, prueba5/0, 
         prueba6/0, prueba7/0, prueba8/0, prueba9/0, prueba10/0]).

% Prueba inicial con 5 servidores
prueba_basica() ->
    base_replicada:start(replica, 5).

% Inserción con diferentes niveles de consistencia
prueba1() ->
    replica_server:put(replica1, "clave1", 100, os:timestamp(), one),
    replica_server:put(replica2, "clave2", 200, os:timestamp(), quorum),
    replica_server:put(replica3, "clave3", 300, os:timestamp(), all).

% Búsqueda con diferentes niveles de consistencia
prueba2() ->
    R1 = replica_server:get(replica1, "clave1", one),
    R2 = replica_server:get(replica3, "clave2", quorum),
    R3 = replica_server:get(replica5, "clave3", all),
    {R1, R2, R3}.

% Eliminación con diferentes niveles de consistencia
prueba3() ->
    replica_server:rem(replica1, "clave1", os:timestamp(), one),
    replica_server:rem(replica4, "clave2", os:timestamp(), quorum),
    replica_server:rem(replica5, "clave3", os:timestamp(), all).

% Verificación de eliminaciones
prueba4() ->
    R1 = replica_server:get(replica1, "clave1", one),
    R2 = replica_server:get(replica4, "clave2", quorum),
    R3 = replica_server:get(replica5, "clave3", all),
    {R1, R2, R3}.

% Prueba de parada masiva
prueba5() ->
    base_replicada:stop().

% Prueba de lecturas y escrituras secuenciales
prueba6() ->
    base_replicada:start(replica, 6),
    R1 = replica_server:get(replica1, "test", one),
    replica_server:put(replica1, "test", 100, 1, one),
    R2 = replica_server:get(replica2, "test", one),
    replica_server:put(replica2, "test", 100, 2, one),
    R3 = replica_server:get(replica3, "test", one),
    R4 = replica_server:get(replica3, "test", quorum),
    replica_server:put(replica3, "test", 100, 3, one),
    R5 = replica_server:get(replica4, "test", quorum),
    replica_server:put(replica4, "test", 100, 4, one),
    R6 = replica_server:get(replica5, "test", all),
    replica_server:put(replica5, "test", 100, 5, all),
    {R1, R2, R3, R4, R5, R6}.

% Prueba con sistema grande
prueba7() ->
    base_replicada:start(replica, 100),
    replica_server:rem(replica15, "prueba", 1, all),
    R1 = replica_server:get(replica50, "prueba", quorum),
    R2 = replica_server:get(replica5, "prueba", one),
    R3 = replica_server:get(replica70, "prueba", all),
    {R1, R2, R3}.

% Prueba de quórum fallido
prueba8() ->
    base_replicada:start(replica, 10),
    [replica_server:put(list_to_atom("replica" ++ integer_to_list(N)), 
                       "test", 100, 22, one) || N <- lists:seq(1,6)],
    replica_server:put(replica10, "test", 200, 2, quorum),
    {replica_server:get(replica3, "test", one),
     replica_server:get(replica9, "test", one)}.

% Prueba de quórum exitoso
prueba9() ->
    base_replicada:start(replica, 10),
    [replica_server:put(list_to_atom("replica" ++ integer_to_list(N)), 
                       "test", 100, 3, one) || N <- lists:seq(1,5)],
    replica_server:put(replica10, "test", 200, 2, quorum),
    {replica_server:get(replica8, "test", one),
     replica_server:get(replica3, "test", one)}.

% Prueba de consistencia ALL fallida
prueba10() ->
    base_replicada:start(replica, 10),
    replica_server:put(replica1, "test", 100, 1500, one),
    replica_server:put(replica10, "test", 200, 1300, all),
    {replica_server:get(replica1, "test", one),
     replica_server:get(replica10, "test", one)}.