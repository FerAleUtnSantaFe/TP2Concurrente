-module(replica_server).
-export([start/2, stop/1, putt/5, remm/4, gett/3, sizee/1, get_state/1]).
-export([init/1, handle_call/3]).

-record(state, {dict, replicas}).

%%% APIs públicas %%%
start(Name, Replicas) ->
    gen_server:start_link({local, Name}, ?MODULE, Replicas, []).

stop(Name) ->
    gen_server:call(Name, {stop, Name}).

putt(Name, Key, Value, TimeStamp, Consistency) ->
    gen_server:call(Name, {putt, Key, Value, TimeStamp, Consistency}).

remm(Name, Key, TimeStamp, Consistency) ->
    gen_server:call(Name, {remm, Key, TimeStamp, Consistency}).

gett(Name, Key, Consistency) ->
    gen_server:call(Name, {gett, Key, Consistency}).

sizee(Name) ->
    gen_server:call(Name, sizee).

get_state(Name) ->
    gen_server:call(Name, getState).

%%% Funcionalidades %%%
init(Replicas) ->
    {ok, #state{dict = dict:new(), replicas = Replicas}}.

handle_call({putt, Key, Value, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = handle_putt(Key, Value, TimeStamp, Consistency, State),
    {reply, ok, NewState};

handle_call({remm, Key, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = handle_remm(Key, TimeStamp, Consistency, State),
    {reply, ok, NewState};

handle_call({gett, Key, Consistency}, _From, State) ->
    {reply, do_gett(Key, Consistency, State), State};

handle_call(sizee, _From, State) ->
    {reply, dict:size(State#state.dict), State};

handle_call({stop, Nombre}, _From, State) ->
    lists:foreach(
        fun(Replica) -> gen_server:call(Replica, {change_replica_list, Nombre}) end,
        State#state.replicas
    ),
    {stop, normal, ok, State};

handle_call({change_replica_list, Nombre}, _From, State) ->
    {reply, ok, State#state{replicas = lists:delete(Nombre, State#state.replicas)}};

handle_call(getState, _From, State) ->
    {reply, State, State};

handle_call({check_putt, Key, _Value, TimeStamp}, _From, State) ->
    {reply, check_putt(Key, TimeStamp, State), State};

handle_call({check_remm, Key, TimeStamp}, _From, State) ->
    {reply, check_remm(Key, TimeStamp, State), State};

handle_call(_Msg, _From, _State) ->
    {reply, error}.

%%% Auxiliares %%%
handle_putt(Key, Value, TimeStamp, one, State) ->
    do_putt_one(Key, Value, TimeStamp, State);
handle_putt(Key, Value, TimeStamp, quorum, State) ->
    do_putt_quorum(Key, Value, TimeStamp, State);
handle_putt(Key, Value, TimeStamp, all, State) ->
    do_putt_all(Key, Value, TimeStamp, State).

handle_remm(Key, TimeStamp, one, State) ->
    do_remm_one(Key, TimeStamp, State);
handle_remm(Key, TimeStamp, quorum, State) ->
    do_remm_quorum(Key, TimeStamp, State);
handle_remm(Key, TimeStamp, all, State) ->
    do_remm_all(Key, TimeStamp, State).

check_putt(Key, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        error -> accept;
        {_, {_, _, false}} -> accept;
        {_, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp -> accept;
        _ -> reject
    end.

check_remm(Key, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTimeStamp, true}} when TimeStamp =< ExistingTimeStamp -> reject;
        {ok, {_, _, false}} -> reject;
        _ -> accept
    end.

do_gett(Key, one, State) ->
    do_gett_one(Key, State);
do_gett(Key, quorum, State) ->
    do_gett_quorum(Key, State);
do_gett(Key, all, State) ->
    do_gett_all(Key, State).

do_putt_one(Key, Value, TimeStamp, State) ->
    Val = dict:find(Key, State#state.dict),
    case Val of
        error -> 
            % Es posible almacenar si no existe la tupla
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {_, {_Value1, _TimeStamp1, false}} ->
            % Se puede almacenar si existia una tupla con esa key pero fue eliminada logicamente
            NewDict = dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {_, {_Value1, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp->
            % Se puede almacenar si existe una tupla con la misma key pero el timestamp es menor
            NewDict = dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        _ -> {error, State}
    end.

do_putt_quorum(Key, Value, TimeStamp, State) ->
    % Evaluaremos primero en la replica organizadora si ésta existe o no, del mismo modo que en la funcion anterior
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            error -> 
                {1, dict:store(Key, {Value, TimeStamp, true}, State#state.dict)};
            {_, {_Value1, _TimeStamp1, false}} ->
                {1, dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict)};
            _ -> {0, State#state.dict}
        end,
    % Mediante el uso de mensajes, contabilizamos que replicas pueden ser almacenadas y cuales no
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case gen_server:call(Replica, {check_putt, Key, Value, TimeStamp}) of
                accept ->
                    {Acc + 1, Rejected};
                reject ->
                    {Acc, Rejected + 1}
            end
        end,
        {Aux, 0},
        State#state.replicas
    ),
    % Finalmente, verificamos la cantidad de confirmados y, si se alcanza el quórum, se procede con la inserción.
    if
        Confirmed >= length(State#state.replicas) div 2 + 1 ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {putt, Key, Value, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_putt_all(Key, Value, TimeStamp, State) ->
    % En primer lugar, verificamos en la réplica organizadora si está presente o no, de manera similar a la función anterior.
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            error -> 
                {1, dict:store(Key, {Value, TimeStamp, true}, State#state.dict)};
            {_, {_Value1, _TimeStamp1, false}} ->
                {1, dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict)};
            _ -> {0, State#state.dict}
        end,
    % A continuación, determinamos qué réplicas tienen capacidad de almacenamiento y cuáles no, utilizando mensajes.
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case gen_server:call(Replica, {check_putt, Key, Value, TimeStamp}) of
                accept ->
                    {Acc + 1, Rejected};
                reject ->
                    {Acc, Rejected + 1}
            end
        end,
        {Aux, 0},
        State#state.replicas
    ),
    % Por último, verificamos el número de confirmaciones y, en ausencia de rechazos, procedemos con la inserción.
    if
        Confirmed > length(State#state.replicas) ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {putt, Key, Value, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_remm_one(Key, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        % En caso de existir y estar activa, se actualiza el timestamp al valor actual y se deshabilita la tupla.
        {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
            NewDict = dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        % Si está presente pero inactiva, no se lleva a cabo ninguna modificación.
        {ok, {_, _, false}} ->
            {noreply, State};
        % Si no se encuentra, se genera una nueva tupla con valor nulo, el timestamp actual y en estado inactivo.
        _ ->
            NewDict = dict:store(Key, {null, TimeStamp, false}, State#state.dict),
            {noreply, State#state{dict=NewDict}}
    end.

do_remm_quorum(Key, TimeStamp, State) ->
    % En primer lugar, verificamos en la réplica organizadora la existencia del elemento, siguiendo el mismo criterio que en la función anterior.
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                {1, dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict)};
            {ok, {_, _, false}} ->
                {0, State#state.dict};
            _ -> 
                {1, dict:store(Key, {null, TimeStamp, false}, State#state.dict)}
        end,
    % A continuación, determinamos qué réplicas están en condiciones de llevar a cabo la eliminación y cuáles no, utilizando mensajes para esta evaluación.
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case gen_server:call(Replica, {check_remm, Key, TimeStamp}) of
                accept ->
                    {Acc + 1, Rejected};
                reject ->
                    {Acc, Rejected + 1}
            end
        end,
        {Aux, 0},
        State#state.replicas
    ),
    % Por último, analizamos la cantidad de confirmaciones y, si se alcanza el quorum requerido, procedemos con la operación.
    if
        Confirmed >= length(State#state.replicas) div 2 + 1 ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {remm, Key, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_remm_all(Key, TimeStamp, State) ->
    % Evaluamos la existencia de la tupla
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                {1, dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict)};
            {ok, {_, _, false}} ->
                {0, State#state.dict};
            _ -> 
                {1, dict:store(Key, {null, TimeStamp, false}, State#state.dict)}
        end,
    % Contamos la cantidad de replicas que pueden realizar la eliminación
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case gen_server:call(Replica, {check_remm, Key, TimeStamp}) of
                accept ->
                    {Acc + 1, Rejected};
                reject ->
                    {Acc, Rejected + 1}
            end
        end,
        {Aux, 0},
        State#state.replicas
    ),
    % Se verifica que todos cumplan y eliminamos
    if
        Confirmed > length(State#state.replicas) ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {remm, Key, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_gett_one(Key, State) ->
    Val = dict:find(Key, State#state.dict),
    case Val of
        % Si se encuentra la tupla se devuelve value y timestamp
        {_, {Value1, TimeStamp1, true}} -> {Value1, TimeStamp1};
        % Si encuentra la tupla y esta desactivada devuelve ko y timestamp
        {_, {_, TimeStamp1, false}} -> {ko, TimeStamp1};
        % Si no existe la tupla retorna notfound
        error -> notfound;
        _ -> notfound
    end.

do_gett_one_replica(Replica, Key) ->
    case gen_server:call(Replica, {gett, Key, one}) of
        {Value1, TimeStamp1} -> {Value1, TimeStamp1};
        _ -> notfound
    end.

do_gett_quorum(Key, State) ->
    % Generamos una lista con los resultados obtenidos al buscar la clave en todas las réplicas.
    Replies = lists:map(
        fun(Replica) ->
            case do_gett_one_replica(Replica, Key) of 
               {Value1, TimeStamp1} -> {Value1, TimeStamp1};
               notfound -> notfound;
               _ -> notfound
            end
        end,
        State#state.replicas
    ),
    % Eliminamos las respuestas "not found" y conservamos únicamente los valores junto con sus respectivos timestamps.
    RepliesFiltered = lists:filter(
        fun({_Value, _TimeStamp}) -> true;
           (_) -> false
        end,
        Replies
    ),
    % Retornamos la tupla que posee el timestamp más reciente.
    case RepliesFiltered of
        [] -> notfound;
        _ -> lists:max(RepliesFiltered)
    end.

do_gett_all(Key, State) ->
    % Obtenemos una lista con los resultados de la búsqueda de la clave en todas las réplicas.
    Replies = lists:map(
        fun(Replica) ->
            do_gett_one_replica(Replica, Key)
        end,
        State#state.replicas
    ),
    % Verificamos si hay alguna respuesta "not found"; si la hay, retornamos "not found". En caso contrario, devolvemos la tupla con el timestamp más reciente
    case lists:any(fun(X) -> X == notfound end, Replies) of
        true -> notfound;
        false ->
            RepliesFiltered = lists:filter(
                fun({_Value, _TimeStamp}) -> true;
                   (_) -> false
                end,
                Replies
            ),
            case RepliesFiltered of
                [] -> notfound;
                _ -> lists:max(RepliesFiltered)
            end
    end.