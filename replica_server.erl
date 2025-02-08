-module(replica_server).
-export([start/2, stop/1, putt/5, remm/4, gett/3, sizee/1, get_state/1]).
-export([init/1, handle_call/3]).
-record(state, {dict, replicas}).

%%% Funciones publicas %%%

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

%%% Funciones privadas %%%
init(Replicas) ->
    {ok, #state{dict = dict:new(), replicas = Replicas}}.

handle_call({putt, Key, Value, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = case Consistency of
        one ->
            do_putt_one(Key, Value, TimeStamp, State);
        quorum ->
            do_putt_quorum(Key, Value, TimeStamp, State);
        all ->
            do_putt_all(Key, Value, TimeStamp, State)
    end,
    {reply, ok, NewState};

handle_call({remm, Key, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = case Consistency of
        one ->
            do_remm_one(Key, TimeStamp, State);
        quorum ->
            do_remm_quorum(Key, TimeStamp, State);
        all ->
            do_remm_all(Key, TimeStamp, State)
    end,
    {reply, ok, NewState};

handle_call({gett, Key, Consistency}, _From, State) ->
    Val = do_gett(Key, Consistency, State),
    {reply, Val, State};

handle_call(sizee, _From, State) ->
    {reply, dict:size(State#state.dict), State};

handle_call({stop, Nombre}, _From, State) ->
    lists:map(
        fun(Replica) ->
            gen_server:call(Replica, {change_replica_list, Nombre})
        end,
        State#state.replicas
    ),
    {stop, normal, ok, State};

handle_call({change_replica_list, Nombre}, _From, State) ->
    NuevaLista = lists:delete(Nombre, State#state.replicas),
    {reply, ok, State#state{replicas=NuevaLista}};

handle_call(getState , _From, State ) ->
    {reply, {State}, State};

handle_call({check_putt, Key, _Value, TimeStamp} , _From, State ) ->
    Reply = case dict:find(Key, State#state.dict) of
        error -> 
            accept;
        {_, {_Value1, _TimeStamp1, false}} ->
            accept;
        {_, {_Value1, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp->
            accept;
        _ ->
            reject
    end,
    {reply, Reply, State};

handle_call({check_remm, Key, TimeStamp} , _From, State ) ->
    Reply = case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTimeStamp, true}} when TimeStamp =< ExistingTimeStamp ->
            % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
            reject;
        {ok, {_, _, false}} ->
            % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
            reject;
        _ ->
            % Si existe pero está desactivada, no se realiza ninguna acción
            accept
    end,
    {reply, Reply, State};

handle_call(_Msg , _From, _State ) ->
    {reply, error}.

%%% Funciones internas %%%
do_gett(Key, Consistency, State) ->
    case Consistency of
        one ->
            do_gett_one(Key, State);
        quorum ->
            do_gett_quorum(Key, State);
        all ->
            do_gett_all(Key, State)
    end.

do_putt_one(Key, Value, TimeStamp, State) ->
    Val = dict:find(Key, State#state.dict),
    case Val of
        error -> 
            % Si no existe la tupla entonces es posible almacenarla
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {_, {_Value1, _TimeStamp1, false}} ->
            % Si existia una tupla con esa key pero fue eliminada logicamente, entonces si se puede almacenar
            NewDict = dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {_, {_Value1, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp->
            % Si exsite una tupla con la misma key pero el timestamp es menor, entonces si se puede almacenar
            NewDict = dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        _ -> {error, State}
    end.

do_putt_quorum(Key, Value, TimeStamp, State) ->
    % Primero evaluamos en la replica organizadora si existe o no, igual que en la funcion anterior
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            error -> 
                {1, dict:store(Key, {Value, TimeStamp, true}, State#state.dict)};
            {_, {_Value1, _TimeStamp1, false}} ->
                {1, dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict)};
            _ -> {0, State#state.dict}
        end,
    % Luego contabilizamos que replicas pueden almacenar y cuales no, mediante el uso de mensajes
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
    % Finalmente evaluamos la cantidad de confirmados, y si tienen quorum entonces se realiza la incercion
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
    % Primero evaluamos en la replica organizadora si existe o no, igual que en la funcion anterior
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            error -> 
                {1, dict:store(Key, {Value, TimeStamp, true}, State#state.dict)};
            {_, {_Value1, _TimeStamp1, false}} ->
                {1, dict:update(Key, fun({_Value, _TimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict)};
            _ -> {0, State#state.dict}
        end,
    % Luego contabilizamos que replicas pueden almacenar y cuales no, mediante el uso de mensajes
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
    % Finalmente evaluamos la cantidad de confirmados, y si no existe ningun rechazado, se realiza la incercion
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
        % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
        {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
            NewDict = dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        % Si existe pero está desactivada, no se realiza ninguna acción
        {ok, {_, _, false}} ->
            {noreply, State};
        % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
        _ ->
            NewDict = dict:store(Key, {null, TimeStamp, false}, State#state.dict),
            {noreply, State#state{dict=NewDict}}
    end.

do_remm_quorum(Key, TimeStamp, State) ->
    % Primero evaluamos en la replica organizadora si existe o no, igual que en la funcion anterior
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                {1, dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict)};
            {ok, {_, _, false}} ->
                {0, State#state.dict};
            _ -> 
                {1, dict:store(Key, {null, TimeStamp, false}, State#state.dict)}
        end,
    % Luego contabilizamos que replicas pueden relizar la eliminacion y cuales no, mediante el uso de mensajes
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
    % Finalmente evaluamos la cantidad de confirmados, y si tienen quorum entonces se realiza la operacion
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
    % Primero evaluamos en la replica organizadora si existe o no, igual que en la funcion anterior
    {Aux, NewDict} = case dict:find(Key, State#state.dict) of
            {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                {1, dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict)};
            {ok, {_, _, false}} ->
                {0, State#state.dict};
            _ -> 
                {1, dict:store(Key, {null, TimeStamp, false}, State#state.dict)}
        end,
    % Luego contabilizamos que replicas pueden relizar la eliminacion y cuales no, mediante el uso de mensajes
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
    % Finalmente evaluamos la cantidad de confirmados, y si tienen quorum entonces se realiza la operacion
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
        % Si existe una tupla con la clave ingresada y esta activa, se devuelve el valor y el timestamp
        {_, {Value1, TimeStamp1, true}} -> {Value1, TimeStamp1};
        % Si existe una tupla con la clave ingresada y no esta activa, se devuelve ko y el timestamp
        {_, {_, TimeStamp1, false}} -> {ko, TimeStamp1};
        % Si no existe una tupla con la clave ingresada, se devuelve notfound
        error -> notfound;
        _ -> notfound
    end.

do_gett_one_replica(Replica, Key) ->
    case gen_server:call(Replica, {gett, Key, one}) of
        {Value1, TimeStamp1} -> {Value1, TimeStamp1};
        _ -> notfound
    end.

do_gett_quorum(Key, State) ->
    % Realizamos una lista con los resultados de buscar la key en todas las replicas
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
    % Filtramos los notfound y nos quedamos solo con los valores y timestamps
    RepliesFiltered = lists:filter(
        fun({_Value, _TimeStamp}) -> true;
           (_) -> false
        end,
        Replies
    ),
    % Devolvemos la tupla con el maximo timestamp
    case RepliesFiltered of
        [] -> notfound;
        _ -> lists:max(RepliesFiltered)
    end.

do_gett_all(Key, State) ->
    % Realizamos una lista con los resultados de buscar la key en todas las replicas
    Replies = lists:map(
        fun(Replica) ->
            do_gett_one_replica(Replica, Key)
        end,
        State#state.replicas
    ),
    % Evaluamos si existe algun notfound, y en caso de existir entonces se devuelve notfound, caso contrario la tupla con mayor timestamp
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