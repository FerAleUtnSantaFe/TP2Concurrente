-module(base_replicada).
-export([start/2, stop/0]).

start(Name, N) ->
    % Generar nombres para todas las réplicas
    ReplicaNames = [list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(I)) || I <- lists:seq(1, N)],
    
    % Iniciar cada réplica con la lista de las demás réplicas
    Results = lists:map(fun(ReplicaName) ->
        OtherReplicas = ReplicaNames -- [ReplicaName],
        replica_server:start(ReplicaName, OtherReplicas)
    end, ReplicaNames),
    
    % Verificar que todas las réplicas iniciaron correctamente
    case lists:all(fun({ok, _}) -> true; (_) -> false end, Results) of
        true -> {ok, ReplicaNames};
        false -> 
            % Si alguna falló, detener todas y retornar error
            stop(),
            {error, startup_failed}
    end.

stop() ->
    % Obtener todas las réplicas registradas y detenerlas
    Replicas = [Name || Name <- registered(), 
                       lists:prefix("replica-", atom_to_list(Name))],
    lists:foreach(fun(Replica) ->
        replica_server:stop(Replica)
    end, Replicas),
    ok.

%%% Segundo módulo: replica_server.erl %%%
-module(replica_server).
-export([start/2, stop/1, put/5, rem/4, get/3, size/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).
-behaviour(gen_server).

-record(state, {
    dict = dict:new(),           % Diccionario principal
    tombstones = dict:new(),     % Registro de elementos eliminados
    replicas = [],              % Lista de otras réplicas
    pending_ops = dict:new()    % Operaciones pendientes
}).

-define(TIMEOUT, 5000).         % Timeout para operaciones distribuidas

%%% API Pública %%%
start(Name, Replicas) ->
    gen_server:start_link({local, Name}, ?MODULE, Replicas, []).

stop(Name) ->
    gen_server:call(Name, stop, ?TIMEOUT).

put(Name, Key, Value, TimeStamp, Consistency) ->
    try
        gen_server:call(Name, {put, Key, Value, TimeStamp, Consistency}, ?TIMEOUT)
    catch
        exit:{timeout, _} -> {error, timeout};
        _:Error -> {error, Error}
    end.

rem(Name, Key, TimeStamp, Consistency) ->
    try
        gen_server:call(Name, {rem, Key, TimeStamp, Consistency}, ?TIMEOUT)
    catch
        exit:{timeout, _} -> {error, timeout};
        _:Error -> {error, Error}
    end.

get(Name, Key, Consistency) ->
    try
        gen_server:call(Name, {get, Key, Consistency}, ?TIMEOUT)
    catch
        exit:{timeout, _} -> {error, timeout};
        _:Error -> {error, Error}
    end.

size(Name) ->
    gen_server:call(Name, size, ?TIMEOUT).

%%% Callbacks gen_server %%%
init(Replicas) ->
    {ok, #state{replicas = Replicas}}.

handle_call({put, Key, Value, TimeStamp, one}, _From, State) ->
    {Reply, NewState} = do_local_put(Key, Value, TimeStamp, State),
    % Propagar asíncronamente a otras réplicas
    lists:foreach(fun(Replica) ->
        gen_server:cast(Replica, {replicate_put, Key, Value, TimeStamp})
    end, State#state.replicas),
    {reply, Reply, NewState};

handle_call({put, Key, Value, TimeStamp, Consistency}, From, State) ->
    RequiredReplies = required_replies(Consistency, length(State#state.replicas)),
    OpId = make_ref(),
    % Registrar operación pendiente
    NewState = State#state{
        pending_ops = dict:store(OpId, {put, From, RequiredReplies, []}, State#state.pending_ops)
    },
    % Realizar operación local
    {LocalResult, StateAfterLocal} = do_local_put(Key, Value, TimeStamp, NewState),
    % Enviar solicitudes a otras réplicas
    lists:foreach(fun(Replica) ->
        gen_server:cast(Replica, {check_put, Key, Value, TimeStamp, OpId, self()})
    end, State#state.replicas),
    % Iniciar timer para timeout
    erlang:send_after(?TIMEOUT, self(), {timeout, OpId}),
    {noreply, StateAfterLocal};

handle_call({rem, Key, TimeStamp, one}, _From, State) ->
    {Reply, NewState} = do_local_rem(Key, TimeStamp, State),
    % Propagar asíncronamente
    lists:foreach(fun(Replica) ->
        gen_server:cast(Replica, {replicate_rem, Key, TimeStamp})
    end, State#state.replicas),
    {reply, Reply, NewState};

handle_call({rem, Key, TimeStamp, Consistency}, From, State) ->
    RequiredReplies = required_replies(Consistency, length(State#state.replicas)),
    OpId = make_ref(),
    NewState = State#state{
        pending_ops = dict:store(OpId, {rem, From, RequiredReplies, []}, State#state.pending_ops)
    },
    {LocalResult, StateAfterLocal} = do_local_rem(Key, TimeStamp, NewState),
    lists:foreach(fun(Replica) ->
        gen_server:cast(Replica, {check_rem, Key, TimeStamp, OpId, self()})
    end, State#state.replicas),
    erlang:send_after(?TIMEOUT, self(), {timeout, OpId}),
    {noreply, StateAfterLocal};

handle_call({get, Key, one}, _From, State) ->
    Reply = do_local_get(Key, State),
    {reply, Reply, State};

handle_call({get, Key, Consistency}, From, State) ->
    RequiredReplies = required_replies(Consistency, length(State#state.replicas)),
    OpId = make_ref(),
    NewState = State#state{
        pending_ops = dict:store(OpId, {get, From, RequiredReplies, []}, State#state.pending_ops)
    },
    LocalResult = do_local_get(Key, State),
    lists:foreach(fun(Replica) ->
        gen_server:cast(Replica, {check_get, Key, OpId, self()})
    end, State#state.replicas),
    erlang:send_after(?TIMEOUT, self(), {timeout, OpId}),
    {noreply, NewState};

handle_call(size, _From, State) ->
    {reply, dict:size(State#state.dict), State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({replicate_put, Key, Value, TimeStamp}, State) ->
    {_, NewState} = do_local_put(Key, Value, TimeStamp, State),
    {noreply, NewState};

handle_cast({replicate_rem, Key, TimeStamp}, State) ->
    {_, NewState} = do_local_rem(Key, TimeStamp, State),
    {noreply, NewState};

handle_cast({check_put, Key, Value, TimeStamp, OpId, From}, State) ->
    Result = can_put(Key, Value, TimeStamp, State),
    gen_server:cast(From, {put_response, OpId, Result}),
    {noreply, State};

handle_cast({check_rem, Key, TimeStamp, OpId, From}, State) ->
    Result = can_remove(Key, TimeStamp, State),
    gen_server:cast(From, {rem_response, OpId, Result}),
    {noreply, State};

handle_cast({check_get, Key, OpId, From}, State) ->
    Result = do_local_get(Key, State),
    gen_server:cast(From, {get_response, OpId, Result}),
    {noreply, State};

handle_cast({put_response, OpId, Result}, State) ->
    handle_operation_response(OpId, Result, State);

handle_cast({rem_response, OpId, Result}, State) ->
    handle_operation_response(OpId, Result, State);

handle_cast({get_response, OpId, Result}, State) ->
    handle_operation_response(OpId, Result, State).

handle_info({timeout, OpId}, State) ->
    case dict:find(OpId, State#state.pending_ops) of
        {ok, {_Op, From, _Required, _Responses}} ->
            gen_server:reply(From, {error, timeout}),
            NewState = State#state{
                pending_ops = dict:erase(OpId, State#state.pending_ops)
            },
            {noreply, NewState};
        error ->
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

%%% Funciones internas %%%
required_replies(quorum, ReplicaCount) ->
    (ReplicaCount div 2) + 1;
required_replies(all, ReplicaCount) ->
    ReplicaCount + 1.

do_local_put(Key, Value, TimeStamp, State) ->
    case can_put(Key, Value, TimeStamp, State) of
        true ->
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            {{ok, accepted}, State#state{dict = NewDict}};
        false ->
            {{ok, rejected}, State}
    end.

do_local_rem(Key, TimeStamp, State) ->
    case can_remove(Key, TimeStamp, State) of
        true ->
            NewDict = case dict:find(Key, State#state.dict) of
                {ok, {Value, _, _}} ->
                    dict:store(Key, {Value, TimeStamp, false}, State#state.dict);
                error ->
                    dict:store(Key, {undefined, TimeStamp, false}, State#state.dict)
            end,
            {{ok, accepted}, State#state{dict = NewDict}};
        false ->
            {{ok, rejected}, State}
    end.

do_local_get(Key, State) ->
    case dict:find(Key, State#state.dict) of
        {ok, {Value, TimeStamp, true}} ->
            {ok, Value, TimeStamp};
        {ok, {_, TimeStamp, false}} ->
            {deleted, TimeStamp};
        error ->
            not_found
    end.

can_put(Key, _Value, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTS, _}} -> TimeStamp > ExistingTS;
        error -> true
    end.

can_remove(Key, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTS, _}} -> TimeStamp > ExistingTS;
        error -> true
    end.

handle_operation_response(OpId, Result, State) ->
    case dict:find(OpId, State#state.pending_ops) of
        {ok, {Op, From, Required, Responses}} ->
            NewResponses = [Result|Responses],
            case length(NewResponses) >= Required of
                true ->
                    FinalResult = case Op of
                        put -> 
                            case lists:all(fun(R) -> R == accepted end, NewResponses) of
                                true -> {ok, accepted};
                                false -> {ok, rejected}
                            end;
                        rem ->
                            case lists:all(fun(R) -> R == accepted end, NewResponses) of
                                true -> {ok, accepted};
                                false -> {ok, rejected}
                            end;
                        get ->
                            % Obtener el resultado con el timestamp más reciente
                            lists:foldl(fun
                                ({ok, V1, T1}, {ok, V2, T2}) when T1 > T2 -> {ok, V1, T1};
                                (R1, R2) when R1 == not_found -> R2;
                                (R1, R2) when R2 == not_found -> R1;
                                (_, R2) -> R2
                            end, not_found, NewResponses)
                    end,
                    gen_server:reply(From, FinalResult),
                    {noreply, State#state{
                        pending_ops = dict:erase(OpId, State#state.pending_ops)
                    }};
                false ->
                    {noreply, State#state{
                        pending_ops = dict:store(OpId, {Op, From, Required, NewResponses}, State#state.pending_ops)
                    }}
            end;
        error ->
            {noreply, State}
    end.