-module(base_no_replicada).
-export([start/1, stop/0, putt/3, remm/2, gett/1, sizee/0, all/0, remall/0, inspect_dict/0]).

start(A) ->
    register(A, spawn(fun() -> init(A) end)).

init(A) ->
    Dict = dict:new(),
    case whereis(servers) of
        undefined ->  
            register(servers, spawn(fun() -> server(A) end)),
            loop(Dict);
        _ ->  
            loop(Dict)
    end.

server(Process) ->
    receive
        {consultaServidor, Pid} -> 
            Pid ! {replyServer, Process},
            server(Process);
        stop -> 
            exit(normal)
    end.

stop() -> 
    call(stop),
    servers ! stop.

all() -> call(all).
remall() -> call(remall).
putt(Key, Value, TimeStamp) -> call({putt, Key, Value, TimeStamp}).
remm(Key, TimeStamp) -> call({remm, Key, TimeStamp}).
gett(Key) -> call({gett, Key}).
sizee() -> call(sizee).
inspect_dict() -> call(inspect).

call(M) ->
    servers ! {consultaServidor, self()},
    receive 
        {replyServer, Process} -> P = Process
    end,
    Request = {request, self(), M},
    P ! Request,
    receive 
        {reply, Reply} -> Reply
    end.

reply(Pid, Reply) ->
    Pid ! {reply, Reply}.

loop(Dict) ->
    receive
        {request, Pid, stop} -> 
            reply(Pid, ok),
            exit(normal);

        {request, Pid, all} ->
            reply(Pid, Dict),
            loop(Dict);

        {request, Pid, sizee} ->
            Size = dict:size(Dict),
            reply(Pid, Size),
            loop(Dict);

        {request, Pid, inspect} -> 
            reply(Pid, Dict),
            loop(Dict);

        {request, Pid, {gett, Key}} ->
            case dict:find(Key, Dict) of
                error ->
                    reply(Pid, notfound),
                    loop(Dict);
                {ok, {Value, TimeStamp, Activo}} ->
                    case Activo of
                        true -> reply(Pid, {ok, {Value, TimeStamp}});
                        false -> reply(Pid, {ko, TimeStamp})
                    end,
                    loop(Dict)
            end;

        {request, Pid, {remm, Key, TimeStamp}} ->
            case dict:find(Key, Dict) of
                error ->
                    Dict1 = dict:store(Key, {null, TimeStamp, false}, Dict),
                    reply(Pid, notfound),
                    loop(Dict1);
                {ok, {_, TimeStamp1, Activo}} ->
                    case Activo of
                        true when TimeStamp > TimeStamp1 ->
                            Fun = fun({V, _, _}) -> {V, TimeStamp, false} end, 
                            Dict1 = dict:update(Key, Fun, Dict),
                            reply(Pid, ok),
                            loop(Dict1);
                        true -> 
                            reply(Pid, ko),
                            loop(Dict);
                        false ->
                            reply(Pid, notfound),
                            loop(Dict)
                    end
            end;

        {request, Pid, remall} ->
            reply(Pid, dict:new()), 
            loop(dict:new());

        {request, Pid, {putt, Key, Value, TimeStamp}} -> 
            case dict:find(Key, Dict) of
                error ->  
                    Dict1 = dict:store(Key, {Value, TimeStamp, true}, Dict),
                    reply(Pid, ok),
                    loop(Dict1);
                {ok, {_, TimeStamp1, _}} when TimeStamp > TimeStamp1 -> 
                    Dict1 = dict:store(Key, {Value, TimeStamp, true}, Dict),
                    reply(Pid, ok),
                    loop(Dict1);
                _ ->
                    reply(Pid, ko),
                    loop(Dict)
            end;

        _ ->
            loop(Dict)
    end.
