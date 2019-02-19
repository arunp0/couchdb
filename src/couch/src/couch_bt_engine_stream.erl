% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_bt_engine_stream).

-compile([{parse_transform, decorators}]).

-export([
    start_multipart/2,
    foldl/3,
    seek/2,
    write/2,
    finalize/1,
    to_disk_term/1,
    is_active_stream/1
]).

-export([handler/3]).

handler(Fun, Args,  {FunName, Line}) -> 
    couch_log:info("~p:~p ~p called ~p", [?MODULE, FunName, Line, config:get_boolean("object_storage", "active", false)]),
    case config:get_boolean("object_storage", "active", false) of
        false -> 
            Result = Fun(Args);
        true ->
            Result = erlang:apply(couch_bt_engine_stream_cloud, FunName, Args)
    end,
    couch_log:info("~p:~p ~p Result is: ~p", [?MODULE, FunName, Line, Result]),
    Result.

-decorate({?MODULE, handler, [], verbose}).
start_multipart(_, _)->
    ok.

-decorate({?MODULE, handler, [], verbose}).
foldl({_Fd, []}, _Fun, Acc) ->
    couch_log:info("111111111 ~p",[_Fd]),
    Acc;

foldl({Fd, [{Pos, _} | Rest]}, Fun, Acc) ->
    couch_log:info("222222222 ~p",[Pos]),
    foldl({Fd, [Pos | Rest]}, Fun, Acc);

foldl({Fd, [Bin | Rest]}, Fun, Acc) when is_binary(Bin) ->
    % We're processing the first bit of data
    % after we did a seek for a range fold.
    couch_log:info("333333333333 ~p",[Bin]),
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc));

foldl({Fd, [Pos | Rest]}, Fun, Acc) when is_integer(Pos) ->
    couch_log:info("44444444444 ~p",[Pos]),
    {ok, Bin} = couch_file:pread_binary(Fd, Pos),
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc)).

-decorate({?MODULE, handler, [], verbose}).
seek({Fd, [{Pos, Length} | Rest]}, Offset) ->
    case Length =< Offset of
        true ->
            seek({Fd, Rest}, Offset - Length);
        false ->
            seek({Fd, [Pos | Rest]}, Offset)
    end;

-decorate({?MODULE, handler, [], verbose}).
seek({Fd, [Pos | Rest]}, Offset) when is_integer(Pos) ->
    {ok, Bin} = couch_file:pread_binary(Fd, Pos),
    case iolist_size(Bin) =< Offset of
        true ->
            seek({Fd, Rest}, Offset - size(Bin));
        false ->
            <<_:Offset/binary, Tail/binary>> = Bin,
            {ok, {Fd, [Tail | Rest]}}
    end.

-decorate({?MODULE, handler, [], verbose}).
write({Fd, Written}, Data) when is_pid(Fd) ->
    {ok, Pos, _} = couch_file:append_binary(Fd, Data),
    {ok, {Fd, [{Pos, iolist_size(Data)} | Written]}}.

-decorate({?MODULE, handler, [], verbose}).
finalize({Fd, Written}) ->
    {ok, {Fd, lists:reverse(Written)}}.

-decorate({?MODULE, handler, [], verbose}).
to_disk_term({_Fd, Written}) ->
    {ok, Written}.

-decorate({?MODULE, handler, [], verbose}).
is_active_stream(_) ->
    false.
