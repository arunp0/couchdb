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

-module(couch_bt_engine_stream_cloud).

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

-define(DEFAULT_FD_SEPERATOR, "##").
-define(SIZE_BLOCK, 5242880). % 5 MiB

-include("erlcloud/include/erlcloud_aws.hrl").

%% decorators
-export([handler/3]).


handler(Fun, Args,  {FunName, Line}) -> 
    couch_log:debug("~p:~p ~p  Args is: ~p ~n ~n", [?MODULE, FunName, Line, Args]),
    Result = Fun(Args),
    couch_log:debug("~p:~p ~p  Result is: ~p ~n ~n", [?MODULE, FunName, Line, Result]),
    Result.


get_config() ->
    {ok, Config} = erlcloud_aws:profile(),


-decorate({?MODULE, handler, [], verbose}).
start_multipart(Bucket, Key)->
    Config = get_config(),
    {ok, Data} =  erlcloud_s3:start_multipart(Bucket, Key, [], [], Config),
    UploadId = proplists:get_value(uploadId, Data),
    couch_log:info("~p called ~p ~p ~p", [?MODULE, Bucket, Key, UploadId]),
    binary_to_list(list_to_binary([Bucket, ?DEFAULT_FD_SEPERATOR , Key, ?DEFAULT_FD_SEPERATOR, UploadId])).


-decorate({?MODULE, handler, [], verbose}).
read_binary(Pos) ->
    Config = get_config(),
    [Bucket, Key, StartByte, EndByte] = string:tokens(Pos, ?DEFAULT_FD_SEPERATOR),
    Range = "bytes=" ++ StartByte ++ "-" ++ EndByte,
    Options = [{range, Range}],
    Body = erlcloud_s3:get_object(Bucket, Key, Options, Config),
    proplists:get_value(content, Body).


-decorate({?MODULE, handler, [], verbose}).
foldl({_Fd, []}, _Fun, Acc) ->
    Acc;

foldl({Fd, [{Pos, _} | Rest]}, Fun, Acc) ->
    foldl({Fd, [Pos | Rest]}, Fun, Acc);

foldl({Fd, [Bin | Rest]}, Fun, Acc) when is_binary(Bin) ->
    % We're processing the first bit of data
    % after we did a seek for a range fold.
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc));

foldl({Fd, [Pos | Rest]}, Fun, Acc) when is_list(Pos) ->
    Bin = read_binary(Pos),
    foldl({Fd, Rest}, Fun, Fun(Bin, Acc)).


-decorate({?MODULE, handler, [], verbose}).
seek({Fd, [{Pos, Length} | Rest]}, Offset) ->
    case Length =< Offset of
        true ->
            seek({Fd, Rest}, Offset - Length);
        false ->
            seek({Fd, [Pos | Rest]}, Offset)
    end;

seek({Fd, [Pos | Rest]}, Offset) when is_list(Pos) ->
    Bin = read_binary(Pos),
    case iolist_size(Bin) =< Offset of
        true ->
            seek({Fd, Rest}, Offset - size(Bin));
        false ->
            <<_:Offset/binary, Tail/binary>> = Bin,
            {ok, {Fd, [Tail | Rest]}}
    end.


-decorate({?MODULE, handler, [], verbose}).
write({Fd, Written}, Data) ->
    Config = get_config(),
    [Bucket, Key, UploadId] = string:tokens(Fd, ?DEFAULT_FD_SEPERATOR),
    PartNumber = length(Written) + 1,
    {ok, Part} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNumber, Data, [], Config),
    ETag = proplists:get_value(etag, Part),
    PartData = {PartNumber, ETag},
    {ok, {Fd, [PartData | Written]}}.


get_disk_term(Fd, DiskTermSize, ContentLength) -> 
    _Fd = Fd ++ ?DEFAULT_FD_SEPERATOR,
    Written1 = [{_Fd ++ integer_to_list((H-1)*?SIZE_BLOCK) ++ ?DEFAULT_FD_SEPERATOR 
        ++ integer_to_list((H*?SIZE_BLOCK)-1), ?SIZE_BLOCK} || H <- lists:seq(1, DiskTermSize)],
    Left = ContentLength - (DiskTermSize * ?SIZE_BLOCK),
    Written2 = case Left of
        0 -> Written1;
        _ -> Written1 ++ [{_Fd ++ integer_to_list(DiskTermSize*?SIZE_BLOCK) ++ ?DEFAULT_FD_SEPERATOR 
                ++ integer_to_list(ContentLength-1), ?SIZE_BLOCK}]
        end,
    Written2.


-decorate({?MODULE, handler, [], verbose}).
finalize({Fd, Written}) ->
    Config = get_config(),
    [Bucket, Key, UploadId] = string:tokens(Fd, ?DEFAULT_FD_SEPERATOR),
    ETags = lists:reverse(Written),
    erlcloud_s3:complete_multipart(Bucket, Key, UploadId, ETags, [], Config), 
    MetaData = erlcloud_s3:get_object_metadata(Bucket, Key, Config),
    {ContentLength, _} = string:to_integer(proplists:get_value(content_length, MetaData)),
    DiskTermSize = (ContentLength div ?SIZE_BLOCK),
    _Fd = binary_to_list(list_to_binary([Bucket, ?DEFAULT_FD_SEPERATOR , Key])),
    Written1 = get_disk_term(_Fd, DiskTermSize, ContentLength),
    {ok, {Fd, Written1}}.


-decorate({?MODULE, handler, [], verbose}).
to_disk_term({_Fd, Written}) ->
    {ok, Written}.


-decorate({?MODULE, handler, [], verbose}).
is_active_stream(_) ->
    % Config = get_config(),
    % [Bucket, Key, UploadId] = string:tokens(Fd, ?DEFAULT_FD_SEPERATOR),
    true.