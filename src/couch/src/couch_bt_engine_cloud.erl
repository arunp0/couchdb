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

-module(couch_bt_engine_cloud).

-compile([{parse_transform, decorators}]).

-export([
    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2
]).

-include("couch_bt_engine.hrl").

%% decorators
-export([handler/3]).

handler(Fun, Args,  {FunName, Line}) -> 
    couch_log:info("~p:~p ~p called", [?MODULE, FunName, Line]),
    Result = Fun(Args),
    couch_log:info("~p:~p ~p  Result is: ~p", [?MODULE, FunName, Line, Result]),
    Result.

get_dbname(FilePath) -> 
    FilePathList = filename:split(FilePath),
    PureFN = lists:last(FilePathList),
    [DbName|_] = string:tokens(PureFN, "."),
    DbName.

-decorate({?MODULE, handler, [], verbose}).
open_write_stream(#st{} = St, Options) ->
    #st{filepath=FilePath} = St,
    DbName = get_dbname(FilePath),
    Bucket = "couch-test-data-files", %TODO: bucket name generated based on db name
    Key = binary_to_list(couch_uuids:new()),
    Fd = couch_bt_engine_stream:start_multipart(Bucket, Key),
    couch_stream:open({couch_bt_engine_stream, {Fd, []}}, Options).


-decorate({?MODULE, handler, [], verbose}).
open_read_stream(#st{} = St, StreamSt) ->
    {ok, {couch_bt_engine_stream, {St#st.fd, StreamSt}}}.


-decorate({?MODULE, handler, [], verbose}).
is_active_stream(#st{} = St, {couch_bt_engine_stream, {Fd, _}}) ->
    couch_bt_engine_stream:is_active_stream(Fd);
is_active_stream(_, _) ->
    false.
