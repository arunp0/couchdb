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

-module(couch_stream_cloud).

-compile([{parse_transform, decorators}]).

-export([
    foldl/3,
    foldl/4
]).

%% decorators
-export([handler/3]).

handler(Fun, Args, {FunName, Line}) -> 
    couch_log:info("~p:~p ~p called ~p ", [?MODULE, FunName, Line, Args]),
    Result = Fun(Args), 
    couch_log:info("~p:~p ~p Result is: ~p", [?MODULE, FunName, Line, Result]),
    Result.


-include_lib("couch/include/couch_db.hrl").

foldl_md5(Bin, {Md5Acc, UserFun, UserAcc}) ->
    NewMd5Acc = couch_hash:md5_hash_update(Md5Acc, Bin),
    {NewMd5Acc, UserFun, UserFun(Bin, UserAcc)}.

-decorate({?MODULE, handler, [], verbose}).
foldl({Engine, EngineState}, Fun, Acc) ->
    Engine:foldl(EngineState, Fun, Acc).

-decorate({?MODULE, handler, [], verbose}).
foldl(Engine, <<>>, Fun, Acc) ->
    foldl(Engine, Fun, Acc);
foldl(Engine, Md5, UserFun, UserAcc) ->
    InitAcc = {couch_hash:md5_hash_init(), UserFun, UserAcc},
    {Md5Acc, _, OutAcc} = foldl(Engine, fun foldl_md5/2, InitAcc),
    Md5 = couch_hash:md5_hash_final(Md5Acc),
    OutAcc.