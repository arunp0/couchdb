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

-module(mem3_reshard_job).


-behaviour(gen_server).


-export([
   start_link/1,
   jobfmt/1,
   pickfun/3,

   init/1,
   terminate/2,
   handle_call/3,
   handle_info/2,
   handle_cast/2,
   code_change/3,

   initial_copy/1,
   topoff/3,
   copy_local_docs/1,
   wait_source_close/1,
   source_delete/1,

   report/1
]).


-include("couch/include/couch_db.hrl").
-include("mem3_reshard.hrl").


start_link(#job{} = Job) ->
    gen_server:start_link(?MODULE, [Job], []).


jobfmt(#job{} = Job) ->
    #job{
        id = Id,
        source = #shard{name = Source},
        targets = Targets,
        split_state = State,
        job_state = JobState,
        pid = Pid
    } = Job,
    TargetCount = length(Targets),
    Msg = "#job{~s ~s /~B job:~s state:~s pid:~p}",
    Fmt = io_lib:format(Msg, [Id, Source, TargetCount, State, JobState, Pid]),
    lists:flatten(Fmt).


init([#job{} = Job0]) ->
    process_flag(trap_exit, true),
    Job = Job0#job{
        target_filters = build_target_filters(Job0#job.targets),
        pid = self(),
        time_started = mem3_reshard:now_sec(),
        workers = [],
        retries = 0
    },
    gen_server:cast(self(), maybe_recover),
    couch_log:notice("~p starting job ~s", [?MODULE, jobfmt(Job)]),
    {ok, Job}.


terminate(Reason, Job) ->
    couch_log:notice("~p terminate ~p ~p", [?MODULE, Reason, jobfmt(Job)]),
    ok.


handle_call(Call, From, Job) ->
    couch_log:notice("~p call ~p from: ~p", [?MODULE, Call, From]),
    {noreply, Job}.


handle_cast(maybe_recover, Job) ->
    {noreply, maybe_recover(Job)};

handle_cast(do_state, Job) ->
    case do_state(Job) of
        {ok, Job1} ->
            {noreply, Job1};
        {stop, Error, Job1} ->
            {stop, Error, Job1};
        {retry, Error, Job1} ->
            {noreply, retry_state(Error, Job1)}
    end;

handle_cast(Cast, Job) ->
    couch_log:notice("~p unknown cast ~p", [?MODULE, Cast]),
    {noreply, Job}.


handle_info(retry, #job{} = Job) ->
    couch_log:notice("~p retry timer expired ~p", [?MODULE, jobfmt(Job)]),
    handle_cast(do_state, Job);

handle_info({'EXIT', Pid, Reason}, #job{workers = Workers} = Job) ->
    case lists:member(Pid, Workers) of
        true ->
            Job1 = Job#job{workers = Workers -- [Pid]},
            worker_exited(Reason, Job1);
        false ->
            ErrMsg = "~p ~p pid exited reason: ~p job: ~s",
            couch_log:error(ErrMsg, [?MODULE, Pid, Reason, jobfmt(Job)]),
            {noreply, Job}
    end;

handle_info(Info, Job) ->
    couch_log:notice("~p info ~p", [?MODULE, Info]),
    {noreply, Job}.


code_change(_OldVsn, Job, _Extra) ->
    {ok, Job}.


-spec next_state(split_state()) -> split_state().
next_state(State) ->
    next_state(State, ?SPLIT_STATES).

next_state(State, [State, Next | _Rest]) ->
    Next;
next_state(completed, _) ->
    completed;
next_state(State, [_ | Rest]) ->
    next_state(State, Rest).


-spec maybe_recover(#job{}) -> #job{} | no_return().
maybe_recover(#job{split_state = new} = Job) ->
    Job1 = reset_targets(Job),
    switch_state(Job1, initial_copy);

maybe_recover(#job{split_state = initial_copy} = Job) ->
    Job1 = reset_targets(Job),
    switch_state(Job1, initial_copy);

maybe_recover(#job{split_state = topoff1} = Job) ->
    switch_state(Job, topoff1);

maybe_recover(#job{split_state = build_indices} = Job) ->
    switch_state(Job, topoff1);

maybe_recover(#job{split_state = topoff2} = Job) ->
    switch_state(Job, topoff1);

maybe_recover(#job{split_state = copy_local_docs} = Job) ->
    switch_state(Job, topoff1);

maybe_recover(#job{split_state = update_shardmap} = Job) ->
    switch_state(Job, update_shardmap);

maybe_recover(#job{split_state = wait_source_close} = Job) ->
    switch_state(Job, wait_source_close);

maybe_recover(#job{split_state = topoff3} = Job) ->
    switch_state(Job, wait_source_close);

maybe_recover(#job{split_state = source_delete} = Job) ->
    switch_state(Job, wait_source_close);

maybe_recover(#job{split_state = completed} = Job) ->
    switch_state(Job, completed);

maybe_recover(Job) ->
    couch_log:error("~p don't know how to recover ~s yet", [?MODULE, jobfmt(Job)]),
    erlang:error({invalid_split_job_recover_state, Job}).


-spec retry_state(term(), #job{}) -> #job{}.
retry_state(Error, #job{retries = Retries, state_info = Info} = Job0) ->
    Job = Job0#job{
        retries = Retries + 1,
        state_info = info_update(error, Error, Info)
    },
    couch_log:notice("~p retrying ~p ~p", [?MODULE, jobfmt(Job), Retries]),
    Job1 = report(Job),
    erlang:send_after(retry_interval_sec() * 1000, self(), retry),
    Job1.


-spec switch_state(#job{}, split_state()) -> #job{}.
switch_state(#job{manager = ManagerPid} = Job0, NewState) ->
    Info = Job0#job.state_info,
    Info1 = info_delete(error, Info),
    Info2 = info_delete(reason, Info1),
    Job = Job0#job{
        split_state = NewState,
        time_updated = mem3_reshard:now_sec(),
        retries = 0,
        state_info = Info2,
        workers = []
    },
    ok = mem3_reshard:checkpoint(ManagerPid, check_state(Job)),
    gen_server:cast(self(), do_state),
    Job.


-spec do_state(#job{}) -> #job{}.
do_state(#job{split_state = initial_copy} = Job) ->
    Pid = spawn_link(?MODULE, initial_copy, [Job]),
    {ok, report(Job#job{workers = [Pid]})};

do_state(#job{split_state = topoff1} = Job) ->
    do_state_topoff(Job);

do_state(#job{split_state = build_indices} = Job) ->
    case build_indices(Job#job.source, Job#job.targets) of
        {ok, []} ->
            {ok, switch_state(Job, next_state(build_indices))};
        {ok, Pids} when is_list(Pids) ->
            {ok, report(Job#job{workers = Pids})};
        {error, Error} ->
            maybe_retry(Job, max_retries(), Error)
    end;

do_state(#job{split_state = topoff2} = Job) ->
    do_state_topoff(Job);

do_state(#job{split_state = copy_local_docs} = Job) ->
    Pid = spawn_link(?MODULE, copy_local_docs, [Job]),
    {ok, report(Job#job{workers = [Pid]})};

do_state(#job{split_state = update_shardmap} = Job) ->
    Pid = spawn_link(mem3_reshard_dbdoc, update_shard_map, [Job]),
    {ok, report(Job#job{workers = [Pid]})};

do_state(#job{split_state = wait_source_close} = Job) ->
    Pid = spawn_link(?MODULE, wait_source_close, [Job]),
    {ok, report(Job#job{workers = [Pid]})};

do_state(#job{split_state = topoff3} = Job) ->
    do_state_topoff(Job);

do_state(#job{split_state = source_delete} = Job) ->
    ok = source_delete(Job),
    {ok, switch_state(Job, next_state(source_delete))};

do_state(#job{split_state = completed} = Job) ->
    couch_log:notice("~p : ~p COMPLETED, exiting normal", [?MODULE, jobfmt(Job)]),
    {stop, normal, Job};

do_state(#job{split_state = State} = Job) ->
    couch_log:error("~p do_state NOT IMPLEMENTED ~p", [?MODULE, jobfmt(Job)]),
    erlang:error({split_state_not_implemented, State, Job}).


-spec maybe_retry(#job{}, any(), integer()) ->
    {stop, any(), #job{}} | {retry, any(), #job{}}.
maybe_retry(#job{retries = Retries} = Job, Max, Error) when Retries =< Max ->
    {retry, Error, Job};

maybe_retry(#job{} = Job, _, Error) ->
    {stop, Error, Job}.


-spec report(#job{}) -> #job{}.
report(#job{manager = ManagerPid} = Job) ->
    Job1 = Job#job{time_updated = mem3_reshard:now_sec()},
    ok = mem3_reshard:report(ManagerPid, Job1),
    Job1.


-spec worker_exited(any(), #job{}) ->
    {noreply, #job{}} | {stop, any()} | {retry, any(), #job{}}.
worker_exited(normal, #job{split_state = State, workers = []} = Job) ->
    couch_log:notice("~p LAST worker exited ~p", [?MODULE, jobfmt(Job)]),
    {noreply, switch_state(Job, next_state(State))};

worker_exited(normal, #job{workers = Workers} = Job) when Workers =/= [] ->
    WaitingOn = length(Workers),
    Msg = "~p worker exited normal ~p, waiting on ~p more workers",
    couch_log:debug(Msg, [?MODULE, jobfmt(Job), WaitingOn]),
    {noreply, Job};

worker_exited(Reason, #job{} = Job0) ->
    couch_log:error("~p worker error ~p ~p", [?MODULE, jobfmt(Job0), Reason]),
    [begin unlink(Pid), exit(Pid, kill) end || Pid <- Job0#job.workers],
    Job = Job0#job{workers = []},
    case maybe_retry(Job, max_retries(), Reason) of
        {stop, Error, Job1} ->
            {stop, Error, Job1};
        {retry, Error, Job1} ->
            {noreply, retry_state(Error, Job1)}
    end.


% This is for belt and suspenders really. Call periodically to validate the
% state is one of the expected states.
-spec check_state(#job{}) -> #job{} | no_return().
check_state(#job{split_state = State} = Job) ->
    case lists:member(State, ?SPLIT_STATES) of
        true ->
            Job;
        false ->
            erlang:error({invalid_shard_split_state, State, Job})
    end.


initial_copy(#job{source = Source, targets = Targets0} = Job) ->
    #shard{name = SourceName} = Source,
    Targets = [{Range, Name} || #shard{range = Range, name = Name} <- Targets0],
    TMap = maps:from_list(Targets),
    couch_log:notice("~p initial_copy start ~s", [?MODULE, shardsstr(Source, Targets0)]),
    {ok, Seq} = couch_db_split:split(SourceName, TMap, fun pickfun/3),
    couch_log:notice("~p initial_copy of ~p finished @ ~p", [?MODULE, Source, Seq]),
    create_artificial_mem3_rep_checkpoints(Job, Seq).


create_artificial_mem3_rep_checkpoints(#job{} = Job, Seq) ->
    #job{source = #shard{name = SourceName}, targets = Targets} = Job,
    TNames = [TN || #shard{name = TN} <- Targets],
    Timestamp = list_to_binary(mem3_util:iso8601_timestamp()),
    couch_util:with_db(SourceName, fun(SDb) ->
        [couch_util:with_db(TName, fun(TDb) ->
            Doc = mem3_rep_checkpoint_doc(SDb, TDb, Filter, Timestamp, Seq),
            {ok, _} = couch_db:update_doc(SDb, Doc, []),
            {ok, _} = couch_db:update_doc(TDb, Doc, []),
            ok
        end) || {TName, Filter} <- lists:zip(TNames, Job#job.target_filters)]
    end),
    ok.


mem3_rep_checkpoint_doc(SourceDb, TargetDb, Filter, Timestamp, Seq) ->
    Node = atom_to_binary(node(), utf8),
    SourceUUID =  couch_db:get_uuid(SourceDb),
    TargetUUID = couch_db:get_uuid(TargetDb),
    History = {[
        {<<"source_node">>, Node},
        {<<"source_uuid">>, SourceUUID},
        {<<"source_seq">>, Seq},
        {<<"timestamp">>, Timestamp},
        {<<"target_node">>, Node},
        {<<"target_uuid">>, TargetUUID},
        {<<"target_seq">>, Seq}
    ]},
    Body = {[
        {<<"seq">>, Seq},
        {<<"target_uuid">>, TargetUUID},
        {<<"history">>, {[{Node, [History]}]}}
    ]},
    Id = mem3_rep:make_local_id(SourceUUID, TargetUUID, Filter),
    #doc{id = Id, body = Body}.


do_state_topoff(#job{} = Job) ->
    #job{source = Source, targets = Targets, target_filters = Filters} = Job,
    TZip = lists:zip(Targets, Filters),
    Pids = [spawn_link(?MODULE, topoff, [Source, T, F]) ||  {T, F} <- TZip],
    {ok, report(Job#job{workers = Pids})}.


-spec build_target_filters([#shard{}]) -> fun().
build_target_filters(Targets) when is_list(Targets) ->
    lists:map(fun(#shard{range = [Begin, End]}) ->
        fun(#full_doc_info{id = DocId}) ->
            HashKey = mem3_util:hash(DocId),
            if
                Begin =< HashKey andalso HashKey =< End -> keep;
                true -> discard
            end
        end
   end, Targets).


-spec topoff(#shard{}, #shard{}, fun()) -> ok | no_return().
topoff(#shard{} = Source, #shard{} = Target, Filter) ->
    couch_log:notice("~p topoff starting ~p", [?MODULE, shardsstr(Source, Target)]),
    Opts =  [{filter, Filter}, {batch_size, 2000}, {batch_count, all}],
    % Using a targets map here in order to avoid mem3_rep attempt to load
    % all target shards from the shard map, as target shards are not in the
    % shard map yet.
    case mem3_rep:go(Source, #{undefined => Target}, Opts) of
        {ok, Count} ->
            Args = [?MODULE, shardsstr(Source, Target), Count],
            couch_log:notice("~p topoff done ~s count: ~p", Args),
            ok;
        {error, Error} ->
            Args = [?MODULE, shardsstr(Source, Target), Error],
            couch_log:error("~p topoff ~s failed: ~p", Args),
            exit({error, Error})
    end.


pickfun(DocId, [[B, E] | _] = Ranges, {_M, _F, _A} = HashFun) when
        is_integer(B), is_integer(E), B =< E ->
    HashKey = mem3_hash:calculate(HashFun, DocId),
    Pred = fun([Begin, End]) ->
        Begin =< HashKey andalso HashKey =< End
    end,
    [Key] = lists:filter(Pred, Ranges),
    Key.


build_indices(#shard{name = Source}, Targets) ->
    {ok, DDocs} = mem3_reshard_index:design_docs(Source),
    Indices = mem3_reshard_index:target_indices(DDocs, Targets),
    mem3_reshard_index:spawn_builders(Indices).


copy_local_docs(#job{source = Source, targets = Targets0}) ->
    #shard{name = SourceName} = Source,
    Targets = [{Range, Name} || #shard{range = Range, name = Name} <- Targets0],
    TMap = maps:from_list(Targets),
    couch_log:notice("~p copy local docs start ~p -> ~p", [?MODULE, Source, Targets]),
    ok = couch_db_split:copy_local_docs(SourceName, TMap, fun pickfun/3),
    couch_log:notice("~p copy local docs finished for ~p", [?MODULE, Source]),
    ok.


wait_source_close(#job{source = #shard{name = Name}}) ->
    Timeout = config:get_integer("mem3_reshard",
        "source_close_timeout_sec", 600),
    couch_util:with_db(Name, fun(Db) ->
        Now = mem3_reshard:now_sec(),
        case wait_source_close(Db, 5, Now + Timeout) of
            true ->
                ok;
            false ->
                exit({error, source_db_close_timeout, Name, Timeout})
        end
    end).


wait_source_close(Db, SleepSec, UntilSec) ->
    case couch_db:monitored_by(Db) -- [self()] of
         [] ->
            true;
         [_ | _] ->
            Now = mem3_reshard:now_sec(),
            case Now < UntilSec of
                true ->
                    LogMsg = "~p : Waiting for source shard ~p to be closed",
                    couch_log:notice(LogMsg, [?MODULE, couch_db:name(Db)]),
                    timer:sleep(SleepSec * 1000),
                    wait_source_close(Db, SleepSec, UntilSec);
                false ->
                    false
            end
    end.


source_delete(#job{source = #shard{name = Name}}) ->
    case config:get_boolean("mem3_reshard", "delete_source", true) of
        true ->
            ok = couch_server:delete(Name, [?ADMIN_CTX]),
            couch_log:warning("~p : deleted source shard ~p", [?MODULE, Name]);
        false ->
            couch_log:notice("~p : not deleting source shard ~p", [?MODULE, Name])
    end,
    ok.


-spec max_retries() -> integer().
max_retries() ->
    config:get_integer("mem3_reshard", "max_retries", 1).


-spec retry_interval_sec() -> integer().
retry_interval_sec() ->
    config:get_integer("mem3_reshard", "retry_interval_sec", 10).


-spec info_update(atom(), any(), [tuple()]) -> [tuple()].
info_update(Key, Val, StateInfo) ->
    lists:keystore(Key, 1, StateInfo, {Key, Val}).


-spec info_delete(atom(), [tuple()]) -> [tuple()].
info_delete(Key, StateInfo) ->
    lists:keydelete(Key, 1, StateInfo).


-spec shardsstr(#shard{}, #shard{} | [#shard{}]) -> string().
shardsstr(#shard{name = SourceName}, #shard{name = TargetName}) ->
    lists:flatten(io_lib:format("~s -> ~s", [SourceName, TargetName]));

shardsstr(#shard{name = SourceName}, Targets) ->
    TNames = [TN || #shard{name = TN} <- Targets],
    TargetsStr = string:join([binary_to_list(T) || T <- TNames], ","),
    lists:flatten(io_lib:format("~s -> ~s", [SourceName, TargetsStr])).


-spec reset_targets(#job{}) -> #job{}.
reset_targets(#job{source = Source, targets = Targets} = Job) ->
    ShardNames = [N || #shard{name = N} <- mem3:shards(mem3:dbname(Source))],
    lists:map(fun(#shard{name = Name}) ->
        case {couch_server:exists(Name), lists:member(Name, ShardNames)} of
            {_, true} ->
                % Should never get here but if we do crash and don't continue
                LogMsg = "~p : ~p target unexpectedly found in shard map ~p",
                couch_log:error(LogMsg, [?MODULE, jobfmt(Job), Name]),
                erlang:error({target_present_in_shard_map, Name});
            {true, false} ->
                LogMsg = "~p : ~p resetting ~p target when recovering",
                couch_log:warning(LogMsg, [?MODULE, jobfmt(Job), Name]),
                ok = couch_db_split:cleanup_target(Source#shard.name, Name);
            {false, false} ->
                ok
        end
    end, Targets),
    Job.
