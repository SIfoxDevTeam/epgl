%%%
%%%  Copyright 2017 Sifox
%%%
%%%  Licensed under the Apache License, Version 2.0 (the "License");
%%%  you may not use this file except in compliance with the License.
%%%  You may obtain a copy of the License at
%%%
%%%      http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%  Unless required by applicable law or agreed to in writing, software
%%%  distributed under the License is distributed on an "AS IS" BASIS,
%%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%  See the License for the specific language governing permissions and
%%%  limitations under the License.
%%%

%% Decoder for logical replication messages of output plugin 'pgoutput'
%% Messages format https://www.postgresql.org/docs/10/static/protocol-logicalrep-message-formats.html

-module(epgl_pgoutput_decoder).

%% API
-export([decode/1]).

-include("epgl_int.hrl").

%% logical replication message types
-define(MESSAGE_TYPE_BEGIN, $B).
-define(MESSAGE_TYPE_COMMIT, $C).
-define(MESSAGE_TYPE_ORIGIN, $O).
-define(MESSAGE_TYPE_RELATION, $R).
-define(MESSAGE_TYPE_TYPE, $Y).
-define(MESSAGE_TYPE_INSERT, $I).
-define(MESSAGE_TYPE_UPDATE, $U).
-define(MESSAGE_TYPE_DELETE, $D).
-define(MESSAGE_TYPE_TRUNCATE, $T).

%% tuple types
-define(TUPLE_TYPE_OLD, $O).
-define(TUPLE_TYPE_NEW, $N).
-define(TUPLE_TYPE_KEY, $K).

%% tuple value kind
-define(TUPLE_VALUE_KIND_NULL, $n).
-define(TUPLE_VALUE_KIND_UNCHANGED, $u).
-define(TUPLE_VALUE_KIND_TEXT, $t).

decode(<<?MESSAGE_TYPE_BEGIN:8, Lsn:?int64, CommitTime:?int64, XID:?int32>>) ->
    Rec = #begin_msg{lsn = Lsn, commit_time = CommitTime, xid = XID},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_COMMIT:8, Flags:8, CommitLsn:?int64, EndLsn:?int64, CommitTime:?int64>>) ->
    Rec = #commit_msg{flags = Flags, commit_lsn = CommitLsn, end_lsn = EndLsn, commit_time = CommitTime},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_ORIGIN:8, OriginLsn:?int64, OriginNameData/binary>>) ->
    [OriginName, _Rest] = decode_string(OriginNameData),
    Rec = #origin_msg{origin_lsn = OriginLsn, origin_name = OriginName},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_RELATION:8, RelationId:?int32, Rest1/binary>>) ->
    [Namespace, Rest2] = decode_string(Rest1),
    [RelationName, Rest3] = decode_string(Rest2),
    <<ReplicaIdentity:8, NumColumns:?int16, Columns/binary>> = Rest3,
    Rec = #relation_msg{
        id = RelationId,
        namespace = Namespace,
        name = RelationName,
        replica_identity = ReplicaIdentity,
        num_columns = NumColumns,
        columns = decode_relation_columns(Columns, [])},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_TYPE:8, Id:?int32, Rest1/binary>>) ->
    [Namespace, Rest2] = decode_string(Rest1),
    [Name, _Rest3] = decode_string(Rest2),
    Rec = #type_msg{
        id = Id,
        namespace = Namespace,
        name = Name},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_INSERT:8, RelationId:?int32, ?TUPLE_TYPE_NEW:8, NumColumns:?int16, ColumnsData/binary>>) ->
    {ColumnValues, <<>>} = decode_tuple_columns(ColumnsData, NumColumns, []),

    Rec = #row_msg{
        msg_type = decode_msg_type(?MESSAGE_TYPE_INSERT),
        relation_id = RelationId,
        num_columns = NumColumns,
        columns = ColumnValues,
        tuple_type = decode_tuple_type(?TUPLE_TYPE_NEW)},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_UPDATE:8, RelationId:?int32, TupleType:8, NumColumns1:?int16, ColumnsData1/binary>>) ->
    case TupleType of
        ?TUPLE_TYPE_NEW ->
            {ColumnValues, <<>>} = decode_tuple_columns(ColumnsData1, NumColumns1, []),
            Rec = #row_msg{
                msg_type = decode_msg_type(?MESSAGE_TYPE_UPDATE),
                relation_id = RelationId,
                num_columns = NumColumns1,
                columns = ColumnValues,
                old_columns = undefined,
                tuple_type = decode_tuple_type(TupleType)},
            {ok, Rec};
        TupleType when TupleType =:= ?TUPLE_TYPE_OLD; TupleType =:= ?TUPLE_TYPE_KEY ->
            {ColumnValuesOld, <<?TUPLE_TYPE_NEW:8, NumColumns2:?int16, ColumnsData2/binary>>}
                = decode_tuple_columns(ColumnsData1, NumColumns1, []),
            {ColumnValues, <<>>} = decode_tuple_columns(ColumnsData2, NumColumns2, []),
            Rec = #row_msg{
                msg_type = decode_msg_type(?MESSAGE_TYPE_UPDATE),
                relation_id = RelationId,
                num_columns = NumColumns1,
                columns = ColumnValues,
                old_columns = ColumnValuesOld,
                tuple_type = decode_tuple_type(TupleType)},
            {ok, Rec}
    end;

decode(<<?MESSAGE_TYPE_DELETE:8, RelationId:?int32, TupleType:8, NumColumns:?int16, ColumnsData/binary>>) ->
    {ColumnValues, <<>>} = decode_tuple_columns(ColumnsData, NumColumns, []),

    Rec = #row_msg{
        msg_type = decode_msg_type(?MESSAGE_TYPE_DELETE),
        relation_id = RelationId,
        num_columns = NumColumns,
        columns = ColumnValues,
        tuple_type = decode_tuple_type(TupleType)},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_TRUNCATE:8, NumRelations:?int32, Options:8, Rst/binary>>) ->
    RelationIds = [Id || <<Id:?int32>> <= Rst],
    Rec = #truncate_msg{
             relation_ids = RelationIds,
             relations_num = NumRelations,
             options = Options
            },
    {ok, Rec};

decode(_Data) ->
    {error, unknown_msg}.

decode_tuple_columns(<<>>, _NumColumns, Acc) ->
    {lists:reverse(Acc), <<>>};
decode_tuple_columns(Rest, 0, Acc) ->
    {lists:reverse(Acc), Rest};
decode_tuple_columns(<<Kind:8, Rest/binary>>, NumColumns, Acc)
    when Kind =:= ?TUPLE_VALUE_KIND_NULL; Kind =:= ?TUPLE_VALUE_KIND_UNCHANGED ->
    decode_tuple_columns(Rest, NumColumns - 1,
        [#column_value{kind = decode_tuple_value_kind(Kind),
            value = decode_tuple_value_kind(Kind)} | Acc]);
decode_tuple_columns(<<?TUPLE_VALUE_KIND_TEXT:8, Length:?int32, Data:Length/bytes, Rest/binary>>, NumColumns, Acc) ->
    decode_tuple_columns(Rest, NumColumns - 1,
        [#column_value{kind = decode_tuple_value_kind(?TUPLE_VALUE_KIND_TEXT), value = Data} | Acc]).

decode_relation_columns(<<>>, Acc) ->
    lists:reverse(Acc);
decode_relation_columns(<<Flags:8, Rest1/binary>>, Acc) ->
    [Name, Rest2] = decode_string(Rest1),
    <<DataTypeId:?int32, Atttypmod:?int32, Rest3/binary>> = Rest2,
    decode_relation_columns(Rest3, [
        #relation_column{
            flags = Flags,
            name = Name,
            data_type_id = DataTypeId,
            atttypmod = Atttypmod
        } | Acc]).

%% decode a single null-terminated string
decode_string(Bin) ->
    binary:split(Bin, <<0>>).

decode_tuple_type(?TUPLE_TYPE_NEW) -> new;
decode_tuple_type(?TUPLE_TYPE_KEY) -> key;
decode_tuple_type(?TUPLE_TYPE_OLD) -> old.

decode_msg_type(?MESSAGE_TYPE_INSERT) -> insert;
decode_msg_type(?MESSAGE_TYPE_UPDATE) -> update;
decode_msg_type(?MESSAGE_TYPE_DELETE) -> delete.

decode_tuple_value_kind(?TUPLE_VALUE_KIND_NULL) -> null;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_UNCHANGED) -> unchanged;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_TEXT) -> text.
