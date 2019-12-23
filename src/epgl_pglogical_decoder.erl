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

%% Decoder for logical replication messages of output plugin 'pglogical_output'
%% Messages format https://github.com/2ndQuadrant/pglogical/blob/eac3cf9eca6207726fbc1f32fc9ad132405936ec/doc/protocol.txt

-module(epgl_pglogical_decoder).

%% API
-export([decode/1]).

-include("epgl_int.hrl").

-define(MESSAGE_TYPE_BEGIN, $B).
-define(MESSAGE_TYPE_COMMIT, $C).
-define(MESSAGE_TYPE_INSERT, $I).
-define(MESSAGE_TYPE_UPDATE, $U).
-define(MESSAGE_TYPE_DELETE, $D).
-define(MESSAGE_TYPE_TABLE_METADATA, $R).
-define(MESSAGE_TYPE_STARTUP, $S).
-define(MESSAGE_TYPE_ORIGIN, $O).

%% tuple types
-define(TUPLE_TYPE_OLD, $K).
-define(TUPLE_TYPE_NEW, $N).

%% tuple value kind
-define(TUPLE_VALUE_KIND_NULL, $n).
-define(TUPLE_VALUE_KIND_UNCHANGED, $u).
-define(TUPLE_VALUE_KIND_TEXT, $t).
-define(TUPLE_VALUE_KIND_INTERNAL_BINARY, $i).
-define(TUPLE_VALUE_KIND_BINARY, $b).

%% Meta blocktype
-define(COLUMN, $C).

%% metadata block header
-define(COLUMN_NAME, $N).

-define(TUPLE_FORMAT, $T).
-define(ATTR_BLOCK, $A).

decode(<<?MESSAGE_TYPE_BEGIN:8, _Flags:8, Lsn:?int64, CommitTime:?int64, RemoteXID:?int32>>) ->
    Rec = #begin_msg{lsn = Lsn, commit_time = CommitTime, xid = RemoteXID},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_COMMIT:8, Flags:8, CommitLsn:?int64, EndLsn:?int64, CommitTime:?int64>>) ->
    Rec = #commit_msg{flags = Flags, commit_lsn = CommitLsn, end_lsn = EndLsn, commit_time = CommitTime},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_ORIGIN:8, _Flags:8, OriginLsn:?int64, Length:8, OriginIdentifier:Length/bytes>>) ->
    Rec = #origin_msg{origin_lsn = OriginLsn,
        origin_name =
            case Length of
                0 -> undefined;
                _ -> delete_terminating0(OriginIdentifier, Length)
            end},
    {ok, Rec};

decode(<<RowMessageType:8, _Flags:8, Relidentifier:?int32,
    TupleType:8, ?TUPLE_FORMAT:8, NumOfFields:?int16, TupleFieldValues/binary>>) when
    RowMessageType == ?MESSAGE_TYPE_INSERT;
    RowMessageType == ?MESSAGE_TYPE_DELETE;
    RowMessageType == ?MESSAGE_TYPE_UPDATE ->

    MsgType = decode_msg_type(RowMessageType),

    {Fields1, Rest} = decode_tuple_fields(TupleFieldValues, NumOfFields, []),

    {Fields, OldFields} =
        case MsgType =:= update andalso TupleType =:= ?TUPLE_TYPE_OLD andalso byte_size(Rest) > 0 of
            true ->
                %% in case of primary key update pglogical will send fields twice:
                %%     old fields with only primary key like for delete
                %%     and new fields like for normal update
                <<_TupleType2:8, ?TUPLE_FORMAT:8, NumOfFields2:?int16, TupleFieldValues2/binary>> = Rest,
                {Fields2, _} = decode_tuple_fields(TupleFieldValues2, NumOfFields2, []),
                {Fields2, Fields1};
            false ->
                {Fields1, undefined}
        end,

    Rec = #row_msg{
        msg_type = MsgType,
        relation_id = Relidentifier,
        num_columns = NumOfFields,
        columns = Fields,
        old_columns = OldFields,
        tuple_type = decode_tuple_type(TupleType)},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_TABLE_METADATA:8, _Flags:8, Relidentifier:?int32,
    Nspnamelength:8, Nspname1:Nspnamelength/bytes,
    Relnamelength:8, Relname1:Relnamelength/bytes,
    ?ATTR_BLOCK:8, Natts:?int16, Fields/binary>>) ->
    Rec = #relation_msg{id = Relidentifier,
        namespace = delete_terminating0(Nspname1, Nspnamelength),
        name = delete_terminating0(Relname1, Relnamelength),
        num_columns = Natts,
        columns = decode_meta_fields(Fields, [])},
    {ok, Rec};

decode(<<?MESSAGE_TYPE_STARTUP:8, Version:8, Parameters/binary>>) ->
    Rec = #startup_msg{version = Version,
        parameters = make_parameters_map(binary:split(Parameters, <<0>>, [global]), #{})},
    {ok, Rec};

decode(_Data) ->
    {error, unknown_msg}.


decode_tuple_fields(<<>>, _NumOfFields, Acc) ->
    {lists:reverse(Acc), <<>>};
decode_tuple_fields(Rest, 0, Acc) ->
    {lists:reverse(Acc), Rest};
decode_tuple_fields(<<?TUPLE_VALUE_KIND_NULL:8, Rest/binary>>, NumOfFields, Acc) ->
    decode_tuple_fields(Rest, NumOfFields - 1, 
        [#column_value{kind = decode_tuple_value_kind(?TUPLE_VALUE_KIND_NULL), value = null} | Acc]);
decode_tuple_fields(<<?TUPLE_VALUE_KIND_UNCHANGED:8, Rest/binary>>, NumOfFields, Acc) ->
    decode_tuple_fields(Rest, NumOfFields - 1, 
        [#column_value{kind = decode_tuple_value_kind(?TUPLE_VALUE_KIND_UNCHANGED), value = unchanged} | Acc]);
decode_tuple_fields(<<Kind:8, Length:32, Data:Length/bytes, Rest/binary>>, NumOfFields, Acc) ->
    Value =
        case Kind of
            ?TUPLE_VALUE_KIND_TEXT -> delete_terminating0(Data, Length);
            _ -> Data
        end,
    Field = #column_value{kind = decode_tuple_value_kind(Kind), value = Value},
    decode_tuple_fields(Rest, NumOfFields - 1, [Field | Acc]).

decode_meta_fields(<<>>, Acc) ->
    lists:reverse(Acc);
decode_meta_fields(<<?COLUMN:8, Flags:8, Rest1/binary>>, Acc) ->
    %% Flags - Currently, the only defined flag is 0x1 indicating that the column is part of the relation's identity key.
    {Rest2, ColumnBlock} = decode_meta_blocks(Rest1, #relation_column{flags = Flags}),
    decode_meta_fields(Rest2, [ColumnBlock | Acc]).

decode_meta_blocks(<<>>, Acc) ->
    {<<>>, Acc};
decode_meta_blocks(<<?COLUMN:8, _F:8, _/binary>> = Rest, Acc) ->
    {Rest, Acc};
decode_meta_blocks(<<?COLUMN_NAME:8, Length:?int16, Data:Length/bytes, Rest/binary>>, Acc) ->
    decode_meta_blocks(Rest, Acc#relation_column{name = delete_terminating0(Data, Length)}).

delete_terminating0(Value, Length) ->
    {Value1, _} = split_binary(Value, (Length - 1)), %% delete terminating \0
    Value1.

make_parameters_map([K, V | T], Acc) ->
    NewKey =
        try
            binary_to_existing_atom(K, latin1)
        catch
            _:_ ->
                binary_to_atom(K, latin1)
        end,
    make_parameters_map(T, Acc#{NewKey => V});
make_parameters_map([], Acc) ->
    Acc;
make_parameters_map([<<>>], Acc) ->
    Acc.

decode_tuple_type(?TUPLE_TYPE_NEW) -> new;
decode_tuple_type(?TUPLE_TYPE_OLD) -> old.

decode_msg_type(?MESSAGE_TYPE_INSERT) -> insert;
decode_msg_type(?MESSAGE_TYPE_UPDATE) -> update;
decode_msg_type(?MESSAGE_TYPE_DELETE) -> delete.

decode_tuple_value_kind(?TUPLE_VALUE_KIND_NULL) -> null;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_UNCHANGED) -> unchanged;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_TEXT) -> text;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_BINARY) -> binary;
decode_tuple_value_kind(?TUPLE_VALUE_KIND_INTERNAL_BINARY) -> internal_binary.