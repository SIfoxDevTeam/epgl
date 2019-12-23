-module(epgl_cast).

-export([cast/1]).

-include("epgl_int.hrl").

cast({_Type, #column_value{kind = null}}) -> null;
cast({_Type, #column_value{kind = unchanged}}) -> unchanged;
cast({Type, #column_value{kind = Kind, value = Value}}) when Kind =:= binary; Kind =:= internal_binary ->
    decode_binary(Type, Value, get(datetime_mod));
cast({Type, #column_value{kind = text, value = Value}}) -> cast({Type, Value});
cast({_Type, null}) -> null;
cast({Type, Value}) when
    Type =:= int2;
    Type =:= int4;
    Type =:= int8 -> binary_to_integer(Value);
cast({Type, Value}) when
    Type =:= float4;
    Type =:= float8 ->
    try
        binary_to_float(Value)
    catch
        error:badarg ->
            float(binary_to_integer(Value))
    end;
cast({bool, Value}) ->
    case Value of
        <<"t">> -> true;
        <<"f">> -> false
    end;
cast({Type, Value}) when
    Type =:= int4range;
    Type =:= {unknown_oid, 3926} -> %% XXX: int8range, epgsql does not support it now
    [From, To] = binary:split(Value, [<<"[">>, <<")">>, <<",">>], [global, trim_all]),
    {binary_to_integer(From), binary_to_integer(To)};
cast({bytea, <<"\\x", Rest/binary>>}) ->
    << <<(binary_to_integer(N, 16))>> || <<N:2/binary-unit:8>> <= Rest >>;
cast({time, Value}) ->
    [HH, MI, SS|_] = binary:split(Value, [<<".">>,<<":">>], [global]), %% we ignore microseconds
    {binary_to_integer(HH), binary_to_integer(MI), binary_to_integer(SS)};
cast({_Type, Value}) -> Value.


decode_binary(bool, <<1:1/big-signed-unit:8>>, _) -> true;
decode_binary(bool, <<0:1/big-signed-unit:8>>, _) -> false;
decode_binary(bpchar, <<C:1/big-unsigned-unit:8>>, _) -> C;
decode_binary(int2, <<N:1/big-signed-unit:16>>, _) -> N;
decode_binary(int4, <<N:1/big-signed-unit:32>>, _) -> N;
decode_binary(int8, <<N:1/big-signed-unit:64>>, _) -> N;
decode_binary(float4, <<N:1/big-float-unit:32>>, _) -> N;
decode_binary(float8, <<N:1/big-float-unit:64>>, _) -> N;
decode_binary(record, B, Codec) -> epgsql_binary:decode_record(B, record, Codec);
decode_binary(jsonb, <<1:8, Value/binary>>, _) -> Value;
decode_binary(Type, B, Codec) when
    Type =:= time;
    Type =:= timetz;
    Type =:= date;
    Type =:= timestamp;
    Type =:= timestamptz;
    Type =:= interval ->
    epgsql_codec_datetime:decode(B, Type, Codec);
decode_binary(uuid, B, Codec) -> epgsql_codec_uuid:decode(B, uuid, Codec);
decode_binary(hstore, B, Codec) -> epgsql_codec_hstore:decode(B, hstore, Codec);
decode_binary(inet, B, Codec) -> epgsql_codec_net:decode(B, inet, Codec);
decode_binary(cidr, B, Codec) -> epgsql_codec_net:decode(B, cidr, Codec);
decode_binary({array, Type}, B, Codec) -> epgsql_binary:decode_array(B, Type, Codec);
decode_binary(point, B, Codec) -> epgsql_codec_geometric:decode(B, point, Codec);
decode_binary(geometry, B, _) -> ewkb:decode_geometry(B);
decode_binary(int4range, B, Codec) -> epgsql_codec_intrange:decode(B, int4range, Codec);
decode_binary(int8range, B, Codec) -> epgsql_codec_intrange:decode(B, int8range, Codec);
decode_binary(_Other, Bin, _) -> Bin.
