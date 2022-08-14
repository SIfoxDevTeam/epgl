-type row_msg_type() :: insert | update | delete.
-type column_value_kind() :: null | internal_binary | unchanged | binary | text.
-type tuple_type() :: new | old | key.

-record(startup_msg, {
    version :: integer(),
    parameters :: map()
}).

-record(begin_msg, {
    lsn :: integer(),
    commit_time :: integer(),
    xid :: integer()
}).

-record(commit_msg, {
    flags :: integer(),
    commit_lsn :: integer(),
    end_lsn :: integer(),
    commit_time :: integer()
}).

-record(origin_msg, {
    origin_lsn :: integer(),
    origin_name :: binary()
}).

-record(relation_column, {
    flags :: integer(),
    name :: binary() | undefined,
    data_type_id :: integer() | undefined,
    atttypmod :: integer() | undefined
}).

-record(relation_msg, {
    id :: integer(),
    namespace :: binary(),
    name :: binary(),
    replica_identity :: integer() | undefined,
    num_columns :: integer(),
    columns :: [#relation_column{}]
}).

-record(type_msg, {
    id :: integer(),
    namespace :: binary(),
    name :: binary()
}).

-record(column_value, {
    kind :: column_value_kind(),
    value :: binary() | null | unchanged
}).

-record(row_msg, {
    msg_type :: row_msg_type(),
    relation_id :: integer(),
    num_columns :: integer(),
    columns :: [#column_value{}],
    old_columns :: [#column_value{}] | undefined,
    tuple_type :: tuple_type()
}).

-record(truncate_msg, {relation_ids :: [integer()],
                       relations_num :: integer(),
                       options :: integer()
                      }).

-define(int16, 1/big-signed-unit:16).
-define(int32, 1/big-signed-unit:32).
-define(int64, 1/big-signed-unit:64).
