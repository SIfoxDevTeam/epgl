-type change_type() :: insert | update | delete.

-record(row, {
    table_name :: string(),
    change_type :: change_type(),
    fields :: list()
}).