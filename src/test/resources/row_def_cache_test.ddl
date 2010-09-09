
/*
schema row_def_cache_test baseid=100 groupschema _akiban_scan_rows_test;


group bgroup {
   table b {
     table bb (bb0, bb2, bb1, bb3)
   }
};

*/

create table b(
    b0 int,
    b1 int,
    b2 int,
    b3 int,
    b4 int,
    b5 int,
    primary key(b3, b2, b4, b1)
) engine = akibadb;

create table bb(
    bb0 int,
    bb1 int,
    bb2 int,
    bb3 int,
    bb4 int,
    bb5 int,
    primary key (bb0, bb5, bb3, bb2, bb4)
) engine = akibadb;

    
