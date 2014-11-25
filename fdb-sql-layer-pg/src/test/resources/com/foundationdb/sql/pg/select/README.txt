explain-2: explain group joins

explain-3: explain INSERT

select-0: single column of primary key match

select-1: all fields of single table

select-1c: sorted covering index

select-1n: name column

select-2: index on grandchild table

select-2a: index equality condition on grandchild table

select-2b: index range inequalities on grandchild

select-2p: index with parameter

select-3: index and select condition on leaf table

select-3p: two index parameters

select-4: inequality on leaf

select-4c: covering group index

select-5: ordering with join

select-5l: ordering with limit

select-6: index one branch, flatten another

select-7: index one branch, product both

select-8: constant in select list

select-9: LEFT join from table index

select-9g: LEFT join covering group index

select-10: group index for condition and ordering

select-10r: reverse ordering

select-11: group index ordering only

select-11m: mixed-mode ordering

select-11r: reverse ordering

select-12: reverse sort multiple fields

select-13: sorting without limit

select-13l: sorting with limit

select-14: IN using index

select-14n: IN not using index

select-15: aggregation with GROUP BY

select-16: aggregation matching no rows, with GROUP BY

select-17: aggregation matching no rows, no GROUP BY

select-18a: int (1) as boolean in WHERE

select-18n: int (0) as boolean in WHERE

select-19: group index with one inequality

select-20: IN with parameter

select-21: subquery for value

select-22: EXISTS as expression (extra OR prevents unnesting)

select-23: IN / ANY as expression

select-23n: NOT IN

select-24: RIGHT join

select-24g: RIGHT join group index

select-25u: USER environment functions

select-26: index range

select-27: side branch with outer join

select-28: IN with key comparison

select-29: Sort from Select

types: select all typed fields

types_a_date: match literal
types_a_datetime:
types_a_decimal:
types_a_double:
types_a_float:
types_a_int:
types_a_text:
types_a_time:
types_a_timestamp:
types_a_udecimal:
types_a_udouble:
types_a_ufloat:
types_a_uint:
types_a_varchar:
types_a_year:

types_a_date_p: match parameter
types_a_datetime_p:
types_a_decimal_p:
types_a_double_p:
types_a_float_p:
types_a_int_p:
types_a_text_p:
types_a_time_p:
types_a_timestamp_p:
types_a_udecimal_p:
types_a_udouble_p:
types_a_ufloat_p:
types_a_uint_p:
types_a_varchar_p:
types_a_year_p:

types_i_date: match indexed with literal
types_i_datetime:
types_i_decimal:
types_i_double:
types_i_float:
types_i_int:
types_i_time:
types_i_timestamp:
types_i_udecimal:
types_i_udouble:
types_i_ufloat:
types_i_uint:
types_i_varchar:
types_i_year:

types_i_date_p: match indexed with parameter
types_i_datetime_p:
types_i_decimal_p:
types_i_double_p:
types_i_float_p:
types_i_int_p:
types_i_time_p:
types_i_timestamp_p:
types_i_udecimal_p:
types_i_udouble_p:
types_i_ufloat_p:
types_i_uint_p:
types_i_varchar_p:
types_i_year_p:
