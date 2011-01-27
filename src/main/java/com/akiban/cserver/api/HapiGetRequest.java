package com.akiban.cserver.api;

import com.akiban.ais.model.TableName;

import java.util.List;

public interface HapiGetRequest {
    String getSchema();

    String getTable();

    TableName getUsingTable();

    List<Predicate> getPredicates();

    interface Predicate {
        TableName getTableName();

        String getColumnName();

        Operator getOp();

        String getValue();

        StringBuilder appendToSB(StringBuilder builder, TableName usingTable);

        public enum Operator {
            EQ("=="),
            NE("!="),
            GT(">"),
            GTE(">="),
            LT("<"),
            LTE("<=")
            ;

            final private String toString;
            Operator(String toString) {
                this.toString = toString;
            }

            @Override
            public String toString() {
                return toString;
            }
        }
    }
}
