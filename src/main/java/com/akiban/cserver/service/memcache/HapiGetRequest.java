package com.akiban.cserver.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class HapiGetRequest {

    public static class Predicate {
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
        private final TableName tableName;
        private final String columnName;
        private final Operator op;
        private final String value;

        public Predicate(TableName tableName, String columnName, Operator op, String value) {
            ArgumentValidation.notNull("table name", tableName);
            ArgumentValidation.notNull("column name", columnName);
            ArgumentValidation.notNull("operator", op);
            ArgumentValidation.notNull("value", value);
            this.tableName = tableName;
            this.columnName = columnName;
            this.op = op;
            this.value = value;
        }

        @Override
        public String toString() {
            return appendToSB(new StringBuilder("Predicate["), null).append(']').toString();
        }

        public StringBuilder appendToSB(StringBuilder builder, TableName usingTable) {
            if (usingTable == null || !usingTable.equals(tableName)) {
                tableName.escape(builder, usingTable.getSchemaName());
                builder.append('.');
            }
            builder.append(columnName).append(op).append(value);
            return builder;
        }
    }

    HapiGetRequest()
    {}

    private String schema;
    private String table;
    private String usingTable;
    private TableName usingTableName;
    private final List<Predicate> predicates = new ArrayList<Predicate>();

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public TableName getUsingTable() {
        if (usingTableName == null) {
            usingTableName = new TableName(getSchema(), usingTable);
        }
        return usingTableName;
    }

    public List<Predicate> getPredicates() {
        return Collections.unmodifiableList(predicates);
    }

    void setTable(String table) {
        this.table = table;
        this.usingTable = table;
    }

    void setUsingTable(String usingTable) {
        this.usingTable = usingTable;
    }

    void setSchema(String schema) {
        this.schema = schema;
    }

    void addPredicate(String columnName, Predicate.Operator operator, String value) {
        predicates.add( new Predicate(getUsingTable(), columnName, operator, value) );
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("HapiGetRequest[ ");
        builder.append(schema).append(':').append(table).append(':');
        final TableName using = getUsingTable();
        if (!using.getTableName().equals(getTable())) {
            builder.append('(').append(using.getTableName()).append(')');
        }
        for (Predicate predicate : predicates) {
            predicate.appendToSB(builder, using).append(',');
        }
        builder.setLength(builder.length()-1);
        builder.append(" ]");
        return builder.toString();
    }

    void validate() {
        //TODO

    }
}
