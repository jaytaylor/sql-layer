package com.akiban.cserver.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.util.ArgumentValidation;

public class SimplePredicate implements HapiGetRequest.Predicate {
    private final TableName tableName;
    private final String columnName;
    private final Operator op;
    private final String value;

    public SimplePredicate(TableName tableName, String columnName, Operator op, String value) {
        ArgumentValidation.notNull("table name", tableName);
        ArgumentValidation.notNull("column name", columnName);
        ArgumentValidation.notNull("operator", op);
        this.tableName = tableName;
        this.columnName = columnName;
        this.op = op;
        this.value = value;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public Operator getOp() {
        return op;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return appendToSB(new StringBuilder("Predicate["), null).append(']').toString();
    }

    @Override
    public StringBuilder appendToSB(StringBuilder builder, TableName usingTable) {
        if (usingTable == null || !usingTable.equals(tableName)) {
            tableName.escape(builder, usingTable == null ? null : usingTable.getSchemaName());
            builder.append('.');
        }
        builder.append(columnName).append(op).append(value);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimplePredicate predicate = (SimplePredicate) o;

        if (!columnName.equals(predicate.columnName)) return false;
        if (op != predicate.op) return false;
        if (!tableName.equals(predicate.tableName)) return false;
        if (value != null ? !value.equals(predicate.value) : predicate.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + columnName.hashCode();
        result = 31 * result + op.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
