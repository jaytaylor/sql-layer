/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.hapi.HapiUtils;
import com.akiban.util.ArgumentValidation;

public class SimpleHapiPredicate implements HapiPredicate {
    private final TableName tableName;
    private final String columnName;
    private final Operator op;
    private final String value;

    public SimpleHapiPredicate(TableName tableName, String columnName, Operator op, String value) {
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
        StringBuilder builder = new StringBuilder();
        builder.append(HapiUtils.escape(getColumnName()));
        builder.append(op);
        builder.append(HapiUtils.escape(getValue()));
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleHapiPredicate predicate = (SimpleHapiPredicate) o;

        return columnName.equals(predicate.columnName) && op == predicate.op && tableName.equals(predicate.tableName) && !(value != null ? !value.equals(predicate.value) : predicate.value != null);

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
