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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.FromTable;
import com.akiban.sql.parser.ResultColumn;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;

/**
 * A column binding: stored in the UserData of a ColumnReference and
 * referring to a column from one of the tables in some FromList,
 * either as a DDL column (which may not be mentioned in any select
 * list) or a result column in a subquery.
 */
public class ColumnBinding 
{
    private FromTable fromTable;
    private Column column;
    private ResultColumn resultColumn;
    private boolean nullable;
        
    public ColumnBinding(FromTable fromTable, Column column, boolean nullable) {
        this.fromTable = fromTable;
        this.column = column;
        this.nullable = nullable;
    }
    public ColumnBinding(FromTable fromTable, ResultColumn resultColumn) {
        this.fromTable = fromTable;
        this.resultColumn = resultColumn;
    }

    public FromTable getFromTable() {
        return fromTable;
    }

    public Column getColumn() {
        return column;
    }

    /** Is the column nullable by virtue of its table being in an outer join? */
    public boolean isNullable() {
        return nullable;
    }

    public ResultColumn getResultColumn() {
        return resultColumn;
    }

    public DataTypeDescriptor getType() throws StandardException {
        if (resultColumn != null) {
            return resultColumn.getType();
        }
        else {
            Type aisType = column.getType();
            TypeId typeId = TypeId.getBuiltInTypeId(aisType.name().toUpperCase());
            if (typeId == null)
                typeId = TypeId.getSQLTypeForJavaType(aisType.name());
            boolean nullable = this.nullable || column.getNullable();
            switch (aisType.nTypeParameters()) {
            case 0:
                return new DataTypeDescriptor(typeId, nullable);
            case 1:
                return new DataTypeDescriptor(typeId, nullable, 
                                              column.getTypeParameter1().intValue());
            case 2:
                {
                    int precision = column.getTypeParameter1().intValue();
                    int scale = column.getTypeParameter2().intValue();
                    int maxWidth = DataTypeDescriptor.computeMaxWidth(precision, scale);
                    return new DataTypeDescriptor(typeId, precision, scale, 
                                                  nullable, maxWidth);
                }
            default:
                assert false;
                return new DataTypeDescriptor(typeId, nullable);
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        if (resultColumn != null) {
            result.append(resultColumn.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(resultColumn.hashCode()));
        }
        else
            result.append(column);
        if (fromTable != null) {
            result.append(" from ");
            result.append(fromTable.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(fromTable.hashCode()));
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ColumnBinding)) return false;
        ColumnBinding other = (ColumnBinding)obj;
        return ((fromTable == other.fromTable) &&
                (column == other.column) &&
                (resultColumn == other.resultColumn));
    }

    @Override
    public int hashCode() {
        int hash = fromTable.hashCode();
        if (column != null)
            hash += column.hashCode();
        if (resultColumn != null)
            hash += resultColumn.hashCode();
        return hash;
    }

}
