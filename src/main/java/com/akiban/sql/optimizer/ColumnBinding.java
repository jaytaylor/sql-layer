/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.FromTable;
import com.akiban.sql.parser.ResultColumn;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.CharacterTypeAttributes;
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
            return getType(column, nullable);
        }
    }
    
    public static DataTypeDescriptor getType(Column column, boolean nullable)
            throws StandardException {
        Type aisType = column.getType();
        String typeName = aisType.name().toUpperCase();
        TypeId typeId = TypeId.getBuiltInTypeId(typeName);
        if (typeId == null)
            typeId = TypeId.getSQLTypeForJavaType(typeName);
        if (column.getNullable())
            nullable = true;
        switch (aisType.nTypeParameters()) {
        case 0:
            return new DataTypeDescriptor(typeId, nullable);
        case 1:
            {
                DataTypeDescriptor type = new DataTypeDescriptor(typeId, nullable, 
                                                                 column.getTypeParameter1().intValue());
                if (typeId.isStringTypeId() &&
                    (column.getCharsetAndCollation() != null)) {
                    CharacterTypeAttributes cattrs = 
                        new CharacterTypeAttributes(column.getCharsetAndCollation().charset(),
                                                    column.getCharsetAndCollation().collation(),
                                                    CharacterTypeAttributes.CollationDerivation.IMPLICIT);
                    type = new DataTypeDescriptor(type, cattrs);
                }
                return type;
            }
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
