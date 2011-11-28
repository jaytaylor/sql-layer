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
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;

import java.sql.Types;

public final class TypesTranslation {
    public static AkType sqlTypeToAkType(DataTypeDescriptor descriptor) {
        switch (descriptor.getJDBCTypeId()) {
        case Types.BIGINT:  return AkType.U_BIGINT;
        case Types.BINARY:  return AkType.VARBINARY;
        case Types.BLOB:    return AkType.VARBINARY;
        case Types.BOOLEAN: return AkType.BOOL;
        case Types.CHAR:    return AkType.VARCHAR;
        case Types.CLOB:    return AkType.TEXT;
        case Types.DATE:    return AkType.DATE;
        case Types.DECIMAL: return AkType.DECIMAL;
        case Types.DOUBLE:  return AkType.DOUBLE;
        case Types.FLOAT:   return AkType.FLOAT;
        case Types.INTEGER: return AkType.LONG;
        case Types.NULL:    return AkType.NULL;
        case Types.NUMERIC: return AkType.DECIMAL;
        case Types.REAL:    return AkType.DOUBLE;
        case Types.SMALLINT: return AkType.LONG;
        case Types.TIME:    return AkType.TIME;
        case Types.TIMESTAMP: return AkType.TIMESTAMP;
        case Types.TINYINT: return AkType.LONG;
        case Types.VARBINARY: return AkType.VARBINARY;
        case Types.VARCHAR: return AkType.VARCHAR;
        case Types.JAVA_OBJECT:
            String name = descriptor.getFullSQLTypeName();
            for (com.akiban.ais.model.Type aisType : com.akiban.ais.model.Types.types()) {
                if (aisType.name().equalsIgnoreCase(name)) {
                    return aisType.akType();
                }
            }
            return AkType.valueOf(name.toUpperCase());
        default:
            throw new UnsupportedOperationException(
                    "unsupported type id: " + descriptor.getJDBCTypeId()
                            + " (" + descriptor.getFullSQLTypeName() + ')'
            );
//        case Types.ARRAY: return
//        case Types.BIT: return
//        case Types.DATALINK: return
//        case Types.DISTINCT: return
//        case Types.JAVA_OBJECT: return
//        case Types.LONGNVARCHAR: return
//        case Types.LONGVARBINARY: return
//        case Types.LONGVARCHAR: return
//        case Types.NCHAR: return
//        case Types.NCLOB: return
//        case Types.NVARCHAR: return
//        case Types.OTHER: return
//        case Types.REF: return
//        case Types.ROWID: return
//        case Types.SQLXML: return
//        case Types.STRUCT: return
        }
    }

    private TypesTranslation() {}
}
