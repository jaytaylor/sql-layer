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
import com.akiban.sql.types.TypeId;

import java.sql.Types;

public final class TypesTranslation {
    public static AkType sqlTypeToAkType(DataTypeDescriptor descriptor) {
        TypeId typeId = descriptor.getTypeId();
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            return AkType.BOOL;
        case TypeId.FormatIds.CHAR_TYPE_ID:
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            return AkType.VARCHAR;
        case TypeId.FormatIds.INT_TYPE_ID:
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            if (typeId == TypeId.YEAR_ID)
                return AkType.YEAR;
            if (typeId.isUnsigned())
                return AkType.U_INT;
            return AkType.LONG; // Not INT.
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned())
                return AkType.U_BIGINT;
            return AkType.LONG;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            return AkType.DECIMAL;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            if (typeId.isUnsigned())
                return AkType.U_DOUBLE;
            return AkType.DOUBLE;
        case TypeId.FormatIds.REAL_TYPE_ID:
            if (typeId.isUnsigned())
                return AkType.U_FLOAT;
            return AkType.FLOAT;
        case TypeId.FormatIds.DATE_TYPE_ID:
            return AkType.DATE;
        case TypeId.FormatIds.TIME_TYPE_ID:
            return AkType.TIME;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            if (typeId == TypeId.DATETIME_ID)
                return AkType.DATETIME;
            return AkType.TIMESTAMP;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
        case TypeId.FormatIds.BLOB_TYPE_ID:
            return AkType.VARBINARY;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
        case TypeId.FormatIds.CLOB_TYPE_ID:
            return AkType.TEXT;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            return AkType.INTERVAL_MONTH;
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            return AkType.INTERVAL_MILLIS;
        }

        String name = descriptor.getFullSQLTypeName();
        for (com.akiban.ais.model.Type aisType : com.akiban.ais.model.Types.types()) {
            if (aisType.name().equalsIgnoreCase(name)) {
                return aisType.akType();
            }
        }
        try {
            return AkType.valueOf(name.toUpperCase());
        }
        catch (IllegalArgumentException ex) {
            throw new UnsupportedOperationException(
                    "unsupported type id: " + typeId + " (" + name + ')'
            );
        }
    }

    private TypesTranslation() {}
}
