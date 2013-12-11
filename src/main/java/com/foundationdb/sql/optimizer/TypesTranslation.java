/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.common.types.StringFactory.Charset;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import java.util.ArrayList;
import java.util.List;

/** Yet another translator between type regimes. */
public final class TypesTranslation {
    public static TInstance toTInstance(DataTypeDescriptor sqlType) {
        TInstance tInstance;
        if (sqlType == null) return null;
        TypeId typeId = sqlType.getTypeId();
        typeIdSwitch:
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            tInstance = AkInterval.SECONDS.tInstanceFrom(sqlType);
            break;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            tInstance = AkInterval.MONTHS.tInstanceFrom(sqlType);
            break;
        case TypeId.FormatIds.BIT_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth(), sqlType.isNullable());
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            tInstance = AkBool.INSTANCE.instance(sqlType.isNullable());
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.VARCHAR);
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            tInstance = MDatetimes.DATE.instance(sqlType.isNullable());
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            tInstance = MNumeric.DECIMAL.instance(sqlType.getPrecision(), sqlType.getScale(), sqlType.isNullable());
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MApproximateNumber.DOUBLE_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MApproximateNumber.DOUBLE.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.INT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MNumeric.INT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.BIGINT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MNumeric.BIGINT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.TEXT);
            break;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.TEXT);
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MApproximateNumber.FLOAT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MApproximateNumber.FLOAT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            if (typeId == TypeId.YEAR_ID) {
                tInstance = MDatetimes.YEAR.instance(sqlType.isNullable());
            }
            else if (typeId.isUnsigned()) {
                tInstance = MNumeric.SMALLINT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MNumeric.SMALLINT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.MEDIUMINT_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.MEDIUMINT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MNumeric.MEDIUMINT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            tInstance = MDatetimes.TIME.instance(sqlType.isNullable());
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            if (typeId == TypeId.DATETIME_ID) {
                tInstance = MDatetimes.DATETIME.instance(sqlType.isNullable());
            }
            else {
                tInstance = MDatetimes.TIMESTAMP.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.TINYINT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                tInstance = MNumeric.TINYINT.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth(), sqlType.isNullable());
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth(), sqlType.isNullable());
            break;
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.VARCHAR);
            break;
        case TypeId.FormatIds.CLOB_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.TEXT);
            break;
        case TypeId.FormatIds.XML_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.TEXT);
            break;
        case TypeId.FormatIds.ROW_MULTISET_TYPE_ID_IMPL:
            {
                TypeId.RowMultiSetTypeId rmsTypeId = 
                    (TypeId.RowMultiSetTypeId)typeId;
                String[] columnNames = rmsTypeId.getColumnNames();
                DataTypeDescriptor[] columnTypes = rmsTypeId.getColumnTypes();
                List<AkResultSet.Column> columns = new ArrayList<>(columnNames.length);
                for (int i = 0; i < columnNames.length; i++) {
                    columns.add(new AkResultSet.Column(columnNames[i],
                                                       toTInstance(columnTypes[i])));
                }
                tInstance = AkResultSet.INSTANCE.instance(columns);
            }
            break;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            {
                String name = typeId.getSQLTypeName();
                for (Type aisType : Types.types()) {
                    if (aisType.name().equalsIgnoreCase(name)) {
                        tInstance = Column.generateTInstance(null, aisType, null, null, false);
                        break typeIdSwitch;
                    }
                }
            }
            /* falls through */
        default:
            throw new UnknownDataTypeException(sqlType.toString());
        }
        return tInstance;
    }

    private static TInstance charTInstance(DataTypeDescriptor type, TString tClass) {
        CharacterTypeAttributes typeAttributes = type.getCharacterAttributes();
        int charsetId = (typeAttributes == null)
                ? StringFactory.DEFAULT_CHARSET.ordinal()
                : Charset.of(typeAttributes.getCharacterSet()).ordinal();
        return tClass.instance(type.getMaximumWidth(), charsetId, type.isNullable());
    }

    private TypesTranslation() {}
}
