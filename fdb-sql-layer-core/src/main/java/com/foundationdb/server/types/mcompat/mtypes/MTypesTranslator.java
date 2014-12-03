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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.Types;

public class MTypesTranslator extends TypesTranslator
{
    public static final TypesTranslator INSTANCE = new MTypesTranslator();

    private MTypesTranslator() {
    }

    @Override
    public TClass typeClassForJDBCType(int jdbcType,
                                       String schemaName, String tableName, String columnName) {
        switch (jdbcType) {
        case Types.BIGINT:
            return MNumeric.BIGINT;
        case Types.BINARY:
        case Types.BIT:
            return MBinary.BINARY;
//        case Types.BLOB:
//        case Types.LONGVARBINARY:
//            return MBinary.LONGBLOB;
        case Types.CHAR:
        case Types.NCHAR:
            return MString.CHAR;
        case Types.CLOB:
        case Types.NCLOB:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
      //case Types.SQLXML:
            return MString.LONGTEXT;
        case Types.DATE:
            return MDateAndTime.DATE;
        case Types.DECIMAL:
        case Types.NUMERIC:
            return MNumeric.DECIMAL;
        case Types.DOUBLE:
            return MApproximateNumber.DOUBLE;
        case Types.FLOAT:
        case Types.REAL:
            return MApproximateNumber.FLOAT;
        case Types.INTEGER:
            return MNumeric.INT;
        case Types.SMALLINT:
            return MNumeric.SMALLINT;
        case Types.TIME:
            return MDateAndTime.TIME;
        case Types.TIMESTAMP:
            return MDateAndTime.DATETIME; // (Not TIMESTAMP.)
        case Types.TINYINT:
            return MNumeric.TINYINT;
        case Types.VARBINARY:
            return MBinary.VARBINARY;
        case Types.VARCHAR:
        case Types.NVARCHAR:
            return MString.VARCHAR;
        default:
            return super.typeClassForJDBCType(jdbcType, schemaName, tableName, columnName);
        }
    }

    @Override
    protected TInstance typeForSQLType(TypeId typeId, DataTypeDescriptor sqlType,
                                       int defaultCharsetId, int defaultCollationId,
                                       String schemaName, String tableName, String columnName) {
        // Handle non-standard / more-specific cases.
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MNumeric.TINYINT_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            if (typeId == TypeId.YEAR_ID) {
                return MDateAndTime.YEAR.instance(sqlType.isNullable());
            }
            else if (typeId.isUnsigned()) {
                return MNumeric.SMALLINT_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.MEDIUMINT_ID:
            if (typeId.isUnsigned()) {
                return MNumeric.MEDIUMINT_UNSIGNED.instance(sqlType.isNullable());
            }
            else {
                return MNumeric.MEDIUMINT.instance(sqlType.isNullable());
            }
        case TypeId.FormatIds.INT_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MNumeric.INT_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MNumeric.BIGINT_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MApproximateNumber.FLOAT_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MApproximateNumber.DOUBLE_UNSIGNED.instance(sqlType.isNullable());
            }
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
            if (typeId.isUnsigned()) {
                return MNumeric.DECIMAL_UNSIGNED.instance(sqlType.getPrecision(), sqlType.getScale(), sqlType.isNullable());
            }
            break;
        /* (This would be needed to allow timestamp in DDL to mean timestamp
            and not datetime.)
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            if (typeId == TypeId.DATETIME_ID) {
                return MDatetimes.DATETIME.instance(sqlType.isNullable());
            }
            break;
        */
        case TypeId.FormatIds.CLOB_TYPE_ID:
            if (typeId == TypeId.TINYTEXT_ID) {
                return typeForStringType(MString.TINYTEXT, sqlType,
                        defaultCharsetId, defaultCollationId,
                        schemaName, tableName, columnName);
            }
            if (typeId == TypeId.TEXT_ID) {
                return typeForStringType(MString.TEXT, sqlType,
                        defaultCharsetId, defaultCollationId,
                        schemaName, tableName, columnName);
            }
            if (typeId == TypeId.MEDIUMTEXT_ID) {
                return typeForStringType(MString.MEDIUMTEXT, sqlType,
                        defaultCharsetId, defaultCollationId,
                        schemaName, tableName, columnName);
            }
            if (typeId == TypeId.LONGTEXT_ID) {
                return typeForStringType(MString.LONGTEXT, sqlType,
                        defaultCharsetId, defaultCollationId,
                        schemaName, tableName, columnName);
            }
            break;
//        case TypeId.FormatIds.BLOB_TYPE_ID:
//            if (typeId == TypeId.BLOB_ID) {
//                return MBinary.BLOB.instance(sqlType.isNullable());
//            }
//            break;
        }
        return super.typeForSQLType(typeId, sqlType,
                defaultCharsetId, defaultCollationId,
                schemaName, tableName, columnName);
    }
    
    @Override
    public Class<?> jdbcClass(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == MDateAndTime.DATE)
            return java.sql.Date.class;
        if ((tclass == MDateAndTime.TIMESTAMP) ||
            (tclass == MDateAndTime.DATETIME))
            return java.sql.Timestamp.class;
        if ((tclass == MNumeric.DECIMAL) ||
            (tclass == MNumeric.DECIMAL_UNSIGNED))
            return java.math.BigDecimal.class;
        if ((tclass == MApproximateNumber.DOUBLE) ||
            (tclass == MApproximateNumber.DOUBLE_UNSIGNED))
            return Double.class;
        if ((tclass == MApproximateNumber.FLOAT) ||
            (tclass == MApproximateNumber.FLOAT_UNSIGNED))
            return Float.class;
        if (tclass == MNumeric.TINYINT)
            return Byte.class;
        if ((tclass == MNumeric.TINYINT_UNSIGNED) ||
            (tclass == MNumeric.SMALLINT) ||
            (tclass == MDateAndTime.YEAR))
            return Short.class;
        if ((tclass == MNumeric.SMALLINT_UNSIGNED) ||
            (tclass == MNumeric.INT) ||
            (tclass == MNumeric.MEDIUMINT))
            return Integer.class;
        if ((tclass == MNumeric.INT_UNSIGNED) ||
            (tclass == MNumeric.BIGINT))
            return Long.class;
        if (tclass == MNumeric.BIGINT_UNSIGNED)
            return java.math.BigInteger.class;
        if ((tclass == MString.CHAR) ||
            (tclass == MString.VARCHAR) ||
            (tclass == MString.TINYTEXT) ||
            (tclass == MString.MEDIUMTEXT) ||
            (tclass == MString.TEXT) ||
            (tclass == MString.LONGTEXT))
            return String.class;
        if (tclass == MDateAndTime.TIME)
            return java.sql.Time.class;
        if ((tclass == MBinary.VARBINARY) ||
            (tclass == MBinary.BINARY))
            return byte[].class;
        return super.jdbcClass(type);
    }

    @Override
    public boolean isTypeSigned(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        return ((tclass == MNumeric.TINYINT) ||
                (tclass == MNumeric.SMALLINT) ||
                (tclass == MNumeric.MEDIUMINT) ||
                (tclass == MNumeric.INT) ||
                (tclass == MNumeric.BIGINT) ||
                (tclass == MNumeric.DECIMAL) ||
                (tclass == MApproximateNumber.DOUBLE) ||
                (tclass == MApproximateNumber.FLOAT));
    }

    @Override
    public long getIntegerValue(ValueSource value) {
        long base = super.getIntegerValue(value);
        if (TInstance.tClass(value.getType()) == MDateAndTime.YEAR) {
            base += 1900;
        }
        return base;
    }

    @Override
    public void setIntegerValue(ValueTarget target, long value) {
        if (TInstance.tClass(target.getType()) == MDateAndTime.YEAR) {
            value -= 1900;
        }
        super.setIntegerValue(target, value);
    }

    @Override
    public TClass typeClassForSystemTimestamp() {
        return MDateAndTime.TIMESTAMP;
    }

    @Override
    public long getTimestampMillisValue(ValueSource value) {
        TClass tclass = TInstance.tClass(value.getType());
        long[] ymdhms = null;
        if (tclass == MDateAndTime.DATE) {
            ymdhms = MDateAndTime.decodeDate(value.getInt32());
        }
        else if (tclass == MDateAndTime.TIME) {
            ymdhms = MDateAndTime.decodeTime(value.getInt32());
        }
        else if (tclass == MDateAndTime.DATETIME) {
            ymdhms = MDateAndTime.decodeDateTime(value.getInt64());
        }
        if (ymdhms != null) {
            DateTime dt = new DateTime((int)ymdhms[0], (int)ymdhms[1], (int)ymdhms[2],
                                       (int)ymdhms[3], (int)ymdhms[4], (int)ymdhms[5]);
            return dt.getMillis();
        }
        else {
            return value.getInt32() * 1000L;
        }
    }

    @Override
    public void setTimestampMillisValue(ValueTarget value, long millis, int nanos) {
        TClass tclass = TInstance.tClass(value.getType());
        if (tclass == MDateAndTime.DATE) {
            value.putInt32(MDateAndTime.encodeDate(millis, DateTimeZone.getDefault().getID()));
        }
        else if (tclass == MDateAndTime.TIME) {
            value.putInt32(MDateAndTime.encodeTime(millis, DateTimeZone.getDefault().getID()));
        }
        else if (tclass == MDateAndTime.DATETIME) {
            value.putInt64(MDateAndTime.encodeDateTime(millis, DateTimeZone.getDefault().getID()));
        }
        else {
            value.putInt32((int)(millis / 1000));
        }
    }

}
