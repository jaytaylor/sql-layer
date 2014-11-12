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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.error.UnsupportedColumnDataTypeException;
import com.foundationdb.server.error.UnsupportedDataTypeException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;

/**
 * Translate between types in particular bundle(s) and Java / standard
 * SQL types.  SQL types are represented via Derby (sql-parser) types
 * or <code>java.sql.Types</code> members.
 */
public abstract class TypesTranslator
{

    /** Get a <code>TClass</code> for the default encoding of strings. */
    public TClass typeClassForString() {
        return typeClassForJDBCType(Types.VARCHAR);
    }

    /** Get a <code>TInstance</code> for the an arbitrary length string. */
    public TInstance typeForString() {
        return typeClassForString().instance(Integer.MAX_VALUE, false);
    }

    /** Get a <code>TInstance</code> for the given string. */
    public TInstance typeForString(String value) {
        if (value == null)
            return typeClassForString().instance(1, true);
        else
            return typeClassForString().instance(value.codePointCount(0, value.length()), false);
    }

    public TInstance typeForString(int length, String charset, String collation,
                                   int defaultCharsetId, int defaultCollationId,
                                   boolean nullable) {
        TClass tclass = typeClassForString();
        assert tclass.hasAttributes(StringAttribute.class) : tclass;
        int charsetId = defaultCharsetId, collationId = defaultCollationId;
        if (charset != null) {
            charsetId = StringFactory.charsetNameToId(charset);
        }
        if (collation != null) {
            collationId = StringFactory.collationNameToId(collation);
        }
        return tclass.instance(length, charsetId, collationId, nullable);
    }

    public TClass typeClassForBinary() {
        return typeClassForJDBCType(Types.VARBINARY);
    }

    public TClass typeClassForSystemTimestamp() {
        return typeClassForJDBCType(Types.TIMESTAMP);
    }

    public int jdbcType(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == null)
            return Types.OTHER;
        else
            return tclass.jdbcType();
    }

    public Class<?> jdbcClass(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == null)
            return Object.class;
        int jdbcType = tclass.jdbcType();
        switch (jdbcType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            return BigDecimal.class;
        case Types.BOOLEAN:
            return Boolean.class;
        case Types.TINYINT:
            return Byte.class;
        case Types.BINARY:
        case Types.BIT:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return byte[].class;
        case Types.DATE:
            return java.sql.Date.class;
        case Types.DOUBLE:
            return Double.class;
        case Types.FLOAT:
        case Types.REAL:
            return Float.class;
        case Types.INTEGER:
            return Integer.class;
        case Types.BIGINT:
            return Long.class;
        case Types.SMALLINT:
            return Short.class;
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.CLOB:
            return String.class;
        case Types.TIME:
            return java.sql.Time.class;
        case Types.TIMESTAMP:
            return java.sql.Timestamp.class;

        /*
        case Types.ARRAY:
            return java.sql.Array.class;
        case Types.BLOB:
            return java.sql.Blob.class;
        case Types.CLOB:
            return java.sql.Clob.class;
        case Types.NCLOB:
            return java.sql.NClob.class;
        case Types.REF:
            return java.sql.Ref.class;
        case Types.ROWID:
            return java.sql.RowId.class;
        case Types.SQLXML:
            return java.sql.SQLXML.class;
        */

        case Types.NULL:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.STRUCT:
        default:
            break;
        }
        if (tclass == AkResultSet.INSTANCE)
            return java.sql.ResultSet.class;
        return Object.class;
    }

    /** Does this type represent a signed numeric type? */
    public boolean isTypeSigned(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == null)
            return false;
        switch (tclass.jdbcType()) {
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.INTEGER:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
            return true;
        default:
            return false;
        }
    }

    public boolean isTypeUnsigned(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == null)
            return false;
        else
            return tclass.isUnsigned();
    }

    /** Give a <code>ValueSource</code> whose {@link #jdbcType} claims
     * to be one of the integer types (<code>TINYINT</code>,
     * <code>SMALLINT</code>, <code>INTEGER</code>,
     * <code>BIGINT<code>), get the integer value.  Needed because of
     * <code>UNSIGNED</code> and <code>YEAR</code> types.
     */
    public long getIntegerValue(ValueSource value) {
        switch (TInstance.underlyingType(value.getType())) {
        case INT_8:
            return value.getInt8();
        case INT_16:
            return value.getInt16();
        case UINT_16:
            return value.getUInt16();
        case INT_32:
            return value.getInt32();
        case INT_64:
        default:
            return value.getInt64();
        }
    }

    /**
     * @see #getIntegerValue
     */
    public void setIntegerValue(ValueTarget target, long value) {
        switch (TInstance.underlyingType(target.getType())) {
        case INT_8:
            target.putInt8((byte)value);
            break;
        case INT_16:
            target.putInt16((short)value);
            break;
        case UINT_16:
            target.putUInt16((char)value);
            break;
        case INT_32:
            target.putInt32((int)value);
            break;
        case INT_64:
        default:
            target.putInt64(value);
        }
    }

    /** Give a <code>ValueSource</code> whose {@link #jdbcType} claims
     * to be one of the decimal types (<code>DECIMAL</code>,
     * <code>NUMERIC</code>), get the decimal value.
     */
    public BigDecimal getDecimalValue(ValueSource value) {
        return TBigDecimal.getWrapper(value, value.getType()).asBigDecimal();
    }

    /** Give a <code>ValueSource</code> whose {@link #jdbcType} claims
     * to be one of the date/time types (<code>DATE</code>,
     * <code>TIME</code>, <code>TIMESTAMP</code>), get the
     * milliseconds portion.  In general, the seconds portion of this
     * value should be zero.  Needed because of <code>DATETIME</code>.
     * @see #getTimestampNanosValue
     */
    public abstract long getTimestampMillisValue(ValueSource value);

    /** Give a <code>ValueSource</code> whose {@link #jdbcType} claims
     * to be <code>TIMESTAMP</code>, get the nanoseconds portion.
     * @see #getTimestampNanosValue
     */
    public int getTimestampNanosValue(ValueSource value) {
        return 0;
    }

    /**
     * @see #getTimestampMillisValue
     */
    public abstract void setTimestampMillisValue(ValueTarget value, long millis, int nanos);

    /** Translate the given parser type to the corresponding type instance. */
    public TInstance typeForSQLType(DataTypeDescriptor sqlType) {
        return typeForSQLType(sqlType,
                StringFactory.DEFAULT_CHARSET_ID,
                StringFactory.DEFAULT_COLLATION_ID);
    }

    public TInstance typeForSQLType(DataTypeDescriptor sqlType,
                                    String schemaName, String tableName, String columnName) {
        return typeForSQLType(sqlType,
                StringFactory.DEFAULT_CHARSET_ID,
                StringFactory.DEFAULT_COLLATION_ID,
                schemaName, tableName, columnName);
    }

    public TInstance typeForSQLType(DataTypeDescriptor sqlType,
                                    int defaultCharsetId, int defaultCollationId) {
        return typeForSQLType(sqlType, defaultCharsetId, defaultCollationId,
                null, null, null);
    }

    public TInstance typeForSQLType(DataTypeDescriptor sqlType,
                                    int defaultCharsetId, int defaultCollationId,
                                    String schemaName, String tableName, String columnName) {
        TInstance type;
        // TODO ensure sqlType is not null, because this cannot be null, or if sqlType is null, get an appropriate TInsntance
        if (sqlType == null) 
            return null;
        else
            return typeForSQLType(sqlType.getTypeId(), sqlType,
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
    }

    protected TInstance typeForSQLType(TypeId typeId, DataTypeDescriptor sqlType,
                                       int defaultCharsetId, int defaultCollationId,
                                       String schemaName, String tableName, String columnName) {
        switch (typeId.getTypeFormatId()) {
        /* No attribute types. */
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            return typeForJDBCType(Types.TINYINT, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            return typeForJDBCType(Types.SMALLINT, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.MEDIUMINT_ID:
        case TypeId.FormatIds.INT_TYPE_ID:
            return typeForJDBCType(Types.INTEGER, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            return typeForJDBCType(Types.BIGINT, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.DATE_TYPE_ID:
            return typeForJDBCType(Types.DATE, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.TIME_TYPE_ID:
            return typeForJDBCType(Types.TIME, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            return typeForJDBCType(Types.TIMESTAMP, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.REAL_TYPE_ID:
            return typeForJDBCType(Types.REAL, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            return typeForJDBCType(Types.DOUBLE, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.BLOB_TYPE_ID:
            return typeForJDBCType(Types.LONGVARBINARY, sqlType.isNullable(), // TODO: Types.BLOB
                    schemaName, tableName, columnName);
        /* Width attribute types. */
        case TypeId.FormatIds.BIT_TYPE_ID:
            return typeForJDBCType(Types.BIT, sqlType.getMaximumWidth(), sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            return typeForJDBCType(Types.VARBINARY, sqlType.getMaximumWidth(), sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            return typeForJDBCType(Types.LONGVARBINARY, sqlType.isNullable(),
                    schemaName, tableName, columnName);
        /* Precision, scale attribute types. */
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
            return typeForJDBCType(Types.DECIMAL, sqlType.getPrecision(), sqlType.getScale(), sqlType.isNullable(),
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            return typeForJDBCType(Types.NUMERIC, sqlType.getPrecision(), sqlType.getScale(), sqlType.isNullable(),
                    schemaName, tableName, columnName);
        /* String (charset, collation) attribute types. */
        case TypeId.FormatIds.CHAR_TYPE_ID:
            return typeForStringType(Types.CHAR, sqlType,
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            return typeForStringType(Types.VARCHAR, sqlType,
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            return typeForStringType(Types.LONGVARCHAR, sqlType,
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.CLOB_TYPE_ID:
            return typeForStringType(Types.LONGVARCHAR, sqlType, // TODO: Types.CLOB
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
        case TypeId.FormatIds.XML_TYPE_ID:
            return typeForStringType(Types.SQLXML, sqlType,
                    defaultCharsetId, defaultCollationId,
                    schemaName, tableName, columnName);
        /* Special case AkSQL types. */
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            return AkBool.INSTANCE.instance(sqlType.isNullable());
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            return AkInterval.SECONDS.typeFrom(sqlType);
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            return AkInterval.MONTHS.typeFrom(sqlType);
        case TypeId.FormatIds.ROW_MULTISET_TYPE_ID_IMPL:
            {
                TypeId.RowMultiSetTypeId rmsTypeId = 
                    (TypeId.RowMultiSetTypeId)typeId;
                String[] columnNames = rmsTypeId.getColumnNames();
                DataTypeDescriptor[] columnTypes = rmsTypeId.getColumnTypes();
                List<AkResultSet.Column> columns = new ArrayList<>(columnNames.length);
                for (int i = 0; i < columnNames.length; i++) {
                    columns.add(new AkResultSet.Column(columnNames[i],
                                                       typeForSQLType(columnTypes[i])));
                }
                return AkResultSet.INSTANCE.instance(columns);
            }
        case TypeId.FormatIds.GUID_TYPE_ID:
            return AkGUID.INSTANCE.instance(sqlType.isNullable());
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            {
                String name = typeId.getSQLTypeName();
                TClass tclass = typeClassForUserDefined(name);
                return tclass.instance(sqlType.isNullable());
            }
        default:
            if (columnName != null) {
                throw new UnsupportedColumnDataTypeException(schemaName, tableName, columnName,
                                                             sqlType.toString());
            }
            else {
                throw new UnsupportedDataTypeException(sqlType.toString());
            }
        }
    }

    protected TClass typeClassForUserDefined(String name) {
        throw new UnknownDataTypeException(name);
    }

    public TInstance typeForJDBCType(int jdbcType, boolean nullable,
                                     String schemaName, String tableName, String columnName) {
        TClass tclass = typeClassForJDBCType(jdbcType, schemaName, tableName, columnName);
        if (tclass == null)
            return null;
        else
            return tclass.instance(nullable);
    }

    public TInstance typeForJDBCType(int jdbcType, int att, boolean nullable,
                                     String schemaName, String tableName, String columnName) {
        TClass tclass = typeClassForJDBCType(jdbcType, schemaName, tableName, columnName);
        if (tclass == null)
            return null;
        else
            return tclass.instance(att, nullable);
    }

    public TInstance typeForJDBCType(int jdbcType, int att1, int att2, boolean nullable,
                                     String schemaName, String tableName, String columnName) {
        TClass tclass = typeClassForJDBCType(jdbcType, schemaName, tableName, columnName);
        if (tclass == null)
            return null;
        else
            return tclass.instance(att1, att2, nullable);
    }

    protected TInstance typeForStringType(int jdbcType, DataTypeDescriptor type,
                                          int defaultCharsetId, int defaultCollationId,
                                          String schemaName, String tableName, String columnName) {
        TClass tclass = typeClassForJDBCType(jdbcType, schemaName, tableName, columnName);
        if (tclass == null)
            return null;
        return typeForStringType(tclass, type,
                defaultCharsetId, defaultCollationId,
                schemaName, tableName, columnName);
    }

    protected TInstance typeForStringType(TClass tclass, DataTypeDescriptor type,
                                          int defaultCharsetId, int defaultCollationId,
                                          String schemaName, String tableName, String columnName) {
        int charsetId, collationId;
        CharacterTypeAttributes typeAttributes = type.getCharacterAttributes();
        if ((typeAttributes == null) || (typeAttributes.getCharacterSet() == null)) {
            charsetId = defaultCharsetId;
        }
        else {
            charsetId = StringFactory.charsetNameToId(typeAttributes.getCharacterSet());
        }
        if ((typeAttributes == null) || (typeAttributes.getCollation() == null)) {
            collationId = defaultCollationId;
        }
        else {
            collationId = StringFactory.collationNameToId(typeAttributes.getCollation());
        }
        return tclass.instance(type.getMaximumWidth(),
                               charsetId, collationId,
                               type.isNullable());
    }
    
    public TClass typeClassForJDBCType(int jdbcType) {
        return typeClassForJDBCType(jdbcType, null, null, null);
    }

    public TClass typeClassForJDBCType(int jdbcType,
                                       String schemaName, String tableName, String columnName) {
        switch (jdbcType) {
        case Types.BOOLEAN:
            return AkBool.INSTANCE;
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.NULL:
        case Types.OTHER:
        case Types.REF:
        case Types.ROWID:
        case Types.STRUCT:
        default:
            if (columnName != null) {
                throw new UnsupportedColumnDataTypeException(schemaName, tableName, columnName,
                                                             jdbcTypeName(jdbcType));
            }
            else {
                throw new UnsupportedDataTypeException(jdbcTypeName(jdbcType));
            }
        }        
    }

    protected static String jdbcTypeName(int jdbcType) {
        try {
            for (Field field : Types.class.getFields()) {
                if (((field.getModifiers() & Modifier.STATIC) != 0) &&
                    Objects.equals(jdbcType, field.get(null))) {
                    return field.getName();
                }
            }
        }
        catch (Exception ex) {
        }
        return String.format("JDBC #%s", jdbcType);
    }
}
