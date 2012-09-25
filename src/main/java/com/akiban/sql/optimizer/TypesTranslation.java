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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnknownDataTypeException;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.aksql.aktypes.AkInterval;
import com.akiban.server.types3.aksql.aktypes.AkResultSet;
import com.akiban.server.types3.common.types.StringFactory.Charset;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.sql.StandardException;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import java.util.ArrayList;
import java.util.List;

/** Yet another translator between type regimes. */
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
        case TypeId.FormatIds.BIT_TYPE_ID:
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
        case TypeId.FormatIds.ROW_MULTISET_TYPE_ID_IMPL:
            return AkType.RESULT_SET;
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

    public static ExpressionType toExpressionType(DataTypeDescriptor sqlType) {
        if (sqlType == null)
            return null;
        TypeId typeId = sqlType.getTypeId();
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            return ExpressionTypes.BOOL;
        case TypeId.FormatIds.DATE_TYPE_ID:
            return ExpressionTypes.DATE;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            return ExpressionTypes.decimal(sqlType.getPrecision(),
                                           sqlType.getScale());
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            if (typeId.isUnsigned())
                return ExpressionTypes.U_DOUBLE;
            else
                return ExpressionTypes.DOUBLE;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            if (typeId == TypeId.YEAR_ID)
                return ExpressionTypes.YEAR;
            /* else falls through */
        case TypeId.FormatIds.TINYINT_TYPE_ID:
        case TypeId.FormatIds.INT_TYPE_ID:
            if (typeId.isUnsigned())
                return ExpressionTypes.U_INT;
            else
                return ExpressionTypes.INT;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned())
                return ExpressionTypes.U_BIGINT;
            else
                return ExpressionTypes.LONG;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
        case TypeId.FormatIds.BLOB_TYPE_ID:
        case TypeId.FormatIds.CLOB_TYPE_ID:
        case TypeId.FormatIds.XML_TYPE_ID:
            return ExpressionTypes.TEXT;
        case TypeId.FormatIds.REAL_TYPE_ID:
            if (typeId.isUnsigned())
                return ExpressionTypes.U_FLOAT;
            else
                return ExpressionTypes.FLOAT;
        case TypeId.FormatIds.TIME_TYPE_ID:
            return ExpressionTypes.TIME;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            if (typeId == TypeId.DATETIME_ID)
                return ExpressionTypes.DATETIME;
            else
                return ExpressionTypes.TIMESTAMP;
        case TypeId.FormatIds.BIT_TYPE_ID:
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            return ExpressionTypes.varbinary(sqlType.getMaximumWidth());
        case TypeId.FormatIds.CHAR_TYPE_ID:
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            return ExpressionTypes.varchar(sqlType.getMaximumWidth(),
                                           sqlType.getCharacterAttributes());
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            return ExpressionTypes.INTERVAL_MILLIS;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            return ExpressionTypes.INTERVAL_MONTH;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            try {
                return ExpressionTypes.newType(AkType.valueOf(sqlType.getFullSQLTypeName().toUpperCase()), 
                                               sqlType.getPrecision(), sqlType.getScale());
            }
            catch (IllegalArgumentException ex) {
                return null;
            }
        default:
            return null;
        }
    }

    public static DataTypeDescriptor fromExpressionType(ExpressionType resultType) {
        return fromExpressionType (resultType, true);
    }

    public static DataTypeDescriptor fromExpressionType(ExpressionType resultType, boolean isNullable) {
        switch (resultType.getType()) {
        case BOOL:
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, isNullable);
        case INT:
            return new DataTypeDescriptor(TypeId.INTEGER_ID, isNullable);
        case LONG:
            return new DataTypeDescriptor(TypeId.BIGINT_ID, isNullable);
        case DOUBLE:
            return new DataTypeDescriptor(TypeId.DOUBLE_ID, isNullable);
        case FLOAT:
            return new DataTypeDescriptor(TypeId.REAL_ID, isNullable);
        case U_INT:
            return new DataTypeDescriptor(TypeId.INTEGER_UNSIGNED_ID, isNullable);
        case U_BIGINT:
            return new DataTypeDescriptor(TypeId.BIGINT_UNSIGNED_ID, isNullable);
        case U_FLOAT:
            return new DataTypeDescriptor(TypeId.REAL_UNSIGNED_ID, isNullable);
        case U_DOUBLE:
            return new DataTypeDescriptor(TypeId.DOUBLE_UNSIGNED_ID, isNullable);
        case DATE:
            return new DataTypeDescriptor(TypeId.DATE_ID, isNullable);
        case TIME:
            return new DataTypeDescriptor(TypeId.TIME_ID, isNullable);
        case TIMESTAMP:
            return new DataTypeDescriptor(TypeId.TIMESTAMP_ID, isNullable);
        case VARCHAR:
            {
                DataTypeDescriptor dtd = new DataTypeDescriptor(TypeId.VARCHAR_ID, isNullable,
                                                                resultType.getPrecision());
                if (resultType.getCharacterAttributes() != null)
                    dtd = new DataTypeDescriptor(dtd, resultType.getCharacterAttributes());
                return dtd;
            }
        case DECIMAL:
            {
                int precision = resultType.getPrecision();
                int scale = resultType.getScale();
                return new DataTypeDescriptor(TypeId.DECIMAL_ID, precision, scale, isNullable,
                                              DataTypeDescriptor.computeMaxWidth(precision, scale));
            }
        case TEXT:
            return new DataTypeDescriptor(TypeId.LONGVARCHAR_ID, isNullable);
        case VARBINARY:
            return new DataTypeDescriptor(TypeId.VARBIT_ID, isNullable);
        case NULL:
            return null;
        case DATETIME:
            return new DataTypeDescriptor(TypeId.DATETIME_ID, isNullable);
        case YEAR:
            return new DataTypeDescriptor(TypeId.YEAR_ID, isNullable);
        case INTERVAL_MILLIS:
            return new DataTypeDescriptor(TypeId.INTERVAL_SECOND_ID, isNullable);
        case INTERVAL_MONTH:
            return new DataTypeDescriptor(TypeId.INTERVAL_MONTH_ID, isNullable);
        default:
            try {
                return new DataTypeDescriptor(TypeId.getUserDefinedTypeId(null,
                                                                          resultType.getType().name(),
                                                                          null),
                                              isNullable);
            }
            catch (StandardException ex) {
                throw new AkibanInternalException("Cannot make type for " + resultType,
                                                  ex);
            }
        }
    }

    public static ExpressionType castType(ExpressionType fromType, AkType toType,
                                          DataTypeDescriptor sqlType) {
        switch (toType) {
        case BOOL:
            return ExpressionTypes.BOOL;
        case INT:
            return ExpressionTypes.INT;
        case YEAR:
            return ExpressionTypes.YEAR;
        case LONG:
            return ExpressionTypes.LONG;
        case DOUBLE:
            return ExpressionTypes.DOUBLE;
        case FLOAT:
            return ExpressionTypes.FLOAT;
        case U_INT:
            return ExpressionTypes.U_INT;
        case U_BIGINT:
            return ExpressionTypes.U_BIGINT;
        case U_FLOAT:
            return ExpressionTypes.U_FLOAT;
        case U_DOUBLE:
            return ExpressionTypes.U_DOUBLE;
        case DATE:
            return ExpressionTypes.DATE;
        case TIME:
            return ExpressionTypes.TIME;
        case DATETIME:
            return ExpressionTypes.DATETIME;
        case TIMESTAMP:
            return ExpressionTypes.TIMESTAMP;
        case TEXT:
            return ExpressionTypes.TEXT;
        case VARCHAR:
            if (sqlType != null)
                return ExpressionTypes.varchar(sqlType.getMaximumWidth(),
                                               sqlType.getCharacterAttributes());
            else
                return ExpressionTypes.varchar(TypeId.VARCHAR_ID.getMaximumMaximumWidth(), null);
        case VARBINARY:
            if (sqlType != null)
                return ExpressionTypes.varbinary(sqlType.getMaximumWidth());
            else
                return ExpressionTypes.varbinary(TypeId.VARBIT_ID.getMaximumMaximumWidth());
        case DECIMAL:
            if (sqlType != null) {
                TypeId typeId = sqlType.getTypeId();
                if (typeId.isNumericTypeId())
                    return ExpressionTypes.decimal(sqlType.getPrecision(),
                                                   sqlType.getScale());
                else
                    return ExpressionTypes.decimal(typeId.getMaximumPrecision(),
                                                   typeId.getMaximumScale());
            }
            else
                return ExpressionTypes.decimal(TypeId.DECIMAL_ID.getMaximumPrecision(),
                                               TypeId.DECIMAL_ID.getMaximumScale());
        case INTERVAL_MILLIS:
            return ExpressionTypes.INTERVAL_MILLIS;
        case INTERVAL_MONTH:
            return ExpressionTypes.INTERVAL_MONTH;
        default:
            return ExpressionTypes.newType(toType, 0, 0);
        }
    }

    public static TInstance toTInstance(DataTypeDescriptor sqlType) {
        if (!Types3Switch.ON)
            return null;
        TInstance tInstance;
        TypeId typeId = sqlType.getTypeId();
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            tInstance = AkInterval.SECONDS.tInstanceFrom(sqlType);
            break;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            tInstance = AkInterval.MONTHS.tInstanceFrom(sqlType);
            break;
        case TypeId.FormatIds.BIT_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth());
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            tInstance = AkBool.INSTANCE.instance();
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            tInstance = charTInstance(sqlType, MString.VARCHAR);
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            tInstance = MDatetimes.DATE.instance();
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            tInstance = MNumeric.DECIMAL.instance(sqlType.getPrecision(), sqlType.getScale());
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MApproximateNumber.DOUBLE_UNSIGNED.instance();
            }
            else {
                tInstance = MApproximateNumber.DOUBLE.instance();
            }
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.INT_UNSIGNED.instance();
            }
            else {
                tInstance = MNumeric.INT.instance();
            }
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.BIGINT_UNSIGNED.instance();
            }
            else {
                tInstance = MNumeric.BIGINT.instance();
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
                tInstance = MApproximateNumber.FLOAT_UNSIGNED.instance();
            }
            else {
                tInstance = MApproximateNumber.FLOAT.instance();
            }
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            if (typeId == TypeId.YEAR_ID) {
                tInstance = MDatetimes.YEAR.instance();
            }
            else if (typeId.isUnsigned()) {
                tInstance = MNumeric.SMALLINT_UNSIGNED.instance();
            }
            else {
                tInstance = MNumeric.SMALLINT.instance();
            }
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            tInstance = MDatetimes.TIME.instance();
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            if (typeId == TypeId.DATETIME_ID) {
                tInstance = MDatetimes.DATETIME.instance();
            }
            else {
                tInstance = MDatetimes.TIMESTAMP.instance();
            }
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                tInstance = MNumeric.TINYINT_UNSIGNED.instance();
            }
            else {
                tInstance = MNumeric.TINYINT.instance();
            }
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth());
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            tInstance = MBinary.VARBINARY.instance(sqlType.getMaximumWidth());
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
                List<AkResultSet.Column> columns = new ArrayList<AkResultSet.Column>(columnNames.length);
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
                    }
                }
            }
            /* falls through */
        default:
            throw new UnknownDataTypeException(sqlType.toString());
        }
        tInstance.setNullable(sqlType.isNullable());
        return tInstance;
    }

    private static TInstance charTInstance(DataTypeDescriptor type, TString tClass) {
        CharacterTypeAttributes typeAttributes = type.getCharacterAttributes();
        int charsetId = (typeAttributes == null)
                ? -1
                : Charset.of(typeAttributes.getCharacterSet()).ordinal();
        return tClass.instance(type.getMaximumWidth(), charsetId);
    }

    private TypesTranslation() {}
}
