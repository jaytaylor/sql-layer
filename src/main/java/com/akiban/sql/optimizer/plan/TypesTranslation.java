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

    private TypesTranslation() {}
}
