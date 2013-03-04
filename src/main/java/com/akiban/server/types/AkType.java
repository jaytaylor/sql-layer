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

package com.akiban.server.types;

import static com.akiban.server.types.AkType.UnderlyingType.BOOLEAN_AKTYPE;
import static com.akiban.server.types.AkType.UnderlyingType.DOUBLE_AKTYPE;
import static com.akiban.server.types.AkType.UnderlyingType.FLOAT_AKTYPE;
import static com.akiban.server.types.AkType.UnderlyingType.LONG_AKTYPE;
import static com.akiban.server.types.AkType.UnderlyingType.OBJECT_AKTYPE;

import com.akiban.server.error.AkibanInternalException;

public enum AkType {
    DATE(LONG_AKTYPE, java.util.Date.class),
    DATETIME(LONG_AKTYPE, java.util.Date.class),
    DECIMAL(OBJECT_AKTYPE, java.math.BigDecimal.class),
    DOUBLE(DOUBLE_AKTYPE, double.class),
    FLOAT(FLOAT_AKTYPE, float.class),
    INT(LONG_AKTYPE, int.class),
    LONG(LONG_AKTYPE, long.class),
    VARCHAR(OBJECT_AKTYPE, java.lang.String.class),
    TEXT(OBJECT_AKTYPE, java.lang.String.class),
    TIME(LONG_AKTYPE, java.sql.Time.class),
    TIMESTAMP(LONG_AKTYPE, java.sql.Timestamp.class),
    U_BIGINT(OBJECT_AKTYPE, java.math.BigInteger.class),
    U_DOUBLE(DOUBLE_AKTYPE, java.math.BigDecimal.class),
    U_FLOAT(FLOAT_AKTYPE, double.class),
    U_INT(LONG_AKTYPE, long.class),
    VARBINARY(OBJECT_AKTYPE, byte[].class),
    YEAR(LONG_AKTYPE, int.class),
    BOOL(BOOLEAN_AKTYPE, boolean.class),
    INTERVAL_MILLIS(LONG_AKTYPE, long.class),
    INTERVAL_MONTH(LONG_AKTYPE, long.class),
    RESULT_SET(OBJECT_AKTYPE, java.sql.ResultSet.class),
    NULL(null, null),
    UNSUPPORTED(null, null),
    ;

    public UnderlyingType underlyingTypeOrNull() {
        return underlyingType;
    }
    
    public UnderlyingType underlyingType() {
        if (underlyingType == null) {
            throw new AkibanInternalException("no underlying type for " + name());
        }
        return underlyingType;
    }
    
    public Class<?> javaClass() {
        return javaClass;
    }

    private AkType(UnderlyingType underlyingType, final Class<?> javaClass) {
        this.underlyingType = underlyingType;
        this.javaClass = javaClass;
    }

    private final UnderlyingType underlyingType;
    private final Class javaClass;

    public enum UnderlyingType {
        BOOLEAN_AKTYPE,
        LONG_AKTYPE,
        FLOAT_AKTYPE,
        DOUBLE_AKTYPE,
        OBJECT_AKTYPE
    }
}