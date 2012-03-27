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

package com.akiban.server.types.util;

import com.akiban.server.types.AbstractValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

public final class BoolValueSource extends AbstractValueSource {

    // BoolValueSource class interface

    public static final ValueSource OF_TRUE = new BoolValueSource(true);
    public static final ValueSource OF_FALSE = new BoolValueSource(false);
    public static final ValueSource OF_NULL = new BoolValueSource(null);

    public static ValueSource of(Boolean value) {
        return value == null ? OF_NULL : of(value.booleanValue());
    }

    public static ValueSource of(boolean value) {
        return value ? OF_TRUE : OF_FALSE;
    }

    // ValueSource interface

    @Override
    public boolean getBool() {
        if (value == null)
            throw new ValueSourceIsNullException();
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public AkType getConversionType() {
        return AkType.BOOL;
    }

    // object interface

    @Override
    public String toString() {
        return toString;
    }

    // for use in this class

    private BoolValueSource(Boolean value) {
        this.value = value;
        this.toString = "BoolValueSource(" + value + ")";
    }

    // object state
    private final Boolean value;
    private final String toString;
}
