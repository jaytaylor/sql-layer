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

package com.akiban.server.types3.pvalue;

import java.util.Arrays;

public final class PValueSources {
    
    public static boolean areEqual(PValueSource one, PValueSource two) {
        PUnderlying underlyingType = one.getUnderlyingType();
        if (underlyingType != two.getUnderlyingType())
            return false;
        if (one.isNull())
            return two.isNull();
        if (two.isNull())
            return false;
        switch (underlyingType) {
        case BOOL:
            return one.getBoolean() == two.getBoolean();
        case INT_8:
            return one.getInt8() == two.getInt8();
        case INT_16:
            return one.getInt16() == two.getInt16();
        case UINT_16:
            return one.getInt16() == two.getInt16();
        case INT_32:
            return one.getInt32() == two.getInt32();
        case INT_64:
            return one.getInt64() == two.getInt64();
        case FLOAT:
            return one.getFloat() == two.getFloat();
        case DOUBLE:
            return one.getDouble() == two.getDouble();
        case BYTES:
            return Arrays.equals(one.getBytes(), two.getBytes());
        default:
            throw new AssertionError(underlyingType);
        }
    }
    
    private PValueSources() {}
}
