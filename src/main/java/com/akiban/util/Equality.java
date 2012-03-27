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

package com.akiban.util;

import java.util.Arrays;

public final class Equality {

    public static boolean areEqual(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null) {
            return false;
        }
        Class<?> aClass = a.getClass();
        if (aClass.isArray()) {
            return equalArrays(a, b);
        }
        return a.equals(b);
    }

    private static boolean equalArrays(Object a, Object b) {
        Class<?> aClass = a.getClass();
        if (!aClass.equals(b.getClass())) {
            return false;
        }
        if (boolean[].class.equals(aClass)) return Arrays.equals( (boolean[])a, (boolean[])b );
        if (byte[].class.equals(aClass)) return Arrays.equals( (byte[])a, (byte[])b );
        if (char[].class.equals(aClass)) return Arrays.equals( (char[])a, (char[])b );
        if (double[].class.equals(aClass)) return Arrays.equals( (double[])a, (double[])b );
        if (float[].class.equals(aClass)) return Arrays.equals( (float[])a, (float[])b );
        if (int[].class.equals(aClass)) return Arrays.equals( (int[])a, (int[])b );
        if (long[].class.equals(aClass)) return Arrays.equals( (long[])a, (long[])b );
        if (short[].class.equals(aClass)) return Arrays.equals( (short[])a, (short[])b );
        if (Object[].class.equals(aClass)) return Arrays.deepEquals( (Object[])a, (Object[])b );
        throw new AssertionError(aClass.getName());
    }

    private Equality() {}
}
