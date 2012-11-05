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

package com.akiban.server.types3;

import java.util.Arrays;

public final class TInstanceGenerator {
    public TInstance setNullable(boolean isNullable) {
        switch (attrs.length) {
        case 0:
            return tclass.instance(isNullable);
        case 1:
            return tclass.instance(attrs[0], isNullable);
        case 2:
            return tclass.instance(attrs[0], attrs[1], isNullable);
        case 3:
            return tclass.instance(attrs[0], attrs[1], attrs[2], isNullable);
        case 4:
            return tclass.instance(attrs[0], attrs[1], attrs[2], attrs[3], isNullable);
        default:
            throw new AssertionError("too many attrs!: " + Arrays.toString(attrs) + " with " + tclass);
        }
    }

    int[] attrs() {
        return Arrays.copyOf(attrs, attrs.length);
    }

    TClass tClass() {
        return tclass;
    }

    public String toString(boolean shorthand) {
        return setNullable(true).toStringIgnoringNullability(shorthand);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public TInstanceGenerator(TClass tclass, int... attrs) {
        this.tclass = tclass;
        this.attrs = Arrays.copyOf(attrs, attrs.length);
    }
    private final TClass tclass;

    private final int[] attrs;
}
