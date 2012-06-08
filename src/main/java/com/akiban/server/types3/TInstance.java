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

public final class TInstance {

    public int attribute(int index) {
        switch (index) {
        case 0: return attr0;
        case 1: return attr1;
        case 2: return attr2;
        case 3: return attr3;
        default: throw new IllegalArgumentException("index out of range for " + tclass +  ": " + index);
        }
    }

    public TClass typeClass() {
        return tclass;
    }

    public TInstance(TClass tclass) {
        this(tclass, 0, EMPTY, EMPTY, EMPTY, EMPTY);
    }

    public TInstance(TClass tclass, int attr0) {
        this(tclass, 1, attr0, EMPTY, EMPTY, EMPTY);
    }
    public TInstance(TClass tclass, int attr0, int attr1) {
        this(tclass, 2, attr0, attr1, EMPTY, EMPTY);
    }

    public TInstance(TClass tclass, int attr0, int attr1, int attr2) {
        this(tclass, 3, attr0, attr1, attr2, EMPTY);
    }

    public TInstance(TClass tclass, int attr0, int attr1, int attr2, int attr3) {
        this(tclass, 4, attr0, attr1, attr2, attr3);
    }

    public boolean isNullable() {
        if (isNullable == null)
            throw new IllegalStateException("nullability has not been set");
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    // object interface

    // TODO

    private TInstance(TClass tclass, int nAttrs, int attr0, int attr1, int attr2, int attr3) {
        assert nAttrs == tclass.nAttributes() : "expected " + tclass.nAttributes() + " attributes but got " + nAttrs;
        this.tclass = tclass;
        this.attr0 = attr0;
        this.attr1 = attr1;
        this.attr2 = attr2;
        this.attr3 = attr3;
    }

    private final TClass tclass;
    private final int attr0, attr1, attr2, attr3;
    private Boolean isNullable;

    private static final int EMPTY = -1;
}
