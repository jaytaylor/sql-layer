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

import com.akiban.server.types3.common.types.NoAttrTClass;

public class TOverloadResult {
    
    public static TOverloadResult fixed(NoAttrTClass plainTClass) {
        return new TOverloadResult(Category.FIXED, plainTClass.instance(), null, null);
    }

    public static TOverloadResult fixed(TInstance tInstance) {
        return new TOverloadResult(Category.FIXED, tInstance, null, null);
    }

    public static TOverloadResult picking() {
        return new TOverloadResult(Category.PICKING, null, null, null);
    }

    public static TOverloadResult custom(TInstance castSource, TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, rule, castSource);
    }

    public static TOverloadResult custom(TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, rule, null);
    }

    public Category category() {
        return category;
    }

    public TInstance fixed() {
        check(Category.FIXED);
        return fixedInstance;
    }

    public TCustomOverloadResult customRule() {
        check(Category.CUSTOM);
        return customRule;
    }
    
    public TInstance customRuleCastSource() {
        return castSource;
    }

    // object interface

    @Override
    public String toString() {
        switch (category) {
        case CUSTOM:    return "custom rule";
        case FIXED:     return fixedInstance.toString();
        case PICKING:   return "picking";
        default: throw new AssertionError(category);
        }
    }

    // private

    private void check(Category expected) {
        assert category == expected : "expected " + expected + " but was " + category;
    }

    // state

    private TOverloadResult(Category category,
                            TInstance fixedInstance,
                            TCustomOverloadResult customRule,
                            TInstance castSource)
    {
        this.category = category;
        this.fixedInstance = fixedInstance;
        this.customRule = customRule;
        this.castSource = castSource;
    }

    private final Category category;
    private final TInstance fixedInstance;
    private final TCustomOverloadResult customRule;
    private final TInstance castSource;

    public enum Category {
        CUSTOM, FIXED, PICKING
    }
}
