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

import com.akiban.util.ArgumentValidation;

public class TOverloadResult {

    public Category category() {
        return category;
    }

    public TClass fixed() {
        check(Category.FIXED);
        return fixedType;
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
        case FIXED:     return fixedType.toString();
        case PICKING:   return "pick from input set " + pickingInputSet;
        default: throw new AssertionError(category);
        }
    }

    // private

    private void check(Category expected) {
        assert category == expected : "expected " + expected + " but was " + category;
    }

    // state

    public TOverloadResult(TClass fixedType) {
        this(Category.FIXED, fixedType, -1, null, null);
        ArgumentValidation.notNull("fixed type", fixedType);
    }

    public TOverloadResult(int pickingInputSet) {
        this(Category.PICKING, null, pickingInputSet, null, null);
    }

    public TOverloadResult(TCustomOverloadResult rule) {
        this(rule, null);
    }

    public TOverloadResult(TCustomOverloadResult rule, TInstance castSource) {
        this(Category.CUSTOM, null, -1, rule, castSource);
        ArgumentValidation.notNull("custom combine rule", rule);
    }

    private TOverloadResult(Category category,
                            TClass fixedType,
                            int pickingInputSet,
                            TCustomOverloadResult customRule,
                            TInstance castSource)
    {
        this.category = category;
        this.fixedType = fixedType;
        this.pickingInputSet = pickingInputSet;
        this.customRule = customRule;
        this.castSource = castSource;
    }

    private final Category category;
    private final TClass fixedType;
    private final int pickingInputSet;
    private final TCustomOverloadResult customRule;
    private final TInstance castSource;

    public enum Category {
        CUSTOM, FIXED, PICKING
    }
}
