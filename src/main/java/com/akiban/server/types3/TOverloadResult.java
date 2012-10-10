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

    public static TOverloadResult fixed(TInstanceGenerator tInstanceGenerator) {
        // This is not the most efficient in that it requires an extra/pointless allocation, but it's only invoked
        // at startup.
        return new TOverloadResult(Category.FIXED, tInstanceGenerator.tClass(), tInstanceGenerator.attrs(), null, null);
    }

    public static TOverloadResult fixed(TClass tClass, int... attrs) {
        return new TOverloadResult(Category.FIXED, tClass, attrs, null, null);
    }

    public static TOverloadResult picking() {
        return new TOverloadResult(Category.PICKING, null, null, null, null);
    }

    public static TOverloadResult custom(TInstanceGenerator castSource, TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, null, rule, castSource);
    }

    public static TOverloadResult custom(TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, null, rule, null);
    }

    public Category category() {
        return category;
    }

    public TInstance fixed(boolean nullable) {
        check(Category.FIXED);
        return fixedInstance.setNullable(nullable);
    }

    public TCustomOverloadResult customRule() {
        check(Category.CUSTOM);
        return customRule;
    }
    
    public TInstance customRuleCastSource(boolean nullable) {
        return castSource.setNullable(nullable);
    }

    // object interface

    @Override
    public String toString() {
        switch (category) {
        case CUSTOM:    return "custom";
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
                            TClass fixedTClass,
                            int[] fixedInstanceAttrs,
                            TCustomOverloadResult customRule,
                            TInstanceGenerator castSource)
    {
        this.category = category;
        this.fixedInstance = (fixedTClass == null) ? null : new TInstanceGenerator(fixedTClass, fixedInstanceAttrs);
        this.customRule = customRule;
        this.castSource = castSource;
        switch (category) {
        case CUSTOM:
            ArgumentValidation.notNull("custom rule", customRule);
            break;
        case FIXED:
            ArgumentValidation.notNull("fixed type", fixedTClass);
            ArgumentValidation.notNull("fixed type attributes", fixedInstanceAttrs);
            break;
        case PICKING:
            break;
        default:
            throw new AssertionError(category);
        }
    }

    private final Category category;
    private final TInstanceGenerator fixedInstance;
    private final TCustomOverloadResult customRule;
    private final TInstanceGenerator castSource;

    public enum Category {
        CUSTOM, FIXED, PICKING
    }
}
