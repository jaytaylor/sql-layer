/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types;

import com.foundationdb.util.ArgumentValidation;

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
        // TODO what is a TOverloadResult and why is a custom one sometimes created without a castSource?
        // perhaps subclassing TOverloadResult would clean this up.
        return castSource == null ? null : castSource.setNullable(nullable);
    }

    // object interface

    public String toString(boolean useShorthand) {
        switch (category) {
        case CUSTOM:    return "custom";
        case FIXED:     return fixedInstance.toString(useShorthand);
        case PICKING:   return "picking";
        default: throw new AssertionError(category);
        }
    }

    @Override
    public String toString() {
        return toString(false);
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
