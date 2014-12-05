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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;

public final class TComparisonExpression extends TComparisonExpressionBase {

    @Override
    protected int compare(TInstance leftInstance, ValueSource leftSource,
                          TInstance rightInstance, ValueSource rightSource)
    {
        TClass tClass = leftInstance.typeClass();
        if (collator != null) {
            assert tClass.underlyingType() == UnderlyingType.STRING : tClass + ": " + tClass.underlyingType();
            String leftString = leftSource.getString();
            String rightString = rightSource.getString();
            return collator.compare(leftString, rightString);
        }
        else {
            return TClass.compare(leftInstance, leftSource, rightInstance, rightSource);
        }
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this(left, comparison, right, null);
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right,
                                 AkCollator collator) {
        super(left, comparison, right);
        TClass oneClass = left.resultType().typeClass();
        TClass twoClass = right.resultType().typeClass();
        if (!oneClass.compatibleForCompare(twoClass))
            throw new IllegalArgumentException("can't compare expressions of different types: " + left + " != " + right);
        if (collator != null && oneClass.underlyingType() != UnderlyingType.STRING) {
            throw new IllegalArgumentException("collator provided, but " + oneClass + " is not a string type");
        }
        this.collator = collator;
    }

    // Collator in advance saves mergeCollations() every eval as TClass.compare() would do
    private final AkCollator collator;
}
