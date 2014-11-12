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

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.foundationdb.server.types.value.ValueSources.getLong;

public final class MKeyComparables {

    public static final TKeyComparable[] intComparisons = createIntComparisons();

    private static TKeyComparable[] createIntComparisons() {
        
        final TComparison integerComparison = new TComparison() {
            @Override
            public int compare(TInstance leftInstance, ValueSource left, TInstance rightInstance, ValueSource right) {
                if(left.isNull()) {
                    if(right.isNull()) {
                        return 0;
                    }
                    return -1;
                }
                if(right.isNull()) {
                    return 1;
                }
                return Longs.compare(getLong(left), getLong(right));
            }

            @Override
            public void copyComparables(ValueSource source, ValueTarget target)
            {
                ValueTargets.putLong(target, getLong(source));
            }
        };

        List<TClass> candidates = Arrays.<TClass>asList(
                MNumeric.TINYINT, MNumeric.SMALLINT, MNumeric.MEDIUMINT, MNumeric.INT, MNumeric.BIGINT,
                MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT_UNSIGNED, MNumeric.MEDIUMINT_UNSIGNED,
                MNumeric.INT_UNSIGNED
                // BIGINT UNSIGNED is not here, because its representation is not a signed long
        );

        Set<Set<TClass>> alreadyCreated = new HashSet<>();
        List<TKeyComparable> results = new ArrayList<>();
        for (TClass outer : candidates) {
            for (TClass inner : candidates) {
                if (inner == outer)
                    continue;
                Set<TClass> pair = new HashSet<>(Arrays.asList(inner, outer));
                if (alreadyCreated.add(pair)) {
                    results.add(new TKeyComparable(outer, inner, integerComparison));
                }
            }
        }
        return results.toArray(new TKeyComparable[results.size()]);
    }
    
    
    private MKeyComparables() {}
}
