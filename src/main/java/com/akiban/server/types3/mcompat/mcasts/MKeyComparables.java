/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TComparison;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.akiban.server.types3.pvalue.PValueSources.getLong;

public final class MKeyComparables {

    public static final TKeyComparable[] intComparisons = createIntComparisons();

    private static TKeyComparable[] createIntComparisons() {
        
        final TComparison integerComparison = new TComparison() {
            @Override
            public int compare(TInstance leftInstance, PValueSource left, TInstance rightInstance, PValueSource right) {
                return Longs.compare(getLong(left), getLong(right));
            }

            @Override
            public void copyComparables(PValueSource source, PValueTarget target)
            {
                PValueTargets.putLong(target, getLong(source));   
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
