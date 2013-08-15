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

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.SequenceValuesValid;
import com.foundationdb.server.error.SequenceIntervalZeroException;
import com.foundationdb.server.error.SequenceLimitExceededException;
import com.foundationdb.server.error.SequenceMinGEMaxException;
import com.foundationdb.server.error.SequenceStartInRangeException;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SequenceTest
{
    private static final boolean CYCLE = true;
    private static final boolean NO_CYCLE = false;

    private Sequence s(long start, long inc, long min, long max, boolean isCycle) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Sequence s = Sequence.create(ais, "test", "s", start, inc, min, max, isCycle);
        SequenceValuesValid valuesValid = new SequenceValuesValid();
        ais.validate(Arrays.asList(valuesValid)).throwIfNecessary();
        return s;
    }

    private void expectExceeded(Sequence s, long rawValue) {
        try {
            s.realValueForRawNumber(rawValue);
            fail("Expected limit exceeded");
        } catch(SequenceLimitExceededException e) {
            // Expected
        }
    }

    private void c(Sequence s, long rawNumber, long expectedRealValue) {
        assertEquals("realValueForRawNumber at raw="+rawNumber, expectedRealValue, s.realValueForRawNumber(rawNumber));
    }

    @Test(expected=SequenceIntervalZeroException.class)
    public void zeroInc() {
        s(1, 0, 1, 10, CYCLE);
    }

    @Test(expected=SequenceMinGEMaxException.class)
    public void minEqualMax() {
        s(1, 1, 10, 10, CYCLE);
    }

    @Test(expected=SequenceMinGEMaxException.class)
    public void minGreaterMax() {
        s(1, 1, 15, 10, CYCLE);
    }

    @Test(expected=SequenceStartInRangeException.class)
    public void startLessMin() {
        s(1, 1, 5, 10, CYCLE);
    }

    @Test(expected=SequenceStartInRangeException.class)
    public void startGreaterMax() {
        s(15, 1, 5, 10, CYCLE);
    }

    @Test
    public void firstValueIsStart() {
        Sequence s = s(5, 1, 1, 10, CYCLE);
        c(s, 1, 5);
    }

    @Test
    public void startOne_IncOne_MinOne_MaxTen_Cycle() {
        long min = 1;
        long max = 10;
        Sequence s = s(1, 1, 1, 10, CYCLE);
        for(long i = min; i <= max; ++i) {
            c(s, i, i);
        }
        c(s, max + 1, min);
    }

    @Test
    public void startOne_IncOne_MinOne_MaxTen_NoCycle() {
        long min = 1;
        long max = 10;
        Sequence s = s(1, 1, min, max, NO_CYCLE);
        for(long i = min; i <= max; ++i) {
            c(s, i, i);
        }
        expectExceeded(s, max+1);
    }

    @Test
    public void startOne_IncThree_MinOne_MaxTen_Cycle() {
        long inc = 3;
        long min = 1;
        long max = 10;
        Sequence s = s(1, inc, min, max, CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, 7);
        c(s, 4, 10);
        c(s, 5, 3);
    }

    @Test
    public void startOne_IncThree_MinOne_MaxTen_NoCycle() {
        long inc = 3;
        long min = 1;
        long max = 10;
        Sequence s = s(1, inc, min, max, NO_CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, 7);
        c(s, 4, 10);
        expectExceeded(s, 5);
    }

    @Test
    public void startOne_IncThree_MinNegFive_MaxFive_Cycle() {
        long inc = 3;
        long min = -5;
        long max = 5;
        Sequence s = s(1, inc, min, max, CYCLE);
        c(s, 1, 1);
        c(s, 2, 4);
        c(s, 3, -4);
        c(s, 4, -1);
        c(s, 5, 2);
    }

    @Test
    public void cycleWithLongMinAndMax() {
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        Sequence s = s(1, 1, min, max, CYCLE);
        c(s, 1, 1);
        c(s, max - 1, max - 1);
        c(s, max, max);
        c(s, max + 1, max + 1);
    }
}
