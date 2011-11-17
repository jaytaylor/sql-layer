/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.util.AssertUtils.assertCollectionEquals;
import static org.junit.Assert.assertSame;

public final class RangeSegmentTest {

    @Test
    public void sacEmptyList() {
        List<RangeSegment> original = Collections.emptyList();
        sacAndCheck(original, Collections.<RangeSegment>emptyList());
    }

    @Test
    public void sacUnchanged() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.LOWER_WILD, RangeEndpoint.exclusive("apple")),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("orange")),
                segment(RangeEndpoint.exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
        List<RangeSegment> copy = new ArrayList<RangeSegment>(original);
        sacAndCheck(original, copy);
    }

    @Test
    public void sacSortNoCombine() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("orange")),
                segment(RangeEndpoint.exclusive("orange"), RangeEndpoint.UPPER_WILD),
                segment(RangeEndpoint.LOWER_WILD, RangeEndpoint.exclusive("apple"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.LOWER_WILD, RangeEndpoint.exclusive("apple")),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("orange")),
                segment(RangeEndpoint.exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacCombineInclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("apple")),
                segment(RangeEndpoint.inclusive("apple"), RangeEndpoint.inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("orange"))
        );
    }

    @Test
    public void sacCombineInclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("apple")),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.exclusive("apple")),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.exclusive("apple")),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.exclusive("apple")),
                segment(RangeEndpoint.inclusive("apple"), RangeEndpoint.inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("orange"))
        );
    }

    @Test
    public void sacSubset() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("zebra")),
                segment(RangeEndpoint.inclusive("cactus"), RangeEndpoint.inclusive("person"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapNoWilds() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("person")),
                segment(RangeEndpoint.inclusive("cactus"), RangeEndpoint.inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildStart() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("person")),
                segment(RangeEndpoint.LOWER_WILD, RangeEndpoint.inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.LOWER_WILD, RangeEndpoint.inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildEnd() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.UPPER_WILD),
                segment(RangeEndpoint.exclusive("apple"), RangeEndpoint.exclusive("person"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacKeepStartAndEndInclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.inclusive("person")),
                segment(RangeEndpoint.inclusive("cat"), RangeEndpoint.inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.inclusive("zebra"))
        );
    }
    @Test
    public void sacKeepStartExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("person")),
                segment(RangeEndpoint.inclusive("cat"), RangeEndpoint.inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.exclusive("aardvark"), RangeEndpoint.inclusive("zebra"))
        );
    }
    
    @Test
    public void sacKeepEndExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.inclusive("person")),
                segment(RangeEndpoint.inclusive("cat"), RangeEndpoint.exclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.inclusive("aardvark"), RangeEndpoint.exclusive("zebra"))
        );
    }

    /**
     * checks sortAndCombine (aka sac) by sac'ing (a copy of) the given list, asserting that what comes out
     * is the same instance, and checking it for equality against a list created from the given expected RangeSegments.
     * @param list the list to sac-and-check
     * @param expectedList the expected RangeSegments
     */
    private static void sacAndCheck(List<? extends RangeSegment> list, List<? extends RangeSegment> expectedList) {
        List<RangeSegment> copy = new ArrayList<RangeSegment>(list);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertSame(copy, sorted);
        assertCollectionEquals(expectedList, new ArrayList<RangeSegment>(sorted));
    }

    private static void sacAndCheck(List<RangeSegment> list, RangeSegment first, RangeSegment... expecteds) {
        List<RangeSegment> expectedsList = new ArrayList<RangeSegment>();
        expectedsList.add(first);
        Collections.addAll(expectedsList, expecteds);
        sacAndCheck(list, expectedsList);
    }

    // purely for convenience

    private static RangeSegment segment(RangeEndpoint start, RangeEndpoint end) {
        return new RangeSegment(start, end);
    }

}
