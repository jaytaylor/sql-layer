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

package com.akiban.sql.optimizer.rule.range;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.sql.optimizer.rule.range.TUtils.exclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.inclusive;
import static com.akiban.sql.optimizer.rule.range.TUtils.segment;
import static com.akiban.util.AssertUtils.assertCollectionEquals;
import static org.junit.Assert.assertNull;
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
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("apple")),
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
        List<RangeSegment> copy = new ArrayList<RangeSegment>(original);
        sacAndCheck(original, copy);
    }

    @Test
    public void sacSortNoCombine() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD),
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("apple"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("apple")),
                segment(exclusive("apple"), exclusive("orange")),
                segment(exclusive("orange"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacCombineInclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("apple")),
                segment(inclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineInclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveExclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(exclusive("apple"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineExclusiveInclusive() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), exclusive("apple")),
                segment(inclusive("apple"), inclusive("orange"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("orange"))
        );
    }

    @Test
    public void sacCombineStartExclusiveInclusiveNulls() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.NULL_EXCLUSIVE, exclusive("apple")),
                segment(RangeEndpoint.NULL_INCLUSIVE, inclusive("banana"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.NULL_INCLUSIVE, inclusive("banana"))
        );
    }

    @Test
    public void sacCombineStartInclusiveExclusiveNulls() {
        List<RangeSegment> original = Arrays.asList(
                segment(RangeEndpoint.NULL_INCLUSIVE, exclusive("apple")),
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("banana"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.NULL_INCLUSIVE, inclusive("banana"))
        );
    }

    @Test
    public void sacCombineStartExclusiveInclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("banana")),
                segment(inclusive("apple"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(inclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineStartInclusiveExclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("apple"), exclusive("banana")),
                segment(exclusive("apple"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(inclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineEndExclusiveInclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("mango")),
                segment(inclusive("banana"), inclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacCombineEndInclusiveExclusiveStrings() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), inclusive("mango")),
                segment(inclusive("banana"), exclusive("mango"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("mango"))
        );
    }

    @Test
    public void sacSubset() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("zebra")),
                segment(inclusive("cactus"), inclusive("person"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), exclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapNoWilds() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("person")),
                segment(inclusive("cactus"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(exclusive("apple"), inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildStart() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("apple"), exclusive("person")),
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(RangeEndpoint.NULL_EXCLUSIVE, inclusive("zebra"))
        );
    }

    @Test
    public void sacOverlapWildEnd() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), RangeEndpoint.UPPER_WILD),
                segment(exclusive("apple"), exclusive("person"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), RangeEndpoint.UPPER_WILD)
        );
    }

    @Test
    public void sacKeepStartAndEndInclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), inclusive("zebra"))
        );
    }
    @Test
    public void sacKeepStartExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(exclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), inclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(exclusive("aardvark"), inclusive("zebra"))
        );
    }
    
    @Test
    public void sacKeepEndExclusivity() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive("cat"), exclusive("zebra"))
        );
        sacAndCheck(
                original,
                segment(inclusive("aardvark"), exclusive("zebra"))
        );
    }

    @Test
    public void sacIncomparableTypesForSorting() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive("person")),
                segment(inclusive(1L), exclusive("zebra"))
        );
        List<RangeSegment> copy = new ArrayList<RangeSegment>(original);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertCollectionEquals(original, copy);
        assertNull("sorted list should be null", sorted);
    }

    @Test
    public void sacIncomparableTypesForMerging() {
        List<RangeSegment> original = Arrays.asList(
                segment(inclusive("aardvark"), inclusive(1L)),
                segment(inclusive("cat"), exclusive(1L))
        );
        List<RangeSegment> copy = new ArrayList<RangeSegment>(original);
        List<RangeSegment> sorted = RangeSegment.sortAndCombine(copy);
        assertCollectionEquals(original, copy);
        assertNull("sorted list should be null", sorted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startIsNull() {
        segment(null, inclusive(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void endIsNull() {
        segment(inclusive(1), null);
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
}
