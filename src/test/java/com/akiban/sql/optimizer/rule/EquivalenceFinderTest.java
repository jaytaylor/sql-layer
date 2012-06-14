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

package com.akiban.sql.optimizer.rule;

import com.akiban.util.AssertUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class EquivalenceFinderTest {

    @Test
    public void identity() {
        check(create() , true, 1, 1);
    }

    @Test
    public void notEquivalent() {
        check(create(), false, 1, 2);
    }

    @Test
    public void simple() {
        EquivalenceFinder<Integer> finder = create();
        finder.markEquivalent(1, 2);
        check(finder, true, 1, 2);
    }

    @Test
    public void transitive() {
        EquivalenceFinder<Integer> finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);

        check(finder, true, 1, 2);
        check(finder, true, 1, 3);
        check(finder, true, 2, 3);
    }

    @Test
    public void transitiveBushy() {
        EquivalenceFinder<Integer> finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        finder.markEquivalent(2, 4);
        finder.markEquivalent(2, 5);
        finder.markEquivalent(5, 6);

        check(finder, true, 1, 6);
    }

    @Test
    public void loopedOneApart() {
        EquivalenceFinder<Integer> finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 1);

        check(finder, true, 1, 2);
    }

    @Test
    public void loopedTwoApart() {
        EquivalenceFinder<Integer> finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        finder.markEquivalent(3, 1);

        check(finder, true, 1, 2);
        check(finder, true, 1, 3);
        check(finder, true, 2, 3);
    }

    @Test
    public void traverseBarelyWorks() {
        EquivalenceFinder<Integer> finder = create(6);
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        finder.markEquivalent(3, 4);
        finder.markEquivalent(4, 5);
        finder.markEquivalent(5, 6);

        check(finder, true, 1, 6);
    }

    @Test
    public void traverseBarelyFails() {
        EquivalenceFinder<Integer> finder = create(4);
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        finder.markEquivalent(3, 4);
        finder.markEquivalent(4, 5);
        finder.markEquivalent(5, 6);

        check(finder, null, 1, 6);
    }
    
    @Test
    public void emptyEquivalenceSet() {
        EquivalenceFinder<Integer>  finder = create();
        checkEquivalents(1, finder);
    }

    @Test
    public void noneEquivalenceSet() {
        EquivalenceFinder<Integer>  finder = create();
        finder.markEquivalent(2, 3);
        checkEquivalents(1, finder);
    }

    @Test
    public void someEquivalenceSet() {
        EquivalenceFinder<Integer>  finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        checkEquivalents(1, finder, 2, 3);
    }

    @Test
    public void loopedEquivalenceSet() {
        EquivalenceFinder<Integer>  finder = create();
        finder.markEquivalent(1, 2);
        finder.markEquivalent(2, 3);
        finder.markEquivalent(3, 1);
        checkEquivalents(1, finder, 2, 3);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullEquivalence() {
        create().markEquivalent(1, null);
    }
    
    protected static void checkEquivalents(Integer from, EquivalenceFinder<? super Integer> finder, Integer... expected) {
        Set<Integer> expectedSet = new HashSet<Integer>();
        Collections.addAll(expectedSet, expected);
        AssertUtils.assertCollectionEquals("equivalents for " + from, expectedSet, finder.findEquivalents(from));
    }
    
    private static <T> void check(EquivalenceFinder<? super T> finder, Boolean expected, T one, T two) {
        checkOneDirection(finder, expected, one, two);
        checkOneDirection(finder, expected, two, one);
    }

    private static <T> void checkOneDirection(EquivalenceFinder<? super T> finder, Boolean expected, T one, T two) {
        Boolean result;
        try {
            result = finder.areEquivalent(one, two);
        } catch (TooMuchTraversingException e) {
            result = null;
        }
        assertEquals("equivalence of " + one + ", " + two, expected, result);
    }

    private static EquivalenceFinder<Integer> create() {
        return new TraversalBoundEquivalenceFinder<Integer>();
    }

    private static EquivalenceFinder<Integer> create(int maxTraversal) {
        return new TraversalBoundEquivalenceFinder<Integer>(maxTraversal);
    }
    
    private static class TooMuchTraversingException extends RuntimeException {
    }

    private static class TraversalBoundEquivalenceFinder<Integer> extends EquivalenceFinder<Integer> {

        private TraversalBoundEquivalenceFinder() {
        }

        public TraversalBoundEquivalenceFinder(int maxTraversal) {
            super(maxTraversal);
        }

        @Override
        void tooMuchTraversing() {
            throw new TooMuchTraversingException();
        }
    }
}
