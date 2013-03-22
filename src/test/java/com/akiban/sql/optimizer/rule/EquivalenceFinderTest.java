
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
        Set<Integer> expectedSet = new HashSet<>();
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
        return new TraversalBoundEquivalenceFinder<>();
    }

    private static EquivalenceFinder<Integer> create(int maxTraversal) {
        return new TraversalBoundEquivalenceFinder<>(maxTraversal);
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
