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
