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

public abstract class EquivalenceFinderTestBase<T> {

    @Test
    public void identity() {
        check(create() , true, one(), one());
    }

    protected abstract T one();
    protected abstract T two();
    protected abstract T three();
    protected abstract T four();
    protected abstract T five();
    protected abstract T six();
    protected abstract T seven();

    @Test
    public void notEquivalent() {
        check(create(), false, one(), two());
    }

    @Test
    public void simple() {
        EquivalenceFinder<T> finder = create();
        finder.markEquivalent(one(), two());
        check(finder, true, one(), two());
    }

    @Test
    public void transitive() {
        EquivalenceFinder<T> finder = create();
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), three());

        check(finder, true, one(), two());
        check(finder, true, one(), three());
        check(finder, true, two(), three());
    }

    @Test
    public void transitiveBushy() {
        EquivalenceFinder<T> finder = create();
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), three());
        finder.markEquivalent(two(), four());
        finder.markEquivalent(two(), five());
        finder.markEquivalent(five(), six());

        check(finder, true, one(), six());
    }

    @Test
    public void loopedOneApart() {
        EquivalenceFinder<T> finder = create();
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), one());

        check(finder, true, one(), two());
    }

    @Test
    public void loopedTwoApart() {
        EquivalenceFinder<T> finder = create();
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), three());
        finder.markEquivalent(three(), one());

        check(finder, true, one(), two());
        check(finder, true, one(), three());
        check(finder, true, two(), three());
    }

    @Test
    public void traverseBarelyWorks() {
        EquivalenceFinder<T> finder = create(5);
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), three());
        finder.markEquivalent(three(), four());
        finder.markEquivalent(four(), five());
        finder.markEquivalent(five(), six());

        check(finder, true, one(), six());
    }

    @Test
    public void traverseBarelyFails() {
        EquivalenceFinder<T> finder = create(4);
        finder.markEquivalent(one(), two());
        finder.markEquivalent(two(), three());
        finder.markEquivalent(three(), four());
        finder.markEquivalent(four(), five());
        finder.markEquivalent(five(), six());

        check(finder, null, one(), six());
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

    private static <T> EquivalenceFinder<T> create() {
        return new TraversalBoundEquivalenceFinder<T>();
    }

    private static <T> EquivalenceFinder<T> create(int maxTraversal) {
        return new TraversalBoundEquivalenceFinder<T>(maxTraversal);
    }
    
    private static class TooMuchTraversingException extends RuntimeException {
    }

    private static class TraversalBoundEquivalenceFinder<T> extends EquivalenceFinder<T> {

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
