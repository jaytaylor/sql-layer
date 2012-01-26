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

public final class EquivalenceFinderTestZeroHashInteger extends EquivalenceFinderTestBase<WeirdlyHashingInteger> {
    @Override
    protected WeirdlyHashingInteger one() {
        return new ConstHashInteger(1);
    }

    @Override
    protected WeirdlyHashingInteger two() {
        return new ConstHashInteger(2);
    }

    @Override
    protected WeirdlyHashingInteger three() {
        return new ConstHashInteger(3);
    }

    @Override
    protected WeirdlyHashingInteger four() {
        return new ConstHashInteger(4);
    }

    @Override
    protected WeirdlyHashingInteger five() {
        return new ConstHashInteger(5);
    }

    @Override
    protected WeirdlyHashingInteger six() {
        return new ConstHashInteger(6);
    }

    @Override
    protected WeirdlyHashingInteger seven() {
        return new ConstHashInteger(7);
    }

    private static class ConstHashInteger extends WeirdlyHashingInteger {

        private ConstHashInteger(int value) {
            super(value);
        }

        @Override
        protected int hashCode(int value) {
            return 0;
        }
    }
}
