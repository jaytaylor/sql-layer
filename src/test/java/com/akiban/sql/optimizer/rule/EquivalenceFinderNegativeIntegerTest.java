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

public final class EquivalenceFinderNegativeIntegerTest extends EquivalenceFinderTestBase<Integer> {
    @Override
    protected Integer one() {
        return -1;
    }

    @Override
    protected Integer two() {
        return -2;
    }

    @Override
    protected Integer three() {
        return -3;
    }

    @Override
    protected Integer four() {
        return -4;
    }

    @Override
    protected Integer five() {
        return -5;
    }

    @Override
    protected Integer six() {
        return -6;
    }

    @Override
    protected Integer seven() {
        return -7;
    }
}
