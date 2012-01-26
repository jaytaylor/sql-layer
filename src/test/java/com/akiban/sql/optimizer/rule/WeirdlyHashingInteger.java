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

abstract class WeirdlyHashingInteger {

    WeirdlyHashingInteger(int value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeirdlyHashingInteger that = (WeirdlyHashingInteger) o;
        return value == that.value;

    }

    @Override
    public String toString() {
        return String.format("(%d w/hash=%d)", value, hashCode());
    }

    @Override
    public int hashCode() {
        return hashCode(value);
    }

    protected abstract int hashCode(int value);

    private int value;
}
