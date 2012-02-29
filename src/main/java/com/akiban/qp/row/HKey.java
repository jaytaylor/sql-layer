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

package com.akiban.qp.row;

import com.akiban.server.types.ValueSource;

public interface HKey extends Comparable<HKey>
{
    // Object interface
    boolean equals(Object hKey);

    // Comparable interface
    @Override
    int compareTo(HKey o);

    // HKey interface
    boolean prefixOf(HKey hKey);
    int segments();
    void useSegments(int segments);
    void copyTo(HKey target);
    void extendWithOrdinal(int ordinal);
    void extendWithNull();
    ValueSource eval(int i);
}
