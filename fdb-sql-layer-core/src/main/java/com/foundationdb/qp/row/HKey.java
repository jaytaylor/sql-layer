/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.row;

import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;

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
    
    // testing only 
    ValueSource value(int index);
    
    // Lower level interface
    public void copyTo (Key start);
    public void copyFrom(Key source);
    public void copyFrom (byte[] source);
    public byte[] hKeyBytes();
}
