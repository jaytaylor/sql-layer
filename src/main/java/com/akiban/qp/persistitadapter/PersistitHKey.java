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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.HKey;
import com.persistit.Key;

class PersistitHKey implements HKey
{
    // Object interface

    @Override
    public String toString()
    {
        return persistitKey.toString();
    }

    // HKey interface

    public int segments()
    {
        return hKey.segments().size();
    }

    public void useSegments(int segments)
    {
        assert segments > 0 && segments < keyDepth.length : segments;
        // setDepth shortens the key's encoded size if necessary but doesn't lengthen it.
        // So setEncodedSize back to the original key length, permitting setDepth to work in all cases.
        // (setEncodedSize is cheap.)
        persistitKey.setEncodedSize(persistitKeySize);
        persistitKey.setDepth(keyDepth[segments]);
    }

    // TODO: Move into Key?
    public boolean prefixOf(HKey hKey)
    {
        PersistitHKey that = (PersistitHKey) hKey;
        if (this.persistitKeySize <= that.persistitKeySize) {
            byte[] thisBytes = this.persistitKey.getEncodedBytes();
            byte[] thatBytes = that.persistitKey.getEncodedBytes();
            for (int i = 0; i < this.persistitKeySize; i++) {
                if (thisBytes[i] != thatBytes[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    // PersistitHKey interface

    public void copyFrom(Key source)
    {
        source.copyTo(persistitKey);
        persistitKeySize = persistitKey.getEncodedSize();
    }

    public void copyTo(Key target)
    {
        persistitKey.copyTo(target);
    }

    public PersistitHKey(PersistitAdapter adapter, com.akiban.ais.model.HKey hKey)
    {
        this.adapter = adapter;
        this.hKey = hKey;
        this.persistitKey = new Key(adapter.persistit.getDb());
        this.keyDepth = new int[hKey.segments().size() + 1];
        int hKeySegments = hKey.segments().size();
        for (int hKeySegment = 0; hKeySegment <= hKeySegments; hKeySegment++) {
            this.keyDepth[hKeySegment] =
                hKeySegment == 0
                ? 0
                // + 1 to account for the ordinal
                : this.keyDepth[hKeySegment - 1] + 1 + hKey.segments().get(hKeySegment - 1).columns().size();
        }
    }

    // Object state

    private final PersistitAdapter adapter;
    private final com.akiban.ais.model.HKey hKey;
    private Key persistitKey;
    private int persistitKeySize;
    // Identifies the persistit key depth for the ith hkey segment, 1 <= i <= #hkey segments.
    private final int[] keyDepth;
}
