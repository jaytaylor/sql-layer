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

import com.akiban.qp.row.HKey;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.persistit.Key;

class PersistitHKey implements HKey
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey.toString();
    }

    @Override
    public boolean equals(Object that)
    {
        return
            that == this ||
            (that != null &&
             PersistitHKey.class.equals(that.getClass()) &&
             this.hKey.equals(((PersistitHKey)that).hKey));
    }

    @Override
    public int hashCode() {
        return hKey.hashCode();
    }

    // Comparable interface

    @Override
    public int compareTo(HKey that)
    {
        return this.hKey.compareTo(((PersistitHKey)that).hKey);
    }


    // HKey interface

    public int segments()
    {
        return hKeySegments;
    }

    public void useSegments(int segments)
    {
        assert segments > 0 && segments < keyDepth.length : segments;
        // setDepth shortens the key's encoded size if necessary but doesn't lengthen it.
        // So setEncodedSize back to the original key length, permitting setDepth to work in all cases.
        // (setEncodedSize is cheap.)
        hKey.setEncodedSize(originalHKeySize);
        hKey.setDepth(keyDepth[segments]);
    }

    // TODO: Move into Key?
    public boolean prefixOf(HKey hKey)
    {
        boolean prefix = false;
        PersistitHKey that = (PersistitHKey) hKey;
        int thisHKeySize = this.hKey.getEncodedSize();
        int thatHKeySize = that.hKey.getEncodedSize();
        if (thisHKeySize <= thatHKeySize) {
            byte[] thisBytes = this.hKey.getEncodedBytes();
            byte[] thatBytes = that.hKey.getEncodedBytes();
            for (int i = 0; i < thisHKeySize; i++) {
                if (thisBytes[i] != thatBytes[i]) {
                    return false;
                }
            }
            prefix = true;
        }
        return prefix;
    }

    @Override
    public void copyTo(HKey target)
    {
        assert target instanceof PersistitHKey;
        PersistitHKey that = (PersistitHKey) target;
        that.copyFrom(hKey);
    }

    @Override
    public void extendWithOrdinal(int ordinal)
    {
        hKey.append(ordinal);
    }

    @Override
    public void extendWithNull()
    {
        hKey.append(null);
    }

    @Override
    public ValueSource eval(int i)
    {
        return source(i);
    }

    // PersistitHKey interface

    public void copyFrom(Key source)
    {
        source.copyTo(hKey);
        originalHKeySize = hKey.getEncodedSize();
    }

    public void copyTo(Key target)
    {
        hKey.copyTo(target);
    }

    public PersistitHKey(PersistitAdapter adapter, com.akiban.ais.model.HKey hKeyMetadata)
    {
        this.hKeyMetadata = hKeyMetadata;
        this.hKey = adapter.newKey();
        this.hKeySegments = hKeyMetadata.segments().size();
        this.keyDepth = hKeyMetadata.keyDepth();
    }

    // For use by this package

    Key key()
    {
        return hKey;
    }
    
    // For use by this class
    
    private PersistitKeyValueSource source(int i)
    {
        if (sources == null) {
            assert types == null;
            sources = new PersistitKeyValueSource[hKeyMetadata.nColumns()];
            types = new AkType[hKeyMetadata.nColumns()];
            for (int c = 0; c < hKeyMetadata.nColumns(); c++) {
                types[c] = hKeyMetadata.columnType(c);
            }
        }
        if (sources[i] == null) {
            sources[i] = new PersistitKeyValueSource();
            sources[i].attach(hKey, keyDepth[i], types[i]);
        } else {
            // TODO: Add state tracking whether hkey has been changed (e.g. by useSegments). Avoid attach calls
            // TODO: when there has been no change.
            sources[i].attach(hKey);
        }
        return sources[i];
    }

    // Object state

    private final com.akiban.ais.model.HKey hKeyMetadata;
    private final Key hKey;
    private final int hKeySegments;
    private int originalHKeySize;
    // Identifies the persistit key depth for the ith hkey segment, 1 <= i <= #hkey segments.
    private final int[] keyDepth;
    private PersistitKeyValueSource[] sources;
    private AkType[] types;
}
