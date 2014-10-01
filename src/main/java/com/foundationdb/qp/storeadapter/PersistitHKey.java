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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.qp.row.HKey;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;

public class PersistitHKey implements HKey
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
                that instanceof PersistitHKey &&
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
    public ValueSource pEval(int i) {
        return pSource(i);
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

    public PersistitHKey(Key key, com.foundationdb.ais.model.HKey hKeyMetadata)
    {
        this.hKeyMetadata = hKeyMetadata;
        this.hKey = key;
        this.hKeySegments = hKeyMetadata.segments().size();
        this.keyDepth = hKeyMetadata.keyDepth();
    }

    public Key key()
    {
        return hKey;
    }
    
    // For use by this class
    
    private PersistitKeyValueSource pSource(int i)
    {
        if (pSources == null) {
            assert underlyingTypes == null;
            pSources = new PersistitKeyValueSource[hKeyMetadata.nColumns()];
            underlyingTypes = new TInstance[hKeyMetadata.nColumns()];
            for (int c = 0; c < hKeyMetadata.nColumns(); c++) {
                underlyingTypes[c] = hKeyMetadata.column(c).getType();
            }
        }
        if (pSources[i] == null) {
            pSources[i] = new PersistitKeyValueSource(underlyingTypes[i]);
            pSources[i].attach(hKey, keyDepth[i], underlyingTypes[i]);
        } else {
            // TODO: Add state tracking whether hkey has been changed (e.g. by useSegments). Avoid attach calls
            // TODO: when there has been no change.
            pSources[i].attach(hKey);
        }
        return pSources[i];
    }

    // Object state

    private final com.foundationdb.ais.model.HKey hKeyMetadata;
    private final Key hKey;
    private final int hKeySegments;
    private int originalHKeySize;
    // Identifies the persistit key depth for the ith hkey segment, 1 <= i <= #hkey segments.
    private final int[] keyDepth;
    private PersistitKeyValueSource[] pSources;
    private TInstance[] underlyingTypes;
}
