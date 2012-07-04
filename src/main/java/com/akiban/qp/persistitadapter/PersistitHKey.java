/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.qp.row.HKey;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
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
     
    @Override
    public PValueSource pEval(int i) {
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
    
    private PersistitKeyPValueSource pSource(int i) 
    {
        if (pSources == null) {
            assert underlyingTypes == null;
            pSources = new PersistitKeyPValueSource[hKeyMetadata.nColumns()];
            underlyingTypes = new PUnderlying[hKeyMetadata.nColumns()];
            for (int c = 0; c < hKeyMetadata.nColumns(); c++) {
                underlyingTypes[c] = hKeyMetadata.column(c).tInstance().typeClass().underlyingType();
            }
        }
        if (pSources[i] == null) {
            pSources[i] = new PersistitKeyPValueSource(underlyingTypes[i]);
            pSources[i].attach(hKey, keyDepth[i], underlyingTypes[i]);
        } else {
            // TODO: Add state tracking whether hkey has been changed (e.g. by useSegments). Avoid attach calls
            // TODO: when there has been no change.
            pSources[i].attach(hKey);
        }
        return pSources[i];
    }

    // Object state

    private final com.akiban.ais.model.HKey hKeyMetadata;
    private final Key hKey;
    private final int hKeySegments;
    private int originalHKeySize;
    // Identifies the persistit key depth for the ith hkey segment, 1 <= i <= #hkey segments.
    private final int[] keyDepth;
    private PersistitKeyValueSource[] sources;
    private PersistitKeyPValueSource[] pSources;
    private AkType[] types;
    private PUnderlying[] underlyingTypes;
}
