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

package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.IndexToHKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;
import com.persistit.Key;
import com.persistit.Value;

public abstract class PersistitIndexRow extends PersistitIndexRowBuffer
{
    // Object interface

    @Override
    public final String toString()
    {
        AkibanAppender buffer = AkibanAppender.of(new StringBuilder());
        buffer.append("(");
        for (int i = 0; i < nIndexFields; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            types[i].format(value(i), buffer);
        }
        buffer.append(")->");
        buffer.append(hKey().toString());
        return buffer.toString();
    }
    
    // Row interface

    // TODO: This is not a correct implementation of hKey, because it returns an empty hKey to be filled in
    // TODO: by the caller. Normally, hKey returns the HKey of the row.
    @Override
    public final HKey hKey()
    {
        return hKeyCache.hKey(leafmostTable);
    }

    @Override
    public final IndexRowType rowType()
    {
        return indexRowType;
    }

    @Override
    public final ValueSource uncheckedValue(int i)
    {
        TInstance type = types[i];
        PersistitKeyValueSource keySource = keyPSource(i, type);
        attach(keySource, i, type);
        return keySource;
    }

    // PersistitIndexRow interface

    public abstract IndexToHKey indexToHKey();

    public long tableBitmap()
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public void copyFrom(Key key, Value value)
    {
        super.copyFrom(key, value);
        constructHKeyFromIndexKey(hKeyCache.hKey(leafmostTable).key(), indexToHKey());
    }

    public void reset()
    {
        keyState.clear();
        super.reset();
    }

    // For use by subclasses

    protected PersistitIndexRow(StoreAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter);
        this.keyState = adapter.createKey();
        resetForWrite(indexRowType.index(), keyState);
        this.indexRowType = indexRowType;
        this.leafmostTable = index.leafMostTable();
        this.hKeyCache = new HKeyCache<>(adapter);

        this.types = index.types();
    }

    // For use by this class

    private PersistitKeyValueSource keyPSource(int i, TInstance type)
    {
        if (keyPSources == null)
            keyPSources = new PersistitKeyValueSource[nIndexFields];
        if (keyPSources[i] == null) {
            keyPSources[i] = new PersistitKeyValueSource(type);
        }
        return keyPSources[i];
    }

    // Object state

    protected final HKeyCache<HKey> hKeyCache;
    protected final Table leafmostTable;
    private final Key keyState;
    private final IndexRowType indexRowType;
    private final TInstance[] types;
    private PersistitKeyValueSource[] keyPSources;
}
