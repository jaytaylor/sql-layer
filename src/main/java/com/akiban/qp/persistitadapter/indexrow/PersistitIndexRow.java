/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;
import com.persistit.Exchange;
import com.persistit.Key;

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
            if (Types3Switch.ON) {
                tInstances[i].format(pvalue(i), buffer);
            }
            else {
                Converters.convert(eval(i), buffer.asValueTarget());
            }
        }
        buffer.append(")->");
        buffer.append(hKey().toString());
        return buffer.toString();
    }
    
    // RowBase interface

    // TODO: This is not a correct implementation of hKey, because it returns an empty hKey to be filled in
    // TODO: by the caller. Normally, hKey returns the HKey of the row.
    @Override
    public final HKey hKey()
    {
        return hKeyCache.hKey(leafmostTable);
    }

    @Override
    public final RowType rowType()
    {
        return indexRowType;
    }

    @Override
    public final ValueSource eval(int i)
    {
        PersistitKeyValueSource keySource = keySource(i);
        attach(keySource, i, akTypes[i], akCollators[i]);
        return keySource;
    }

    @Override
    public final PValueSource pvalue(int i)
    {
        TInstance tInstance = tInstances[i];
        PersistitKeyPValueSource keySource = keyPSource(i, tInstance);
        attach(keySource, i, tInstance);
        return keySource;
    }

    // PersistitIndexRow interface

    public abstract IndexToHKey indexToHKey();

    public long tableBitmap()
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    public void copyFromExchange(Exchange exchange)
    {
        copyFrom(exchange);
        constructHKeyFromIndexKey(hKeyCache.hKey(leafmostTable).key(), indexToHKey());
    }

    public void reset()
    {
        keyState.clear();
        super.reset();
    }

    // For use by subclasses

    protected PersistitIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter.getUnderlyingStore());
        this.keyState = adapter.persistit().createKey();
        resetForWrite(indexRowType.index(), keyState);
        this.indexRowType = indexRowType;
        this.leafmostTable = (UserTable) index.leafMostTable();
        this.hKeyCache = new HKeyCache<>(adapter);
        if (Types3Switch.ON) {
            this.tInstances = index.tInstances();
            this.akTypes = null;
            this.akCollators = null;
        }
        else {
            this.akTypes = index.akTypes();
            this.akCollators = index.akCollators();
            this.tInstances = null;
        }
    }

    // For use by this class

    private PersistitKeyValueSource keySource(int i)
    {
        if (keySources == null)
            keySources = new PersistitKeyValueSource[nIndexFields];
        if (keySources[i] == null) {
            keySources[i] = new PersistitKeyValueSource();
        }
        return keySources[i];
    }

    private PersistitKeyPValueSource keyPSource(int i, TInstance tInstance)
    {
        if (keyPSources == null)
            keyPSources = new PersistitKeyPValueSource[nIndexFields];
        if (keyPSources[i] == null) {
            keyPSources[i] = new PersistitKeyPValueSource(tInstance);
        }
        return keyPSources[i];
    }

    // Object state

    protected final HKeyCache<PersistitHKey> hKeyCache;
    protected final UserTable leafmostTable;
    private final Key keyState;
    private final IndexRowType indexRowType;
    private final AkType[] akTypes;
    private final AkCollator[] akCollators;
    private final TInstance[] tInstances;
    private PersistitKeyValueSource[] keySources;
    private PersistitKeyPValueSource[] keyPSources;
}
