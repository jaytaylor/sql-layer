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

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.storeadapter.NewRowBackedIndexRow;
import com.foundationdb.server.types.TInstance;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.persistit.Key;
import com.persistit.Value;

public abstract class IndexRow extends AbstractRow implements com.geophile.z.Record
{
    // Row interface

    @Override
    public IndexRowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // IndexRow interface

    public abstract <S> void append(S source, TInstance type);
    public abstract void append (EdgeValue value);

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    public abstract void resetForRead(Index index, Key key, Value value); 
    public abstract void resetForWrite(Index index, Key key);
    
    public abstract int compareTo(IndexRow startKey, int startBoundColumns,
            boolean[] ascending); 
    
    public abstract int compareTo(IndexRow thatKey, int startBoundColumns);

    public abstract void reset();
    
    public abstract boolean keyEmpty();
    
    // Group Index Row only - table bitmap stored in index value
    public abstract void tableBitmap(long bitmap);
    public abstract long tableBitmap(); 

    // com.geophile.z.Record interface

    @Override
    public final long z()
    {
        // z is set only for a query object. If it hasn't been set, then we have an index row
        // with a field containing a z-value.
        if (z == Space.Z_NULL) {
            z = this.uncheckedValue(zPosition()).getInt64();
        }
        return z;
    }

    @Override
    public final void z(long z)
    {
        this.z = z;
    }

    @Override
    public final void copyTo(Record record)
    {
        throw new UnsupportedOperationException();
    }

    // TODO: Remove these as we get rid of the Key use in the upper layers
    
    public abstract void copyPersistitKeyTo(Key key);
    public abstract void appendFieldTo(int position, Key target);
    public abstract void copyFrom(Key key, Value value);

    protected abstract int zPosition();

NewRowBackedIndexRow    private long z = Space.Z_NULL;

    public static enum EdgeValue {
        BEFORE,
        AFTER;
    }
}
