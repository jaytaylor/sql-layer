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

package com.foundationdb.server.store;

import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.qp.storeadapter.FDBAdapter;

/**
 * Only takes care of the immediate copy of the key-value pair
 * into the <code>raw</code> fields.  Caller must deal with these
 * further, using {@link #unpackKey} and {@link #unpackValue} or
 * {@link #expandRowData}.
 */
public class FDBStoreDataKeyValueIterator extends FDBStoreDataIterator
{
    private final AsyncIterator<KeyValue> underlying;

    public FDBStoreDataKeyValueIterator(FDBStoreData storeData,
                                        AsyncIterator<KeyValue> underlying) {
        super(storeData);
        this.underlying = underlying;
    }

    @Override
    public boolean hasNext() {
        try {
            return underlying.hasNext();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        }
    }

    @Override
    public Void next() {
        try {
            KeyValue kv = underlying.next();
            storeData.rawKey = kv.getKey();
            storeData.rawValue = kv.getValue();
            return null;
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        }
    }

    @Override
    public void close() {
        underlying.dispose();
    }
}
