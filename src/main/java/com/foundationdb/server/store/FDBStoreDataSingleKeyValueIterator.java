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

import com.foundationdb.async.Future;
import com.foundationdb.qp.storeadapter.FDBAdapter;

/**
 * Substiture for {@link FDBStoreDataKeyValueIterator} that uses a single value future
 * and emits that {@link KeyValue} if present.
 */
public class FDBStoreDataSingleKeyValueIterator extends FDBStoreDataIterator
{
    private final byte[] key;
    private Future<byte[]> futureValue;
    private byte[] value;

    public FDBStoreDataSingleKeyValueIterator(FDBStoreData storeData,
                                              byte[] key,
                                              Future<byte[]> value) {
        super(storeData);
        this.key = key;
        this.futureValue = value;
    }

    @Override
    public boolean hasNext() {
        if (futureValue == null) {
            return false;
        }
        try {
            value = futureValue.get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        }
        futureValue = null;
        return (value != null);
    }

    @Override
    public Void next() {
        storeData.rawKey = key;
        storeData.rawValue = value;
        return null;
    }

    @Override
    public void close() {
        if (futureValue != null) {
            futureValue.dispose();
        }
    }
}
