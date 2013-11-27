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

package com.foundationdb.server.store.format.columnkeys;

import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataIterator;

import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class ColumnKeysStorageIterator extends FDBStoreDataIterator
{
    private final AsyncIterator<KeyValue> underlying;
    private final int limit;
    private KeyValue pending;
    private int count;

    public ColumnKeysStorageIterator(FDBStoreData storeData, 
                                     AsyncIterator<KeyValue> underlying,
                                     int limit) {
        super(storeData);
        this.underlying = underlying;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return (((pending != null) || underlying.hasNext()) &&
                ((limit <= 0) || (count < limit)));
    }

    @Override
    public Void next() {
        Map<String,Object> value = new HashMap<>();
        Tuple lastKey = null;
        while (true) {
            KeyValue kv;
            if (pending != null) {
                kv = pending;
                pending = null;
            }
            else if (underlying.hasNext()) {
                kv = underlying.next();
            }
            else {
                break;
            }
            Tuple key = Tuple.fromBytes(kv.getKey());
            String name = key.getString(key.size() - 1);
            key = key.popBack();
            if (lastKey == null) {
                lastKey = key;
            }
            else if (!tupleEquals(key, lastKey)) {
                pending = kv;
                break;
            }
            value.put(name, Tuple.fromBytes(kv.getValue()).get(0));
        }
        storeData.rawKey = lastKey.pack();
        storeData.otherValue = value;
        count++;
        return null;
    }

    // TODO: Until fixed version of ByteArrayUtil.compareUnsigned is released.
    public boolean tupleEquals(Tuple t1, Tuple t2) {
        return java.util.Arrays.equals(t1.pack(), t2.pack());
    }

    @Override
    public void close() {
        underlying.dispose();
    }
}
