/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.persistit.Key;
import com.persistit.Value;

import java.util.Iterator;
import java.util.Map.Entry;

public class MemoryStoreData
{
    public final Session session;
    public final MemoryStorageDescription storageDescription;
    public final Key persistitKey;
    public final Key endKey;
    public Value persistitValue;
    public byte[] rawKey;
    public byte[] rawValue;
    public Row row;
    public Iterator<Entry<byte[],byte[]>> iterator;

    public MemoryStoreData(Session session, MemoryStorageDescription storageDescription, Key persistitKey, Key endKey) {
        this.session = session;
        this.storageDescription = storageDescription;
        this.persistitKey = persistitKey;
        this.endKey = endKey;
    }

    public boolean next() {
        if(iterator.hasNext()) {
            Entry<byte[], byte[]> entry = iterator.next();
            rawKey = entry.getKey();
            rawValue = entry.getValue();
            return true;
        } else {
            return false;
        }
    }

    public void closeIterator() {
        if(iterator != null) {
            iterator = null;
        }
    }
}
