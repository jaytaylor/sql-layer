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

import com.fasterxml.jackson.databind.deser.std.NullifyingDeserializer;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.persistit.Key;
import com.persistit.Value;

/**
 * State used for traversing / modifying FDB storage.
 */
public class FDBStoreData {
    public final FDBStorageDescription storageDescription;
    public final Key persistitKey;
    public final Session session;
    public byte[] rawKey;
    public byte[] rawValue;
    public Value persistitValue;
    public Object otherValue;
    public FDBStoreDataIterator iterator;
    public enum NudgeDir {LEFT, RIGHT_NO_STRINC, RIGHT_STRINC, DEEPER };
    public NudgeDir nudged;
    
    public FDBStoreData(Session session, FDBStorageDescription storageDescription, Key persistitKey) {
        this.storageDescription = storageDescription;
        this.persistitKey = persistitKey;
        this.session = session;
    }

    public boolean next() {
        if (iterator.hasNext()) {
            iterator.next();
            return true;
        }
        else {
            return false;
        }
    }

    public void closeIterator() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }
}
