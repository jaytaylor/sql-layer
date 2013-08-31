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

import com.foundationdb.server.store.FDBTransactionService.TransactionState;

import com.foundationdb.ais.model.Index;
import com.foundationdb.server.error.DuplicateKeyException;

import com.foundationdb.async.Future;
import com.foundationdb.tuple.Tuple;
import com.persistit.Key;
import com.persistit.Persistit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FDBPendingUniquenessChecks
{
    private static final Logger LOG = LoggerFactory.getLogger(FDBPendingUniquenessChecks.class);
    private final Map<Index,Map<byte[],Future<byte[]>>> pending = new HashMap<>();

    public void add(TransactionState txn, Index index, byte[] key, Future<byte[]> future) {
        // Do this periodically just to keep the size of things down.
        checkUniqueness(txn, false);
        Map<byte[],Future<byte[]>> futures = pending.get(index);
        if (futures == null) {
            futures = new HashMap<>();
            pending.put(index, futures);
        }
        futures.put(key, future);
    }
    
    protected void checkUniqueness(TransactionState txn, boolean wait) {
        for (Map.Entry<Index,Map<byte[],Future<byte[]>>> pentry : pending.entrySet()) {
            Iterator<Map.Entry<byte[],Future<byte[]>>> iter = pentry.getValue().entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<byte[],Future<byte[]>> fentry = iter.next();
                Future<byte[]> future = fentry.getValue();
                if (!wait && !future.isDone()) {
                    continue;
                }
                long startNanos = System.nanoTime();
                future.blockUntilReady();
                long endNanos = System.nanoTime();
                txn.uniquenessTime += (endNanos - startNanos);
                if (future.get() != null) {
                    // Recover Index and Key for error message.
                    Index index = pentry.getKey();
                    Key key = new Key((Persistit)null);
                    byte[] keyBytes = Tuple.fromBytes(fentry.getKey()).getBytes(2);
                    key.setEncodedSize(keyBytes.length);
                    System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
                    throw new DuplicateKeyException(index.getIndexName().toString(), key);
                }
                iter.remove();
            }
        }
    }

    public void clear() {
        for (Map<byte[],Future<byte[]>> futures : pending.values()) {
            futures.clear();
        }
    }
}
