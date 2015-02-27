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

package com.foundationdb.blob;

import com.foundationdb.*;
import com.foundationdb.async.*;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple2;

public class SQLBlob extends BlobAsync {
    
    public SQLBlob(Subspace subspace){
        super (subspace);
    }

    public Future<Void> setLinkedTable(TransactionContext tcx, final int tableId) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.set(attributeKey(), Tuple2.from((long)tableId).pack());
                return new ReadyFuture<>((Void) null);
            }
        });
    }
    
    public Future<Integer> getLinkedTable(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Integer>>() {
            @Override
            public Future<Integer> apply(Transaction tr) {
                return tr.get(attributeKey()).map(new Function<byte[], Integer>() {
                    @Override
                    public Integer apply(byte[] tableIdBytes) {
                        if(tableIdBytes == null) {
                            return null;
                        }
                        int tableId = (int)Tuple2.fromBytes(tableIdBytes).getLong(0);
                        return Integer.valueOf(tableId);
                    }
                });
            }
        });
    }

    public Future<Boolean> isLinked(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Boolean>>() {
            @Override
            public Future<Boolean> apply(Transaction tr) {
                return tr.get(attributeKey()).map(new Function<byte[], Boolean>() {
                    @Override
                    public Boolean apply(byte[] tableIdBytes) {
                        if(tableIdBytes == null) {
                            return false;
                        }
                        return true;
                    }
                });
            }
        });
    }    
}
