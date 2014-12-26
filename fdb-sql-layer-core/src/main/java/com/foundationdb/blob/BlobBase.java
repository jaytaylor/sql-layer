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

package com.foundationdb.blob;

import com.foundationdb.*;
import com.foundationdb.async.*;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.*;

public class BlobBase extends BlobAsync {
    
    public BlobBase(Subspace subspace){
        super (subspace);
    }

    public Future<Void> setLinkedSchema(TransactionContext tcx, final String schemaName) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.set(attributeKey(), Tuple2.from(schemaName).pack());
                return new ReadyFuture<>((Void) null);
            }
        });
        
    }
    
    public Future<String> getLinkedSchema(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<String>>() {
            @Override
            public Future<String> apply(Transaction tr) {
                return tr.get(attributeKey()).map(new Function<byte[],String>() {
                    @Override
                    public String apply(byte[] schemaNameBytes) {
                        if(schemaNameBytes == null) {
                            return "";
                        }
                        return Tuple2.fromBytes(schemaNameBytes).getString(0);
                    }
                });
            }
        });
    }
}
