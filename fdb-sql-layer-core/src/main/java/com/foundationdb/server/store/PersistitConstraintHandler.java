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

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.ForeignKey.Action;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.KeyState;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class PersistitConstraintHandler extends ConstraintHandler<PersistitStore,Exchange,PersistitStorageDescription>
{
    private final PersistitTransactionService txnService;

    public PersistitConstraintHandler(PersistitStore store, ConfigurationService config, TypesRegistryService typesRegistryService, ServiceManager serviceManager, PersistitTransactionService txnService) {
        super(store, config, typesRegistryService, serviceManager);
        this.txnService = txnService;
    }

    @Override 
    protected void checkReferencing(Session session, Index index, Exchange exchange,
                                    Row row, ForeignKey foreignKey, String operation) {
        checkReferencing(session, index, exchange, row, foreignKey, operation, false);
    }
    
    protected void checkReferencing(Session session, Index index, Exchange exchange,
                                    Row row, ForeignKey foreignKey, String operation,
                                    boolean recheck) {
        assert index.isUnique() : index;
        boolean notReferencing = false;
        try {
            notReferencing = !entryExists(index, exchange);
            // Avoid write skew from concurrent insert referencing and delete referenced.
            exchange.lock();
        }
        catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
        if (notReferencing) {
            PersistitDeferredForeignKeys deferred = 
                txnService.getDeferredForeignKeys(session, true);
            if (!recheck && deferred.isDeferred(foreignKey, null)) {
                deferred.addDeferred(new DeferredReferencingCheck(index, exchange, row, foreignKey, operation), foreignKey);
            }
            else {
                notReferencing(session, index, exchange, row, foreignKey, operation);
            }
        }
    }
        
    @Override
    protected void checkNotReferenced(Session session, Index index,
            Exchange exchange, Row row, ForeignKey foreignKey,
            boolean selfReference, Action action, String operation) {
        checkNotReferenced(session, index, exchange, row, foreignKey, selfReference, action, operation, false);
    }

    protected void checkNotReferenced(Session session, Index index, Exchange exchange,
                                        Row row, ForeignKey foreignKey,
                                        boolean selfReference, ForeignKey.Action action, String operation,
                                        boolean recheck) {
        boolean stillReferenced = false;
        int depth = exchange.getKey().getDepth();
        try {
            if (row == null) {
                // Scan all (after null), filling exchange for error report.
                while (exchange.traverse(Key.Direction.GT, true)) {
                    if (!keyHasNullSegments(exchange.getKey(), index)) {
                        stillReferenced = true;
                        break;
                    }
                }
            }
            else {
                if (selfReference) {
                    stillReferenced = entryExistsSkipSelf(index, exchange);
                } 
                else {
                    stillReferenced = entryExists(index, exchange);
                }
            }
        }
        catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
        if (stillReferenced) {
            PersistitDeferredForeignKeys deferred = 
                txnService.getDeferredForeignKeys(session, true);
            if (!recheck && deferred.isDeferred(foreignKey, action)) {
                deferred.addDeferred(new DeferredNotReferencedCheck(index, exchange, depth, row, foreignKey, action, operation), foreignKey);
            }
            else {
                stillReferenced(session, index, exchange, row, foreignKey, operation);
            }
        }
    }

    private static boolean entryExists(Index index, Exchange exchange) throws PersistitException {
        // Normal case, reference does not contain all columns
        if (exchange.getKey().getDepth() < index.getAllColumns().size()) {
            return exchange.hasChildren();
        }
        // Exactly matches index, including HKey columns
        return exchange.traverse(Key.Direction.EQ, false, -1);
    }

    /*
     * The self reference check here is a table with a FK which references the same table
     * and the row we're looking at (to delete), references itself. e.g. pk = 1, fk = 1 
     * In this case it's ok to delete the row, but we want to check if there are any other
     * FK references to this table. e.g. pk = 3, fk = 1. In the self reference case we 
     * know there will be one entry in the table, and we want to check if there is more 
     * than one. This does not need to check the contents of the keys, only their count.
     */
    private static boolean entryExistsSkipSelf(Index index, Exchange exchange) throws PersistitException {
        
        boolean status = false; 
        if (exchange.getKey().getDepth() < index.getAllColumns().size() &&
                exchange.hasChildren()) {
            KeyFilter kf = new KeyFilter(exchange.getKey());
            status = exchange.next(kf);
            status = status && exchange.next(kf);
        } else {
            status = exchange.traverse(Key.Direction.EQ, false, -1);
        }
        return status;
    }

    class DeferredReferencingCheck implements PersistitDeferredForeignKeys.DeferredForeignKey {
        private final Index index;
        private final KeyState key;
        private final Row row;
        private final ForeignKey foreignKey;
        private final String operation;

        public DeferredReferencingCheck(Index index, Exchange exchange,
                                        Row row, ForeignKey foreignKey,
                                        String operation) {
            this.index = index;
            this.key = new KeyState(exchange.getKey());
            this.foreignKey = foreignKey;
            this.operation = operation;
            this.row = row;
        }
        
        @Override
        public void run(Session session) {
            Exchange exchange = store.createStoreData(session, index);
            key.copyTo(exchange.getKey());
            try {
                checkReferencing(session, index, exchange, row, foreignKey, operation, true);
            }
            finally {
                store.releaseStoreData(session, exchange);
            }
        }
    }

    class DeferredNotReferencedCheck implements PersistitDeferredForeignKeys.DeferredForeignKey {
        private final Index index;
        private final KeyState key;
        private final Row row;
        private final ForeignKey foreignKey;
        private final ForeignKey.Action action;
        private final String operation;

        public DeferredNotReferencedCheck(Index index, Exchange exchange, int depth,
                Row row, ForeignKey foreignKey,
                ForeignKey.Action action, String operation) {
            this.index = index;
            this.key = copyKey(exchange.getKey(), depth, row);
            this.row = row;
            this.foreignKey = foreignKey;
            this.action = action;
            this.operation = operation;
        }

        @Override
        public void run(Session session) {
            Exchange exchange = store.createStoreData(session, index);
            key.copyTo(exchange.getKey());
            try {
                checkNotReferenced(session, index, exchange, row, foreignKey, false, action, operation, true);
            }
            finally {
                store.releaseStoreData(session, exchange);
            }
        }
    }

    protected static KeyState copyKey(Key key, int depth, Row row) {
        if (row == null) {
            key.clear();
            key.append(null);
        }
        else {
            key.setDepth(depth);
        }
        return new KeyState(key);            
    }
}
