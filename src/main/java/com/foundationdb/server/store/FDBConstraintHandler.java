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
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.expressions.TypesRegistryService;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.format.FDBStorageDescription;

import static com.foundationdb.server.store.FDBStoreDataHelper.*;

public class FDBConstraintHandler extends ConstraintHandler<FDBStore,FDBStoreData,FDBStorageDescription>
{
    private final FDBTransactionService txnService;

    public FDBConstraintHandler(FDBStore store, ConfigurationService config, TypesRegistryService typesRegistryService, ServiceManager serviceManager, FDBTransactionService txnService) {
        super(store, config, typesRegistryService, serviceManager);
        this.txnService = txnService;
    }

    @Override
    protected void checkReferencing(Session session, Index index, FDBStoreData storeData,
                                    RowData row, ForeignKey foreignKey, String action) {
        // At present, a unique index has the rest of the index entry
        // in the value, so the passed in key will match exactly.
        assert index.isUnique() : index;
        TransactionState txn = txnService.getTransaction(session);
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.foreignKeyReferencingCheck(session, txn, index, storeData.persistitKey,
                                                             foreignKey, action);
        if (txn.getIndexChecks() == null) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                notReferencing(session, index, storeData, row, foreignKey, action);
            }
        }
        else {
            txn.getIndexChecks().add(session, txn, index, check);
        }
    }

    @Override
    protected void checkNotReferenced(Session session, Index index, FDBStoreData storeData,
                                      RowData row, ForeignKey foreignKey, String action) {
        TransactionState txn = txnService.getTransaction(session);
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.foreignKeyNotReferencedCheck(session, txn, index, storeData.persistitKey, (row == null),
                                                               foreignKey, action);
        if (txn.getIndexChecks() == null) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                if (row == null) {
                    // Need actual key found for error message.
                    FDBStoreDataHelper.unpackTuple(index, storeData.persistitKey, check.getRawKey());
                }
                stillReferenced(session, index, storeData, row, foreignKey, action);
            }
        }
        else {
            txn.getIndexChecks().add(session, txn, index, check);
        }
    }

}
