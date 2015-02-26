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

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.ForeignKey.Action;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.foundationdb.server.types.service.TypesRegistryService;

public class MemoryConstraintHandler extends ConstraintHandler<MemoryStore,MemoryStoreData,MemoryStorageDescription>
{
    private final MemoryTransactionService txnService;

    protected MemoryConstraintHandler(MemoryStore store,
                                      MemoryTransactionService txnService,
                                      ConfigurationService config,
                                      TypesRegistryService typesRegistryService,
                                      ServiceManager serviceManager) {
        super(store, config, typesRegistryService, serviceManager);
        this.txnService = txnService;
    }

    @Override
    protected void checkReferencing(Session session,
                                    Index index,
                                    MemoryStoreData storeData,
                                    Row row,
                                    ForeignKey foreignKey,
                                    String operation) {
        assert index.isUnique() : index;
        MemoryIndexChecks.CheckPass finalPass =
            txnService.isDeferred(session, foreignKey) ?
                MemoryIndexChecks.CheckPass.TRANSACTION :
                MemoryIndexChecks.CheckPass.ROW;
        MemoryIndexChecks.IndexCheck check =
            MemoryIndexChecks.foreignKeyReferencingCheck(storeData, index, foreignKey, finalPass, operation);
        txnService.addPendingCheck(session, check);
    }

    @Override
    protected void checkNotReferenced(Session session,
                                      Index index,
                                      MemoryStoreData storeData,
                                      Row row,
                                      ForeignKey foreignKey,
                                      boolean selfReference,
                                      Action action,
                                      String operation) {
        MemoryIndexChecks.CheckPass finalPass;
        if(action == ForeignKey.Action.RESTRICT) {
            finalPass = MemoryIndexChecks.CheckPass.ROW;
        } else if(txnService.isDeferred(session, foreignKey)) {
            finalPass = MemoryIndexChecks.CheckPass.TRANSACTION;
        } else {
            finalPass = MemoryIndexChecks.CheckPass.STATEMENT;
        }
        MemoryIndexChecks.IndexCheck check =
            MemoryIndexChecks.foreignKeyNotReferencedCheck(storeData,
                                                           index,
                                                           (row == null),
                                                           foreignKey,
                                                           selfReference,
                                                           finalPass,
                                                           operation);
        txnService.addPendingCheck(session, check);
    }
}
