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
import com.foundationdb.server.service.session.Session;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class PersistitDeferredForeignKeys
{
    private Map<ForeignKey,Boolean> transactionDeferred = new HashMap<>();
    private Queue<DeferredForeignKey> statementQueue = new ArrayDeque<>();
    private Queue<DeferredForeignKey> transactionQueue = new ArrayDeque<>();

    public void setDeferredForeignKey(ForeignKey foreignKey, boolean deferred) {
        transactionDeferred = ForeignKey.setDeferred(transactionDeferred,
                                                     foreignKey, deferred);
    }
    
    public boolean isDeferred(ForeignKey foreignKey, ForeignKey.Action action) {
        return (action == ForeignKey.Action.NO_ACTION) ||
            foreignKey.isDeferred(transactionDeferred);
    }

    public void addDeferred(DeferredForeignKey entry, ForeignKey foreignKey) {
        if (foreignKey.isDeferred(transactionDeferred))
            transactionQueue.add(entry);
        else
            statementQueue.add(entry); // Deferred to statement end by NO_ACTION.
    }

    public void checkStatementForeignKeys(Session session) {
        runDeferred(session, statementQueue);
    }
    
    public void checkTransactionForeignKeys(Session session) {
        runDeferred(session, statementQueue);
        runDeferred(session, transactionQueue);
    }

    public static interface DeferredForeignKey {
        public void run(Session session);
    }

    protected void runDeferred(Session session, Queue<DeferredForeignKey> queue) {
        while (true) {
            DeferredForeignKey deferred = queue.poll();
            if (deferred == null) break;
            deferred.run(session);
        }
    }
}
