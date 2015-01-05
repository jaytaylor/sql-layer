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

package com.foundationdb.server.test.mt.util;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MonitoredOperatorThread extends MonitoredThread
{
    private final OperatorCreator operatorCreator;
    private final boolean retryOnRollback;


    public MonitoredOperatorThread(String name,
                                   ServiceHolder services,
                                   OperatorCreator operatorCreator,
                                   ThreadMonitor monitor,
                                   Set<Stage> stageMarks,
                                   boolean retryOnRollback) {
        super(name, services, monitor, stageMarks);
        this.operatorCreator = operatorCreator;
        this.retryOnRollback = retryOnRollback;
    }

    //
    // MonitoredThread
    //

    @Override
    protected boolean doRetryOnRollback() {
        return retryOnRollback;
    }

    @Override
    protected boolean doRetryOnTableVersionChange() {
        return false;
    }

    @Override
    protected void runInternal(Session session) {
        to(Stage.PRE_BEGIN);
        boolean success = false;
        getServiceHolder().getTransactionService().beginTransaction(session);
        // To ensure our desired ordering
        getServiceHolder().getTransactionService().getTransactionStartTimestamp(session);
        to(Stage.POST_BEGIN);
        try {
            while(!success) {
                to(Stage.PRE_SCAN);
                Schema schema = SchemaCache.globalSchema(getServiceHolder().getSchemaManager().getAis(session));
                Operator plan = operatorCreator.create(schema);
                StoreAdapter adapter = getServiceHolder().getStore().createAdapter(session);
                QueryContext context = new SimpleQueryContext(adapter);
                QueryBindings bindings = context.createBindings();
                Cursor cursor = API.cursor(plan, context, bindings);
                cursor.openTopLevel();
                try {
                    Row row;
                    while((row = cursor.next()) != null) {
                        getScannedRows().add(row);
                        switch(getScannedRows().size()) {
                            case 1: to(Stage.SCAN_FIRST_ROW); break;
                            case 2: to(Stage.SCAN_SECOND_ROW); break;
                        }
                    }
                } finally {
                    cursor.closeTopLevel();
                }
                to(Stage.POST_SCAN);

                to(Stage.PRE_COMMIT);
                getServiceHolder().getTransactionService().commitTransaction(session);
                success = true;
                to(Stage.POST_COMMIT);
            }
        } finally {
            if(!success) {
                to(Stage.PRE_ROLLBACK);
                getServiceHolder().getTransactionService().rollbackTransactionIfOpen(session);
                to(Stage.POST_ROLLBACK);
            }
        }
    }
}
