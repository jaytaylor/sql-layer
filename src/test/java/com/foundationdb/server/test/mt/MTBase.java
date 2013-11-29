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

package com.foundationdb.server.test.mt;

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
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ServiceHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

// Extend ITBase for the miscellaneous Row/Operator test helpers
public abstract class MTBase extends ITBase implements ServiceHolder
{
    public MTBase() {
        super("MT");
    }

    //
    // MTBase
    //

    protected List<Row> runPlanTxn(final OperatorCreator creator) {
        return txnService().run(session(), new Callable<List<Row>>() {
            @Override
            public List<Row> call() throws Exception {
                Schema schema = SchemaCache.globalSchema(ais());
                Operator plan = creator.create(schema);
                StoreAdapter adapter = store().createAdapter(session(), schema);
                QueryContext context = new SimpleQueryContext(adapter);
                QueryBindings bindings = context.createBindings();
                List<Row> rows = new ArrayList<>();
                Cursor cursor = API.cursor(plan, context, bindings);
                cursor.openTopLevel();
                try {
                    Row row;
                    while((row = cursor.next()) != null) {
                        rows.add(row);
                    }
                } finally {
                    cursor.closeTopLevel();
                }
                return rows;
            }
        });
    }


    //
    // ServiceHolder
    //

    @Override
    public TransactionService getTransactionService() {
        return txnService();
    }

    @Override
    public SchemaManager getSchemaManager() {
        return serviceManager().getSchemaManager();
    }

    @Override
    public Store getStore() {
        return store();
    }

    @Override
    public Session createSession() {
        return createNewSession();
    }
}
