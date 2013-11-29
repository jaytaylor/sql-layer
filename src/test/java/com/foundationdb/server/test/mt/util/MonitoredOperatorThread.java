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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MonitoredOperatorThread extends Thread implements HasTimeMarker
{
    private static final Logger LOG = LoggerFactory.getLogger(MonitoredOperatorThread.class);

    private final ServiceHolder services;
    private final ThreadMonitor monitor;
    private final OperatorCreator operatorCreator;
    private final Set<Stage> stageMarks;

    private final List<Row> scannedRows;
    private final TimeMarker timeMarker;
    private Stage curStage = null;
    private Throwable failure = null;


    public MonitoredOperatorThread(String name,
                                   ServiceHolder services,
                                   OperatorCreator operatorCreator,
                                   ThreadMonitor monitor,
                                   Set<Stage> stageMarks) {
        super(name);
        this.services = services;
        this.operatorCreator = operatorCreator;
        this.monitor = monitor;
        this.stageMarks = stageMarks;
        this.scannedRows = new ArrayList<>();
        this.timeMarker = new TimeMarker();
    }

    //
    // MonitoredPlanRunable
    //

    public List<Row> getScannedRows() {
        return scannedRows;
    }

    public boolean hadFailure() {
        return (failure != null);
    }

    public Throwable getFailure() {
        return failure;
    }

    //
    // HasTimeMarker
    //

    @Override
    public TimeMarker getTimeMarker() {
        return timeMarker;
    }

    //
    // Runnable
    //

    @Override
    public void run() {
        to(Stage.START);
        try(Session session = services.createSession()) {
            runInternal(session);
        } catch(Throwable t) {
            wrap(t);
        } finally {
            to(Stage.FINISH);
        }
    }

    //
    // Internal
    //

    private void runInternal(Session session) {

        to(Stage.PRE_BEGIN);
        boolean success = false;
        services.getTransactionService().beginTransaction(session);
        to(Stage.POST_BEGIN);

        try {
            to(Stage.PRE_SCAN);
            Schema schema = SchemaCache.globalSchema(services.getSchemaManager().getAis(session));
            Operator plan = operatorCreator.create(schema);
            StoreAdapter adapter = services.getStore().createAdapter(session, schema);
            QueryContext context = new SimpleQueryContext(adapter);
            QueryBindings bindings = context.createBindings();
            Cursor cursor = API.cursor(plan, context, bindings);
            cursor.openTopLevel();
            try {
                Row row;
                while((row = cursor.next()) != null) {
                    scannedRows.add(row);
                    switch(scannedRows.size()) {
                        case 1: to(Stage.SCAN_FIRST_ROW); break;
                        case 2: to(Stage.SCAN_SECOND_ROW); break;
                    }
                }
            } finally {
                cursor.closeTopLevel();
            }
            to(Stage.POST_SCAN);

            to(Stage.PRE_COMMIT);
            services.getTransactionService().commitTransaction(session);
            success = true;
            to(Stage.POST_COMMIT);
        } finally {
            if(!success) {
                to(Stage.PRE_ROLLBACK);
                services.getTransactionService().rollbackTransaction(session);
                to(Stage.POST_ROLLBACK);
            }
        }
    }

    private void to(Stage stage) {
        try {
            monitor.at(stage);
            LOG.trace("to: {}", stage);
            if(stageMarks.contains(stage)) {
                timeMarker.mark(getName() + ":" + stage.name());
            }
            curStage = stage;
        } catch(Throwable t) {
            wrap(t);
        }
    }

    private void wrap(Throwable t) {
        LOG.debug("wrapping", t);
        assert (failure == null) : failure;
        failure = t;
        if(t instanceof Error) {
            throw (Error)t;
        }
        if(t instanceof RuntimeException) {
            throw (RuntimeException)t;
        }
        throw new RuntimeException(t);
    }
}
