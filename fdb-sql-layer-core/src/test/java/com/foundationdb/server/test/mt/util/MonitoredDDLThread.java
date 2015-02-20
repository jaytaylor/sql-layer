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

import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.Collection;
import java.util.List;

public class MonitoredDDLThread extends MonitoredThread
{
    private final OnlineDDLMonitor onlineDDLMonitor;
    private final Collection<OnlineDDLMonitor.Stage> onlineStageMarks;
    private final String schema;
    private final String ddl;
    private final List<DataTypeDescriptor> descriptors;
    private final List<String> columnNames;
    private final OnlineCreateTableAsBase.TestSession server;

    public MonitoredDDLThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> threadStageMarks,
                              OnlineDDLMonitor onlineDDLMonitor,
                              Collection<OnlineDDLMonitor.Stage> onlineStageMarks,
                              String schema,
                              String ddl,
                              List<DataTypeDescriptor> descriptors,
                              List<String> columnNames,
                              OnlineCreateTableAsBase.TestSession server) {
        super(name, services, monitor, threadStageMarks);
        this.onlineDDLMonitor = new OnlineDDLMonitorShim(onlineDDLMonitor);
        this.onlineStageMarks = onlineStageMarks;
        this.schema = schema;
        this.ddl = ddl;
        this.descriptors = descriptors;
        this.columnNames = columnNames;
        this.server = server;
    }

    public MonitoredDDLThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> threadStageMarks,
                              OnlineDDLMonitor onlineDDLMonitor,
                              Collection<OnlineDDLMonitor.Stage> onlineStageMarks,
                              String schema,
                              String ddl) {
        this(name, services, monitor, threadStageMarks, onlineDDLMonitor, onlineStageMarks, schema, ddl, null, null, null);
    }

    //
    // MonitoredThread
    //

    @Override
    protected boolean doRetryOnRollback() {
        return true;
    }

    @Override
    protected void runInternal(Session session) {
        getServiceHolder().getDDLFunctions().setOnlineDDLMonitor(onlineDDLMonitor);
        try {
            SchemaFactory schemaFactory = new SchemaFactory(schema);
            if(server != null) {
                server.setSession(session);
                schemaFactory.ddl(getServiceHolder().getDDLFunctions(), session, descriptors, columnNames, server, ddl);
            } else {
                schemaFactory.ddl(getServiceHolder().getDDLFunctions(), session, ddl);
            }



        } finally {
            getServiceHolder().getDDLFunctions().setOnlineDDLMonitor(null);
        }
    }

    //
    // Internal
    //

    private class OnlineDDLMonitorShim implements OnlineDDLMonitor
    {
        private final OnlineDDLMonitor delegate;

        private OnlineDDLMonitorShim(OnlineDDLMonitor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void at(OnlineDDLMonitor.Stage stage) {
            delegate.at(stage);
            LOG.trace("at: {}", stage);
            if(onlineStageMarks.contains(stage)) {
                mark(stage.name());
            }
        }
    }
}
