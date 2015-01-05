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

import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.OnlineCreateTableAsMT;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.server.ServerSessionBase;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.List;

public interface ConcurrentTestBuilder
{
    ConcurrentTestBuilder add(String name, OperatorCreator creator);
    ConcurrentTestBuilder mark(ThreadMonitor.Stage... stages);
    ConcurrentTestBuilder sync(String name, ThreadMonitor.Stage stage);
    ConcurrentTestBuilder rollbackRetry(boolean doRetry);
    ConcurrentTestBuilder sync(String testName, String syncName, ThreadMonitor.Stage stage);

    ConcurrentTestBuilder add(String name, String schema, String ddl);
    ConcurrentTestBuilder mark(OnlineDDLMonitor.Stage... stages);
    ConcurrentTestBuilder sync(String name, OnlineDDLMonitor.Stage stage);
    ConcurrentTestBuilder sync(String testName, String syncName, OnlineDDLMonitor.Stage stage);

    List<MonitoredThread> build(ServiceHolder serviceHolder);
    List<MonitoredThread> build(ServiceHolder serviceHolder, List<DataTypeDescriptor> descriptors,
                                List<String> columnNames, OnlineCreateTableAsBase.TestSession server);
}
