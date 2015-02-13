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

package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.server.ServerStatement;

abstract class ExecutableStatement implements ServerStatement
{
    public abstract ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings);
    public abstract StatementTypes getStatementType();

    public JDBCResultSetMetaData getResultSetMetaData() {
        return null;
    }

    public JDBCParameterMetaData getParameterMetaData() {
        return null;
    }

    public long getEstimatedRowCount() {
        return -1;
    }

    public long getAISGeneration() {
        return 0;
    }
    

}
