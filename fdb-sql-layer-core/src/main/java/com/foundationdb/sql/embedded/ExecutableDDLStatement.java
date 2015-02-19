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
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.parser.DDLStatementNode;

class ExecutableDDLStatement extends ExecutableStatement
{
    private DDLStatementNode ddl;
    private String sql;

    protected ExecutableDDLStatement(DDLStatementNode ddl, String sql) {
        this.ddl = ddl;
        this.sql = sql;
    }

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        AISDDL.execute(ddl, sql, context);
        return new ExecuteResults();
    }

    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.DDL_STMT;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.IMPLICIT_COMMIT;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

}
