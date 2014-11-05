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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.sql.optimizer.plan.CostEstimate;

class ExecutableQueryOperatorStatement extends ExecutableOperatorStatement
{
    private CostEstimate costEstimate;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutableQueryOperatorStatement.class);
    
    protected ExecutableQueryOperatorStatement(Schema schema,
                                               Operator resultOperator,
                                               JDBCResultSetMetaData resultSetMetaData, 
                                               JDBCParameterMetaData parameterMetaData,
                                               CostEstimate costEstimate) {
        super(schema, resultOperator, resultSetMetaData, parameterMetaData);
        this.costEstimate = costEstimate;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        Cursor cursor = null;
        try {
            context.initStore(getSchema());
            cursor = API.cursor(resultOperator, context, bindings);
            cursor.openTopLevel();
            ExecuteResults result = new ExecuteResults(cursor);
            cursor = null;
            return result;
        } catch (RuntimeException e) {
            LOG.error("caught error: {}", e.toString());
            cursor = null;
            throw e;
        }       
        finally {
            if (cursor != null) {
                cursor.closeTopLevel();
            }
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public long getEstimatedRowCount() {
        if (costEstimate == null)
            return -1;
        else
            return costEstimate.getRowCount();
    }

}
