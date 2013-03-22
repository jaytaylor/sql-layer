
package com.akiban.sql.embedded;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.sql.optimizer.plan.CostEstimate;

class ExecutableQueryOperatorStatement extends ExecutableOperatorStatement
{
    private CostEstimate costEstimate;

    protected ExecutableQueryOperatorStatement(Operator resultOperator,
                                               JDBCResultSetMetaData resultSetMetaData, 
                                               JDBCParameterMetaData parameterMetaData,
                                               CostEstimate costEstimate) {
        super(resultOperator, resultSetMetaData, parameterMetaData);
        this.costEstimate = costEstimate;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        Cursor cursor = null;
        try {
            cursor = API.cursor(resultOperator, context);
            cursor.open();
            ExecuteResults result = new ExecuteResults(cursor);
            cursor = null;
            return result;
        }
        finally {
            if (cursor != null) {
                cursor.destroy();
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
