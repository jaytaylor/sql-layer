
package com.akiban.sql.embedded;

import com.akiban.qp.operator.Operator;

abstract class ExecutableOperatorStatement extends ExecutableStatement
{
    protected Operator resultOperator;
    protected JDBCResultSetMetaData resultSetMetaData;
    protected JDBCParameterMetaData parameterMetaData;
    
    protected ExecutableOperatorStatement(Operator resultOperator,
                                          JDBCResultSetMetaData resultSetMetaData,
                                          JDBCParameterMetaData parameterMetaData) {
        this.resultOperator = resultOperator;
        this.resultSetMetaData = resultSetMetaData;
        this.parameterMetaData = parameterMetaData;
    }
    
    public Operator getResultOperator() {
        return resultOperator;
    }

    @Override
    public JDBCResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

}
