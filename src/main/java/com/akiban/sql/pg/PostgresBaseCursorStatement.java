
package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;

import java.io.IOException;

/**
 * Common handling for cursor-related statements.
 */
public abstract class PostgresBaseCursorStatement implements PostgresStatement
{
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public boolean hasAISGeneration() {
        return false;
    }

    @Override
    public void setAISGeneration(long generation) {
    }

    @Override
    public long getAISGeneration() {
        return 0;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return null;
    }

}
