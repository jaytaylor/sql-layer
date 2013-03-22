
package com.akiban.sql.server;

/**
 * An executable statement. Base only handles transactionality, not
 * actual execution and returning of values.
 */
public interface ServerStatement
{
    /** What transaction mode(s) does this statement use? */
    public enum TransactionMode { 
        ALLOWED,                // Does not matter.
        NONE,                   // Must not have a transaction; none created.
        NEW,                    // Must not have a transaction: read only created.
        NEW_WRITE,              // Must not have a transaction: read write created.
        READ,                   // New read only or existing allowed.
        WRITE,                  // New or existing read write allowed.
        REQUIRED,               // Must have transaction: read only okay.
        REQUIRED_WRITE,         // Must have read write transaction.
        WRITE_STEP_ISOLATED     // Existing write must use step isolation.
    };

    public enum TransactionAbortedMode {
        ALLOWED,                // Statement always allowed
        NOT_ALLOWED,            // Statement never allowed
    }

    public enum AISGenerationMode {
        ALLOWED,               // Statement can be used under any generation
        NOT_ALLOWED            // Statement can only be used under one generation
    }

    public TransactionMode getTransactionMode();
    public TransactionAbortedMode getTransactionAbortedMode();
    public AISGenerationMode getAISGenerationMode();
}
