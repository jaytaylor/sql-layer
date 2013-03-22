
package com.akiban.server.service.monitor;

public interface CursorMonitor {
    /** The id of the session owning the cursor. */
    int getSessionId();

    /** The name of the cursor, if any. */
    String getName();    

    /** The SQL of the cursor's statement. */
    String getSQL();    

    /** The name of the corresponding prepared statement, if any. */
    String getPreparedStatementName();    

    /** The time at which the cursor was opened. */
    long getCreationTimeMillis();

    /** The number of rows returned so far. */
    int getRowCount();

}
