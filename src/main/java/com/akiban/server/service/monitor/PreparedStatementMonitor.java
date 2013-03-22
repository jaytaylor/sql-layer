
package com.akiban.server.service.monitor;

public interface PreparedStatementMonitor {
    /** The id of the session owning the prepared statement. */
    int getSessionId();

    /** The name of the statement, if any. */
    String getName();    

    /** The SQL of the statement. */
    String getSQL();    

    /** The time at which the statement was prepared. */
    long getPrepareTimeMillis();

    /** The estimated number of rows that will be returned. */
    int getEstimatedRowCount();

}
