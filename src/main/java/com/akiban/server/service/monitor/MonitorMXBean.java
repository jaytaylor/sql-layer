
package com.akiban.server.service.monitor;

public interface MonitorMXBean
{
    /** Enable the query log. */
    void enableQueryLog();

    /** Disable the query log. */
    void disableQueryLog();

    /** Set the filename for the query log. Must be called before
     * {@link enableQueryLog}.
     */
    void setQueryLogFileName(String fileName);

    /** Get the current query log or <code>null</code> if not set. */
    String getQueryLogFileName();

    /** Set minimum time in milliseconds for a query to be logged or
     * <code>-1</code> for no limit. 
     */
    void setExecutionTimeThreshold(long threshold);

    /** Get minimum time in milliseconds for a query to be logged or
     * <code>-1</code> for no limit. 
     */
    long getExecutionTimeThreshold();
}
