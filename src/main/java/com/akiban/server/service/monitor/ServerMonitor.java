
package com.akiban.server.service.monitor;

public interface ServerMonitor {
    /** What kind of server is this? */
    String getServerType();

    /** The port in use, or <code>-1</code> if not listening or not applicable. */
    int getLocalPort();

    /** The system time at which this server started. */
    long getStartTimeMillis();
    
    /** The total number of sessions services, including those no longer active. */
    int getSessionCount();
}
