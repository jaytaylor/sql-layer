package com.akiban.cserver.service.session;

public interface SessionServiceMXBean {
    /** Total number of created sessions. This is <em>not</em> incremented atomically with token creation,
     * so it should be considered an estimate. However, it's safe to assume that this number monotonically increases.
     */
    int getSessionsCreated();

    /** Number of sessions  that have been closed. This is <em>not</em> incremented atomically with token closure,
     * so it should be considered an estimate. However, it's safe to assume that this number monotonically increases.
     */
    int getSessionsClosed();

    /** Total number of created sessions. This is <em>not</em> incremented atomically with token activation
     * or removal, so it should be considered an estimate.
     */
    int getSessionsActive();
}
