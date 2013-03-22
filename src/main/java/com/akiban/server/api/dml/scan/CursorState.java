
package com.akiban.server.api.dml.scan;

/**
 * The state of a scan cursor.
 */
public enum CursorState {
    /**
     * Newly opened, hasn't started scanning yet.
     */
    FRESH(true),
    /**
     * At least one scan, but more rows may be available.
     */
    SCANNING(true),
    /**
     * Scanning is complete; subsequent scan requests will fail.
     */
    FINISHED(false),
    /**
     * Scanning cannot continue because a field involved in the index has been modified.
     */
    CONCURRENT_MODIFICATION(false),
    /**
     * Scanning cannot continue because of a DDL that may have affected this scan.
     */
    DDL_MODIFICATION(false),
    /**
     * The requested cursor is unknown or has been removed.
     */
    UNKNOWN_CURSOR(false)
    ;

    private final boolean isOpenState;

    CursorState(boolean openState) {
        isOpenState = openState;
    }

    public boolean isOpenState() {
        return isOpenState;
    }
}
