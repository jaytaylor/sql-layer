package com.akiban.cserver.api.dml.scan;

/**
 * The state of a scan cursor.
 */
public enum CursorState {
    /**
     * Newly opened, hasn't started scanning yet.
     */
    FRESH,
    /**
     * At least one scan, but more rows may be available.
     */
    SCANNING,
    /**
     * Scanning is complete; subsequent scan requests will fail.
     */
    FINISHED,
    /**
     * The requested cursor is unknown or has been removed.
     */
    UNKNOWN_CURSOR
}
