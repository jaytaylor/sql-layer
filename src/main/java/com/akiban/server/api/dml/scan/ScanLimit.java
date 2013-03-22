
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;

public interface ScanLimit {

    /**
     * A singleton that represents no scan limit.
     */
    public static final ScanLimit NONE = new NoScanLimit();

    /**
     * Whether the limit has been reached; a {@code false} value indicates that the scan should continue. This method
     * is invoked directly after the row is collected, and before it's outputted; if this method returns {@code false},
     * the method will not be outputted
     *
     * @param row the row that has just been collected
     * @return whether scanning should stop
     */
    boolean limitReached(RowData row);

}
