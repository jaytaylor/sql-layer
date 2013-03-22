
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.util.GrowableByteBuffer;

public interface LegacyRowOutput {
    GrowableByteBuffer getOutputBuffer();

    /**
     * Signals that a row has been written into the buffer. It could be that the row actually caused the scan's
     * limit to be exceeded, and that it should actually be disregarded; if so, that is communicated via
     * limitExceeded
     * @param limitExceeded whether the row that was written was actually in excess of the limit
     */
    void wroteRow(boolean limitExceeded);

    void addRow(RowData rowData);

    int getRowsCount();

    boolean getOutputToMessage();

    /**
     * Marks a state for this output. Scans may have to be retried; the scan loop will first mark the LegacyRowOutput
     * and then {@link #rewind()} it before retrying, if necessary.
     */
    void mark();

    /**
     * Tells this LegacyRowOutput to go back to its previously marked state. If there was no previously marked
     * state, this behavior is undefined by the LegacyRowOutput interface.
     */
    void rewind();
}
