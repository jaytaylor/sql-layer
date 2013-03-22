
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;

/**
 * A ScanLimit that represents no limit. This is package private because nobody should ever instantiate it directly;
 * instead, they should grab the singleton reference {@link ScanLimit#NONE}
 */
final class NoScanLimit implements ScanLimit {
    @Override
    public String toString()
    {
        return "none";
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        return false;
    }
}
