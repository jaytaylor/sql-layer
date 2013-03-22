
package com.akiban.server.api.dml.scan;

public interface ScanRequest extends ScanRange {
    int getIndexId();
    int getScanFlags();
    ScanLimit getScanLimit();
    void dropScanLimit();
}
