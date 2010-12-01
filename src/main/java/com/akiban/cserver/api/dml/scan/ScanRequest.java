package com.akiban.cserver.api.dml.scan;

public interface ScanRequest extends ScanRange {
    int getIndexId();
    int getScanFlags();
}
