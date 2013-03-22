
package com.akiban.server.error;

public class ScanRetryAbandonedException extends InvalidOperationException {
    public ScanRetryAbandonedException (int retries) {
        super(ErrorCode.SCAN_RETRY_ABANDONDED, retries);
    }
}
