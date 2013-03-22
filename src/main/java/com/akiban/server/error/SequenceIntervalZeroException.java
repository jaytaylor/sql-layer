package com.akiban.server.error;

public class SequenceIntervalZeroException extends InvalidOperationException {
    public SequenceIntervalZeroException () {
        super (ErrorCode.SEQUENCE_INTERVAL_ZERO);
    }

}
