package com.akiban.server.error;

public class SequenceStartInRangeException extends InvalidOperationException {
    public SequenceStartInRangeException () {
        super (ErrorCode.SEQUENCE_START_IN_RANGE);
    }

}
