package com.akiban.server.error;

public class SequenceMinGEMaxException extends InvalidOperationException  {
    public  SequenceMinGEMaxException () {
        super (ErrorCode.SEQUENCE_MIN_GE_MAX);
    }
}
