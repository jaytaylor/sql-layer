package com.akiban.server.error;

public class AISTooLargeException extends InvalidOperationException {
    public AISTooLargeException (int size, int maxSize) {
        super(ErrorCode.AIS_TOO_LARGE, size, maxSize);
    }
}
