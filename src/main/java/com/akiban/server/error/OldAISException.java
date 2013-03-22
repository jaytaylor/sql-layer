
package com.akiban.server.error;

public final class OldAISException extends InvalidOperationException {
    public OldAISException(int oldGeneration, int currentGeneration) {
        super(ErrorCode.STALE_AIS, oldGeneration, currentGeneration);
    }
}
