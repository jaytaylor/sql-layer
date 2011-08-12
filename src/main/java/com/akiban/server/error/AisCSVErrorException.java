package com.akiban.server.error;

public class AisCSVErrorException extends InvalidOperationException {
    public AisCSVErrorException (String source, String message) {
        super (ErrorCode.AIS_CSV_ERROR, source, message);
    }
}
