
package com.akiban.server.error;

public class ExternalRowReaderException extends InvalidOperationException {
    public ExternalRowReaderException(String message) {
        super (ErrorCode.EXTERNAL_ROW_READER_EXCEPTION, message);
    }
}
