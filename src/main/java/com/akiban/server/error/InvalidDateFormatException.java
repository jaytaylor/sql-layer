
package com.akiban.server.error;

public class InvalidDateFormatException extends InvalidOperationException {
    public InvalidDateFormatException (String type, String date) {
        super (ErrorCode.INVALID_DATE_FORMAT, type, date);
    }
}
