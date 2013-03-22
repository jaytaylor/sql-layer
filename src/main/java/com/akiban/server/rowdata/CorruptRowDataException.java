
package com.akiban.server.rowdata;

public class CorruptRowDataException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CorruptRowDataException(final String msg) {
        super(msg);
    }
}
