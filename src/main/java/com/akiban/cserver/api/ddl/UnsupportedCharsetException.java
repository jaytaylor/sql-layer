package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class UnsupportedCharsetException extends DDLException {
    public UnsupportedCharsetException(InvalidOperationException e) {
        super(e);
    }
}
