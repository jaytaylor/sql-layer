package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class ParseException extends DDLException {
    public ParseException(InvalidOperationException e) {
        super(e);
    }
}
