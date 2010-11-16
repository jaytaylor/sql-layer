package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class ParseException extends DDLException {
    ParseException(InvalidOperationException e) {
        super(e);
    }
}
