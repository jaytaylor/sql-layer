package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class DuplicateColumnNameException extends DDLException {
    public DuplicateColumnNameException(InvalidOperationException e) {
        super(e);
    }
}
