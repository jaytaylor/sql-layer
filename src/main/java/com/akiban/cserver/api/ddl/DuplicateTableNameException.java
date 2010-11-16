package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class DuplicateTableNameException extends DDLException {
    public DuplicateTableNameException(InvalidOperationException e) {
    super(e);
    }
}
