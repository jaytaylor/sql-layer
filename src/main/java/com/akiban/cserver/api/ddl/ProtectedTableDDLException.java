package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class ProtectedTableDDLException extends DDLException {
    public ProtectedTableDDLException(InvalidOperationException e) {
        super(e);
    }
}
