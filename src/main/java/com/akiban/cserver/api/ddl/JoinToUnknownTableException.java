package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class JoinToUnknownTableException extends DDLException {
    public JoinToUnknownTableException(InvalidOperationException e) {
    super(e);
    }
}
