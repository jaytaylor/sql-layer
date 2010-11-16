package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class JoinToWrongColumnsException extends DDLException {
    public JoinToWrongColumnsException(InvalidOperationException e) {
    super(e);
    }
}
