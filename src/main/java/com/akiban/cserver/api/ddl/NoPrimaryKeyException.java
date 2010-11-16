package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class NoPrimaryKeyException extends DDLException {
    public NoPrimaryKeyException(InvalidOperationException e) {
    super(e);
    }
}
