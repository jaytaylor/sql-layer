package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class GroupWithProtectedTableException extends DDLException {
    public GroupWithProtectedTableException(InvalidOperationException e) {
    super(e);
    }
}
