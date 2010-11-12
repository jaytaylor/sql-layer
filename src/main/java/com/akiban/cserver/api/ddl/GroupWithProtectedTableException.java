package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class GroupWithProtectedTableException extends DDLException {
    public GroupWithProtectedTableException(String message) {
        super(ErrorCode.JOIN_TO_PROTECTED_TABLE, message);
    }
}
