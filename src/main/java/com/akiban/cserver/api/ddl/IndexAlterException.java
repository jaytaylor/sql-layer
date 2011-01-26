package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public class IndexAlterException extends DDLException {
    public IndexAlterException(InvalidOperationException e) {
        super(e);
    }

    public IndexAlterException(ErrorCode code, String message) {
        super(code, message);
    }
}
