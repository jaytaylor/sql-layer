package com.akiban.server.error;

import com.persistit.exception.PersistitException;

public class PersistItErrorException extends InvalidOperationException {
    public PersistItErrorException (PersistitException ex) {
        super(ErrorCode.PERSISTIT_ERROR, ex.getMessage());
    }
}
