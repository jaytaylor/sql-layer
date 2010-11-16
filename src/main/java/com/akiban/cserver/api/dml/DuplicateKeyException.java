package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class DuplicateKeyException extends DMLException {
    public DuplicateKeyException(InvalidOperationException e) {
    super(e);
    }
}
