package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class NoSuchColumnException extends DMLException {
    public NoSuchColumnException(InvalidOperationException e) {
    super(e);
    }
}
