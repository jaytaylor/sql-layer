package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class UnsupportedReadException extends DMLException {
    public UnsupportedReadException(InvalidOperationException e) {
    super(e);
    }
}
