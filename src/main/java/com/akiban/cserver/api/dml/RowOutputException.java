package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class RowOutputException extends DMLException {
    public RowOutputException(InvalidOperationException e) {
    super(e);
    }
}
