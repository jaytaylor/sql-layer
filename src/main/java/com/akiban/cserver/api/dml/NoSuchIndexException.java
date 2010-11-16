package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class NoSuchIndexException extends DMLException {
    public NoSuchIndexException(InvalidOperationException e) {
    super(e);
    }
}
