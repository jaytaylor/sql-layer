package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class UnsupportedModificationException extends DMLException {
    public UnsupportedModificationException(InvalidOperationException e) {
    super(e);
    }
}
