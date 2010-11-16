package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class NoSuchRowException extends DMLException {
    public NoSuchRowException(InvalidOperationException e) {
    super(e);
    }
}
