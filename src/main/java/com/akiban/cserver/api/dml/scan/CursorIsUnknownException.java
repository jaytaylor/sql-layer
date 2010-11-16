package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.dml.DMLException;

public final class CursorIsUnknownException extends DMLException {
    public CursorIsUnknownException(InvalidOperationException e) {
    super(e);
    }
}
