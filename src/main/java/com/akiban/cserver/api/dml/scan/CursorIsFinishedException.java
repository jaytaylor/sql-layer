package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.dml.DMLException;

public final class CursorIsFinishedException extends DMLException {
    public CursorIsFinishedException(InvalidOperationException e) {
    super(e);
    }
}
