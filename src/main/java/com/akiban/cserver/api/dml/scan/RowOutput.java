package com.akiban.cserver.api.dml.scan;

public interface RowOutput {
    void output(NewRow row) throws RowOutputException;
}
