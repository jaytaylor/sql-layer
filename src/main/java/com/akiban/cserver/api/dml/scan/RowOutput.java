package com.akiban.cserver.api.dml.scan;

public interface RowOutput {
    void output(NiceRow row) throws RowOutputException;
}
