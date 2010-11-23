package com.akiban.cserver.api.dml.scan;

import java.nio.ByteBuffer;

public interface LegacyRowOutput {
    ByteBuffer getOutputBuffer() throws RowOutputException;

    void wroteRow() throws RowOutputException;

    int getRowsCount();
}
