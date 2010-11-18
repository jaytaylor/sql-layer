package com.akiban.cserver.api.dml;

import com.akiban.cserver.api.dml.scan.NiceRow;

import java.nio.ByteBuffer;

public interface RowOutput {

    ByteBuffer getOutputBuffer() throws RowOutputException;

    void wroteRow();

    int getRowsCount();
}
