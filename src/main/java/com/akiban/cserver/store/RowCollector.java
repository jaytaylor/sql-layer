package com.akiban.cserver.store;

import java.nio.ByteBuffer;

public interface RowCollector {

    public final int SCAN_FLAGS_DESCENDING = 1 << 0;

    public final int SCAN_FLAGS_START_EXCLUSIVE = 1 << 1;

    public final int SCAN_FLAGS_END_EXCLUSIVE = 1 << 2;

    public final int SCAN_FLAGS_SINGLE_ROW = 1 << 3;

    public final int SCAN_FLAGS_PREFIX = 1 << 4;

    public final int SCAN_FLAGS_START_AT_EDGE = 1 << 5;

    public final int SCAN_FLAGS_END_AT_EDGE = 1 << 6;

    public final int SCAN_FLAGS_DEEP = 1 << 7;

    public boolean collectNextRow(final ByteBuffer payload) throws Exception;

    public boolean hasMore() throws Exception;

    public void close();
    
    public int getDeliveredRows();

    public int getDeliveredBuffers();
    
    public int getRepeatedRows();
    
    public long getDeliveredBytes();
    
    public int getTableId();
    
    public long getId();

}
