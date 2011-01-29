package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public final class RawByteOutputter implements HapiProcessor.Outputter{
    private static final RawByteOutputter instance = new RawByteOutputter();

    public static RawByteOutputter instance() {
        return instance;
    }

    private RawByteOutputter() {}

    @Override
    public void output(RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException {
        for(RowData data : rows) {
            outputStream.write(data.getBytes(), data.getRowStart(), data.getRowSize());
        }
    }
}
