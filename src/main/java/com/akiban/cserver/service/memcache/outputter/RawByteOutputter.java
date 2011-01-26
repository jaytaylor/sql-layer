package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.ByteArrayOutputStream;
import java.util.List;

public final class RawByteOutputter implements HapiProcessor.Outputter<byte[]> {
    private static final RawByteOutputter instance = new RawByteOutputter();

    public static RawByteOutputter instance() {
        return instance;
    }

    private RawByteOutputter() {}

    @Override
    public byte[] output(RowDefCache rowDefCache, List<RowData> rows) {
        final int initialSize = estimateIntializeBufferSize(rows);
        ByteArrayOutputStream output = new ByteArrayOutputStream(initialSize);
        for(RowData data : rows) {
            output.write(data.getBytes(), data.getBufferStart(), data.getBufferLength());
        }
        return output.toByteArray();
    }

    @Override
    public byte[] error(String message) {
        return message.getBytes();
    }

    private static int estimateIntializeBufferSize(List<RowData> rows) {
        return rows.size() == 0 ? 1024 : rows.get(0).getBytes().length;
    }
}
