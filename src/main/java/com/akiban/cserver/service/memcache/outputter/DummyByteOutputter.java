package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.util.List;

public final class DummyByteOutputter implements HapiProcessor.Outputter<byte[]> {
    private static final DummyByteOutputter instance= new DummyByteOutputter();

    public static DummyByteOutputter instance() {
        return instance;
    }

    private DummyByteOutputter() {}

    @Override
    public byte[] output(RowDefCache rowDefCache, List<RowData> rows, StringBuilder sb) {
        return "DUMMY DATA".getBytes();
    }

    @Override
    public byte[] error(String message) {
        return message.getBytes();
    }
}
