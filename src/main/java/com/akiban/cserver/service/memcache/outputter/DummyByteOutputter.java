package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public final class DummyByteOutputter implements HapiProcessor.Outputter {
    private static final DummyByteOutputter instance= new DummyByteOutputter();

    public static DummyByteOutputter instance() {
        return instance;
    }

    private DummyByteOutputter() {}

    @Override
    public void output(HapiGetRequest request, RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException {
        outputStream.write("DUMMY DATA".getBytes());
    }
}
