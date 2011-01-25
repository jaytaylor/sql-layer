package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.util.List;

public final class JsonByteOutputter implements HapiProcessor.Outputter<byte[]> {
    private static final JsonByteOutputter instance = new JsonByteOutputter();

    public static JsonByteOutputter instance() {
        return instance;
    }

    private JsonByteOutputter() {}

    @Override
    public byte[] error(String message) {
        return message.getBytes();
    }

    @Override
    public byte[] output(RowDefCache rowDefCache, List<RowData> rows) {
        return JsonOutputter.instance().output(rowDefCache, rows).getBytes();
    }
}
