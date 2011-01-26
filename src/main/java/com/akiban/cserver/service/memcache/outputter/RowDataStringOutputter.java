package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class RowDataStringOutputter implements HapiProcessor.Outputter<byte[]> {
    private static final RowDataStringOutputter instance = new RowDataStringOutputter();

    public static RowDataStringOutputter instance() {
        return instance;
    }

    private RowDataStringOutputter() {}
    
    @Override
    public byte[] output(RowDefCache rowDefCache, List<RowData> rows, StringBuilder sb) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output);
        for (RowData data : rows) {
            String toString = data.toString(rowDefCache);
            writer.println(toString);
        }
        writer.flush();
        return output.toByteArray();
    }

    @Override
    public byte[] error(String message) {
        return message.getBytes();
    }
}
