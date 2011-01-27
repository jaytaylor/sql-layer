package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class RowDataStringOutputter implements HapiProcessor.Outputter {
    private static final RowDataStringOutputter instance = new RowDataStringOutputter();

    public static RowDataStringOutputter instance() {
        return instance;
    }

    private RowDataStringOutputter() {}
    
    @Override
    public void output(RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException {
        PrintWriter writer = new PrintWriter(outputStream);
        for (RowData data : rows) {
            String toString = data.toString(rowDefCache);
            writer.println(toString);
        }
        writer.flush();
    }
}
