package com.akiban.cserver.api;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public interface HapiProcessor {
    public interface Outputter {
        void output(HapiGetRequest request, RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException;
    }
	public void processRequest(Session session, HapiGetRequest request, Outputter outputter, OutputStream outputStream)
            throws HapiRequestException;
}
