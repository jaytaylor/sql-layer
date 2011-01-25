package com.akiban.cserver.api;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.service.session.Session;

import java.nio.ByteBuffer;
import java.util.List;

public interface HapiProcessor {
    public interface Outputter<T> {
        T output(RowDefCache rowDefCache, List<RowData> rows);
        @Deprecated // TODO remove once ~yshavit/akiban-server/memcache-tests-2 is in
        T error(String message);
    }
	public <T> T processRequest(Session session, String request, Outputter<T> outputter);
}
