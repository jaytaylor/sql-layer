package com.akiban.cserver.service.memcache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.Store;
import com.akiban.util.ArgumentValidation;

final class HapiProcessorImpl {

	static <T> T processRequest(Store store, Session session, String request,
                                 ByteBuffer byteBuffer, HapiProcessor.Outputter<T> outputter, final StringBuilder sb)
    {
        ArgumentValidation.notNull("outputter", outputter);
        String[] tokens = request.split(":");

        if(tokens.length == 3 || tokens.length == 4) {
            String schema = tokens[0];
            String table = tokens[1];
            String colkey = tokens[2];
            String min_val = null;
            String max_val = null;


            if(tokens.length == 4) {
                min_val = max_val = tokens[3];
            }

            final RowDefCache cache = store.getRowDefCache();
            try {
                List<RowData> list = store.fetchRows(session, schema, table, colkey, min_val, max_val, null, byteBuffer);
                return outputter.output(cache, list, sb);
            } catch (Exception e) {
                e.printStackTrace(); // TODO : once ~yshavit/akiban-server/memcache-tests-2 is in, just throw this
                return outputter.error("read error: " + e.getMessage());
            }

        }
        else {
            return outputter.error("invalid key: " + request);
        }
	}
}
